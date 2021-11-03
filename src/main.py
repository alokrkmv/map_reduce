'''
Authors : Alok, Ritvika, Sidharth
Created at : 10/09/2021
'''
import os.path
import random
from concurrent.futures import ProcessPoolExecutor
from time import time
from os import path as ospath, listdir
from pathlib import Path as pathlibpath
import multiprocessing as mp

'''
This file contains scripts like reading the config from files. 
Invoking the master thread etc
'''
import sys
import json
from map import Mapper
from reduce import  Reducer
# This function reads configs from the config file and returns mapper 
# reducer and  input data file name.
def read_configs(file_path):
    try:
        with open(file_path,'r') as conf_file:

            for item in conf_file:
                configs = json.loads(item)
                file_name = configs.get("input_file",None)
                # In the first milestone we are doing single threaded application of map reduce so we will
                # be only using one mapper and one reducer
                mapper = int(configs.get("number_of_mapper",None))
                reducer = int(configs.get("number_of_reducer",None))
    except Exception as e:
        sys.exit('Fatal error unable to read the requested file')

    return file_name,mapper,reducer

# Function to intialize master and transfer control
def initialize_master(number_of_mappers,number_of_reducers,input_file,user_defined_map,user_defined_reduce):
    master_instance = Master(number_of_mappers,number_of_reducers,input_file,user_defined_map,user_defined_reduce)
    master_instance.start_process()
    # return master_instance.read_output()

# The master class which will set all the configs and start the execution of mapper and reducers
class Master:
    def __init__(self,number_of_mappers,number_of_reducers,input_file,user_defined_map,user_defined_reduce):
        #moved it to above from below
        # Intilaizing master config
        self.number_of_mappers = number_of_mappers
        self.number_of_reducers = number_of_reducers
        self.input_file = input_file
        self.user_defined_map = user_defined_map
        self.user_defined_reduce = user_defined_reduce

        #master reading data
        file_path=os.path.abspath(os.getcwd()) +self.input_file

        self.job_id = f'{int(time())}'

        if ospath.isdir(f'./tmp/{self.job_id}'):
            i = 1
            while ospath.isdir(f'./tmp/{self.job_id}-{i}'):
                i += 1
            self.job_id += f'-{i}'
        
        self.TMP_DIR = f'./tmp/{self.job_id}'

        #to split data as per number of mappers
        SPLIT_DIR = f"{self.TMP_DIR}/input"
				# Create input_splits directory
        pathlibpath(ospath.dirname(
            f'{SPLIT_DIR}/')).mkdir(parents=True, exist_ok=True)
        with open(file_path, 'r') as reader:
            n = 0
            line = reader.readline()
            while len(line) != 0:
                if not line.endswith('\n'):
                    line += '\n'
                mapper_id = n % self.number_of_mappers
                with open(f'{SPLIT_DIR}/{mapper_id}.txt', 'a') as writer:
                    writer.write(line)
                line = reader.readline()
                n += 1
        self.input_files = [
					f'{SPLIT_DIR}/{i}.txt' for i in range(self.number_of_mappers)]
    def start_process(self):
        print("Running Mappers")
        # Instantiating  the mapper class
        self.mappers = []
        self.reducers = []
        self.active_reducers = []
        '''
        We are moving from process pool executor to vanila multiprocessing so that fault tolerence can be implemented
        '''
        
        for i in range(len(self.input_files)):
            self.mappers.append(Mapper(self.input_files[i],f'{self.TMP_DIR}/intermediate',self.user_defined_map,i,self.number_of_reducers))
        
        # Starting  Mapper phase
        self.processes = [None]*self.number_of_mappers
        self.reducer_ids = [None]*self.number_of_mappers
        self.ps = [None]*self.number_of_mappers
        self.cs = [None]*self.number_of_mappers
        self.mapper_status = [True]*self.number_of_mappers


        # executor = ProcessPoolExecutor(max_workers=self.number_of_mappers)
        # for i in range(0,self.number_of_mappers):
        #     executor.submit(self.mappers[i].start_process)
        # executor.submit(mapper.start_mapper())
        # mapper.start_mapper()

        for i, m in enumerate(self.mappers):

            #queue used for message passing
            self.reducer_ids[i] = mp.Queue()
            # ps[i], cs[i] = mp.Pipe()
            self.cs[i] = mp.Queue()
            self.processes[i] = mp.Process(target = m.start_mapper, args = (self.reducer_ids[i], self.cs[i]))
            #execute mapper
            self.processes[i].start()

        print("Finished with mapper execution")

        mapping_status = False
        while (mapping_status == False):
            mapping_status = True
            for i, m in enumerate(self.mappers):
                curr_status = None
                while True:
                    try:
                        [curr_status, timestamp] = self.cs[i].get(timeout = self.timeout)
                        break
                    except:
                        mapping_status = False
                        break

            if curr_status == 'D' and self.mapper_status[i] == True:				
                self.mapper_status[i] = False
                #get all valid reducer_ids
                self.active_reducers += self.reducer_ids[i].get()
                #wait until all processes have been completed
                self.processes[i].join()
            else:
                mapping_status = True

    # This function reads the final output file and
    def read_output(self):

        file_path = f"{self.reducer_dir}/{'0.txt'}"
        final_dict = {}
        try:
            with open(file_path, 'r') as outputfile:
                for item in outputfile:
                    final_dict = json.loads(item)
            return final_dict
        except Exception as e:
            sys.exit("Something went wrong in fetching the output from final file")

    def write_data(self,writeData,output_path):
        out_file = f"{output_path}/{'0.txt'}"
        outputFile=open(out_file,"w")
        for line in writeData:
            outputFile.write(line)
