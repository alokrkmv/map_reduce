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
import sys
import shutil


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

            # Reading the config file
            for item in conf_file:
                configs = json.loads(item)
                file_name = configs.get("input_file",None)
                mapper = int(configs.get("number_of_mapper",None))
                reducer = int(configs.get("number_of_reducer",None))
    except Exception as e:
        sys.exit('Fatal error unable to read the requested file')

    return file_name,mapper,reducer



# Function to intialize master and transfer control
def initialize_master(number_of_mappers,number_of_reducers,input_file,user_defined_map,user_defined_reduce,kill_idx):
    master_instance = Master(number_of_mappers,number_of_reducers,input_file,user_defined_map,user_defined_reduce,kill_idx)
    master_instance.start_process()

# The master class which will set all the configs and start the execution of mapper and reducers
class Master:
    def __init__(self,number_of_mappers,number_of_reducers,input_file,user_defined_map,user_defined_reduce,kill_idx):
        # Intilaizing master config
        self.number_of_mappers = number_of_mappers
        self.number_of_reducers = number_of_reducers
        self.input_file = input_file
        self.user_defined_map = user_defined_map
        self.user_defined_reduce = user_defined_reduce
        self.timeout = 3
        self.kill_idx = kill_idx
        #master reading data
        file_path=os.path.abspath(os.getcwd()) +self.input_file

        self.job_id = f'{int(time())}'

        if ospath.isdir(f'./tmp/{self.job_id}'):
            i = 1
            while ospath.isdir(f'./tmp/{self.job_id}-{i}'):
                i += 1
            self.job_id += f'-{i}'
        
        self.TMP_DIR = f'./tmp/{self.job_id}'
        self.OUT_DIR = f'./output/{self.job_id}'

        #To split data as per number of mappers
        SPLIT_DIR = f"{self.TMP_DIR}/input"
				# Creating the directory for the input data
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
    def retry(self, i, num_workers, kill_idx):

        # Restart a new mapper if the previous one is killed
        
        # Print any worker failure to console
        print (f"Mapper {i}  of {num_workers} has crashed, generating a new worker")
        self.processes[i].kill()
        self.reducer_ids[i] = mp.Queue()
        
        # Restart the process if the previous one is killed
        self.processes[i] = mp.Process(target = self.mappers[i].start_mapper, args = (self.reducer_ids[i], self.cs[i]))
        
        # Execute Worker
        self.processes[i].start()

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

        for i, m in enumerate(self.mappers):

            # Message queue for communicating with the mapper
            self.reducer_ids[i] = mp.Queue()
            self.cs[i] = mp.Queue()
            self.processes[i] = mp.Process(target = m.start_mapper, args = (self.reducer_ids[i], self.cs[i]))
            # Start the execution of Mappers
            self.processes[i].start()
            # If kill index is set then kill the mapper
            if self.kill_idx == i:
                print (f"Killing process {i} to simulate fault tolerence")
                self.processes[i].kill()

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
                        self.retry(i, self.number_of_mappers, self.kill_idx)
            	
            if curr_status == 'D' and self.mapper_status[i] == True:
                			
                self.mapper_status[i] = False
                # Keeping track of the number of active reducers
                self.active_reducers += self.reducer_ids[i].get()
                # Wait until all processes have been completed
                self.processes[i].join()
            else:
                mapping_status = False

    # Reduce phase
        self.processes = [None]*self.number_of_reducers
        self.cs = [None]*self.number_of_reducers
        self.reducer_status = [True]*self.number_of_reducers
        for i in range(0,self.number_of_reducers):
            self.reducers.append(Reducer(
                f'{self.TMP_DIR}/intermediate', self.OUT_DIR, self.user_defined_reduce,i, self.number_of_mappers, ))
        for i, r in enumerate(self.reducers):
            
            
            self.cs[i] = mp.Queue()
            self.processes[i] = mp.Process(target = r.start_reducer, args=(self.cs[i], ))
            self.processes[i].start()
        reducing_status =  False
     
        while reducing_status == False:
            reducing_status = True
            for i, r in enumerate(self.reducers):
                curr_status = None

                while True:
                    try:
                      
                        
                        [curr_status, timestamp] = self.cs[i].get(timeout = self.timeout)
                        break
                    except:
                        reducing_status = False
                        break
                if curr_status == 'D' and self.reducer_status[i] == True:
                    self.reducer_status[i] = False
                    self.processes[i].join()
                elif curr_status == 'R':
                    reducing_status = True
        
        print(f"Reducers Finished please find the ouput in ouput/{self.job_id}")

        # Removing the temporary directory

        file_path = f'{self.TMP_DIR}'
        try:
            shutil.rmtree(file_path)
        except OSError as e:
            print("Error: %s : %s" % (file_path, e.strerror))

        

    # This function reads the final output file and

    def write_data(self,writeData,output_path):
        out_file = f"{output_path}/{'0.txt'}"
        outputFile=open(out_file,"w")
        for line in writeData:
            outputFile.write(line)
