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
    # master_instance.start_process()
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
            mappers_used = set()
            n = 0
            line = reader.readline()
            while len(line) != 0:
                if not line.endswith('\n'):
                    line += '\n'
                mapper_id = n % self.number_of_mappers
                mappers_used.add(mapper_id)
                with open(f'{SPLIT_DIR}/{mapper_id}.txt', 'a') as writer:
                    writer.write(line)
                line = reader.readline()
                n += 1
        if len(mappers_used) < self.number_of_mappers:
            print(
                f"Insufficient input lines. Setting mappers to {mappers_used}")
            self.M = mappers_used
    def start_process(self,number_of_workers=None):
        print("Running Mappers")
        # Instantiating  the mapper class
        mapper = Mapper(self.input_file,self.mapper_dir,self.user_defined_map)
        '''
        Using python process module instead of python thread module to avoid GIL
        All though the same implementation can be extended to threads by using ThreadPoolExecutor instead 
        of ProcessPoolExecutor.
        '''
        self.mappers.append(Mapper(
				idx, self.R, self.input_file_paths[idx], f'{self.TMP_DIR}/intermediate', map_func))
        
        # Starting just a single worker as per the mid milestone requirement
        executor = ProcessPoolExecutor(max_workers=1)
        executor.submit(mapper.start_mapper())
        # mapper.start_mapper()

        print("Finished with mapper execution")

        # Once the mappers have finished their job we will start reducer workers
        # In this case as we only have to do single threaded implementation
        # we will start only one worker for mid project milestone

        print("Starting the reduce job")
        reducer = Reducer(self.mapper_dir,self.reducer_dir,self.user_defined_reduce)
        executor.submit(reducer.start_reducer())
        print(f"Reducer job finished successfully please find the final output in {self.reducer_dir}")

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
