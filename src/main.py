'''
Authors : Alok, Ritvika, Sidharth
Created at : 10/09/2021
'''
from random import random
from concurrent.futures import ProcessPoolExecutor

'''
This file contains scripts like reading the config from files. 
Invoking the master thread etc
'''
import sys
import json
from map import Mapper
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

# The master class which will set all the configs and start the execution of mapper and reducers
class Master:
    def __init__(self,number_of_mappers,number_of_reducers,input_file,user_defined_map,user_defined_reduce):
        self.worker_id = random.randit(3000, 5000)

        # Creating the directory where mapper worker will save their intermediate output
        self.mapper_dir = f'./intermediate/{self.worker_id}'
        # Creating the final reduce directory where final output from reducer will save their output
        self.reducer_dir = f'./reducer/{self.worker_id}'

        # Intilaizing master config
        self.number_of_mappers = number_of_mappers
        self.number_of_reducers = number_of_reducers
        self.input_file = input_file
        self.user_defined_map = user_defined_map
        self.user_defined_reduce = user_defined_reduce

    def start_process(self):
        print("Running Mappers")
        # Instantiating  the mapper class
        mapper = Mapper(self.input_file,self.mapper_dir,self.user_defined_map)
        '''
        Using python process module instead of python thread module to avoid GIL
        All though the same implementation can be extended to threads by using ThreadPoolExecutor instead 
        of ProcessPoolExecutor.
        '''

        # Starting just a single worker as per the mid milestone requirement
        executor = ProcessPoolExecutor(max_workers=1)
        executor.submit(mapper.start_mapper())











        
