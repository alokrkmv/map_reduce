'''
Authors : Alok, Ritvika, Sidharth
Created at : 10/09/2021
'''
import os.path
import random
from concurrent.futures import ProcessPoolExecutor

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
    return master_instance.read_output()

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
        textFile=open(file_path,"r")
        data=textFile.readlines()
        splitLength=len(data)//self.number_of_mappers
        splittedData=self.splitList(data,splitLength)

        #to split data as per number of mappers
        for i in range(self.number_of_mappers):
            
            self.worker_id = random.randint(3000, 5000)

            # Creating the directory where master will save the splitted input
            master_dir = f'./partitioned_input/{self.worker_id}'
            master_path_exists = os.path.exists(master_dir)
            if not master_path_exists:
                os.makedirs(master_dir)
            self.master_dir = master_dir
            inputMapperData=splittedData[i]
            self.write_data(inputMapperData,self.master_dir)
        
            # Creating the directory where mapper worker will save their intermediate output
            mapper_dir = f'./intermediate/{self.worker_id}'
            mapper_path_exists = os.path.exists(mapper_dir)
            if not mapper_path_exists:
                os.makedirs(mapper_dir)
            self.mapper_dir = mapper_dir

            # Creating the final reduce output directory where final output from reducer will save their output
            reduce_dir = f'./final_output/{self.worker_id}'
            reducer_path_exists = os.path.exists(reduce_dir)
            if not reducer_path_exists:
                os.makedirs(reduce_dir)
            self.reducer_dir = reduce_dir

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

    def splitList(self,a,n):
        b={}
        counter=0
        for i in range(0,len(a),n):
            #print('value of i',i)
            #print('value of n',n)
            b[counter]=a[i:i+n]
            counter+=1
            #print(b)
        return b

    def write_data(self,writeData,output_path):
        out_file = f"{output_path}/{'0.txt'}"
        outputFile=open(out_file,"w")
        for line in writeData:
            outputFile.write(line)
