'''
Authors : Alok, Ritvika, Sidharth
Created at : 10/09/2021
'''

'''
This file contains scripts like reading the config from files. 
Invoking the master thread etc
'''
import sys
import json
# This function reads configs from the config file and returns mapper 
# reducer and  input data file name.
def read_configs(file_path):
    try:
        with open(file_path,'r') as conf_file:

            for item in conf_file:
                configs = json.loads(item)
                file_name = configs.get("input_file",None)
                mapper = int(configs.get("number_of_mapper",None))
                reducer = int(configs.get("number_of_reducer",None))
    except Exception as e:
        sys.exit('Fatal error unable to read the requested file')

    return file_name,mapper,reducer

        
