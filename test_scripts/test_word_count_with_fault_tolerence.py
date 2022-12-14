import sys
import os
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../src")

from main import read_configs, initialize_master

import sys
import string


def udf_mapper(key, value, emit_intermediate):
        split_values = value.split(' ')
        for split in split_values:
            emit_intermediate((split, 1))

 # user_defined_reduce
def udf_reducer(key, values, emit_final):
    result = 0
    for v in values:
        result += v
    emit_final(key, result)

if __name__ == "__main__":
    
    # Function to clean the text file ( Remove the punctuations upper or lower casing etc)
    file_path = os.path.abspath(os.getcwd()) + "/test_scripts/test_config.txt"
    file_name, mapper, reducer = read_configs(file_path)
    file_name = "/test_scripts/" + file_name
    map_reduce_ouput = initialize_master(mapper, reducer, file_name, udf_mapper, udf_reducer,1)