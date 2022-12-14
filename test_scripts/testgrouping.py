import sys
import os
import json
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../src")

from main import read_configs, initialize_master

import sys
import string

def test_configs():

    file_path = os.path.abspath(os.getcwd()) + "/test_scripts/test_config.txt"

    file_name,mapper,reducer = read_configs(file_path)
    print(type(file_name),type(mapper),type(reducer))

# Function to group by traditional method of iteration
def traditional_count():
    file_path = os.path.abspath(os.getcwd()) + "/test_scripts/AgeData.txt"
    textFile = open(file_path, "r")
    data=textFile.readlines()    # reading file line by line
    traditionalOutputData={}
    for line in data:
        m=line.split("\n")
        n=m[0].split(",")        #splitting only the required data and adding them to dictionary
        if(len(n)>1):
            traditionalOutputData[n[1]]=traditionalOutputData.get(n[1],[])+[n[0]]
        else:
            continue
    for i in traditionalOutputData:
        traditionalOutputData[i]=sorted(traditionalOutputData[i]) #sort the content for a given key
    return traditionalOutputData

def map_reduce_output():

    file_path = os.path.abspath(os.getcwd())+"/output/thread_outputs/test_3.txt"
    final_dict = {}
    try:
        with open(file_path, 'r') as outputfile:
            for item in outputfile:
                final_dict = json.loads(item)
        return final_dict
    except Exception as e:
        sys.exit("Something went wrong in fetching the output from final file")


def compare_results(traditional_count,map_reduce_output):
    if(traditional_count==map_reduce_output):
        print('traditional grouping and map reduce grouping are exactly the same')
    else:
        print('traditional grouping and map reduce grouping are not same')

if __name__ == '__main__':

    # User defined mapper
    def udf_mapper(key, value, emit_intermediate):
        split_values = value.split(',')
        emit_intermediate((split_values[1],split_values[0]))


    # user_defined_reduce
    def udf_reducer(key, values, emit_final):
        values.sort()
        emit_final(key, values)


    try:
        file_path = os.path.abspath(os.getcwd()) + "/test_scripts/test_config_group.txt"
        file_name, mapper, reducer = read_configs(file_path)
        file_name = "/test_scripts/" + file_name
        initialize_master(mapper, reducer, file_name, udf_mapper, udf_reducer,None)
        iterative_count = traditional_count()
        map_reduce_output = map_reduce_output()
        compare_results(iterative_count,map_reduce_output)
    except ValueError as v:
        sys.exit("Something went wrong in running test script 1")
