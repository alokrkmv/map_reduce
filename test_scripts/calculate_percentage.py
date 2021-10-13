import sys
import os
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../src")

from main import read_configs, initialize_master

import sys
import string

def test_configs():

    file_path = os.path.abspath(os.getcwd()) + "/test_scripts/percentage_dataset.txt"

    file_name,mapper,reducer = read_configs(file_path)
    print(type(file_name),type(mapper),type(reducer))
# if __name__=='__main__':
#     test_configs()
def traditional_percentage():
    file_path = os.path.abspath(os.getcwd()) + "/test_scripts/percentage_dataset.txt"
    text_file = open(file_path, "r")

    # read whole file to a string
    data = text_file.read()

    # close file
    text_file.close()
    # break the string into list of words
    str_list = data.split()
    traditional_percentage = {}
    for i in str_list:
        temp=i.split(",",1)
        scores=temp[1]
        final_scores=scores.split(",")
        traditional_sum=int(final_scores[0])+int(final_scores[1])+int(final_scores[2])+int(final_scores[3])+int(final_scores[4])
        percentage_score=(traditional_sum/50)*100
        traditional_percentage[i[0]]=percentage_score

    return traditional_percentage

def compare_results(traditional_percentage,map_reduce_ouput):
    mismatch_dict = {k: traditional_percentage[k] for k in traditional_percentage if k in map_reduce_ouput and traditional_percentage[k] != map_reduce_ouput[k]}
    if len(mismatch_dict) == 0:
        print("Traditional percentage and map reduce percentage is exactly same")
    else:
        print("There is some error in the percentage from map_reduce_output")

if __name__ == '__main__':

    # User defined mapper
    def udf_mapper(key, value, emit_intermediate):
        split_values = value.split(',')
        student_name = split_values[0]
        for split in split_values[1:]:
            emit_intermediate((student_name, int(split)))

    # user_defined_reduce
    def udf_reducer(key, values, emit_final):
        result = 0
        percentage_student=0.00
        total_score=50
        for v in values:
            result += v
            percentage_student=(result/total_score)*100
        emit_final(key, percentage_student)

    try:
        file_path = os.path.abspath(os.getcwd()) + "/test_scripts/test_config_percentage.txt"
        file_name, mapper, reducer = read_configs(file_path)
        file_name = "test_scripts/" + file_name
        map_reduce_ouput = initialize_master(mapper, reducer, file_name, udf_mapper, udf_reducer)

        # map_reduce_ouput = master_thread.read_output()
        iterative_percentage = traditional_percentage()
        compare_results(iterative_percentage,map_reduce_ouput)
    except ValueError as v:
        sys.exit("Something went wrong in running test script 1")
