import sys
import os
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../src")

from main import read_configs, initialize_master

import sys
import string

def test_configs():

    file_path = os.path.abspath(os.getcwd()) + "/test_scripts/average_dataset.txt"

    file_name,mapper,reducer = read_configs(file_path)
    print(type(file_name),type(mapper),type(reducer))
# if __name__=='__main__':
#     test_configs()
def traditional_count():
    file_path = os.path.abspath(os.getcwd()) + "/test_scripts/average_dataset.txt"
    text_file = open(file_path, "r")

    # read whole file to a string
    data = text_file.read()

    # close file
    text_file.close()
    # break the string into list of words
    str_list = data.split()
    traditional_count = {}
    for i in str_list:
        temp=i.split(",",1)
        scores=temp[1]
        final_scores=scores.split(",")
        traditional_sum=int(final_scores[0])+int(final_scores[1])+int(final_scores[2])+int(final_scores[3])+int(final_scores[4])
        percentage_traditional=(traditional_sum/50)*100
        traditional_count[i[0]]=percentage_traditional

    return traditional_count

def compare_results(traditional_count,map_reduce_word_count):
    mismatch_dict = {k: traditional_count[k] for k in traditional_count if k in map_reduce_word_count and traditional_count[k] != map_reduce_word_count[k]}
    if len(mismatch_dict) == 0:
        print("Traditional word count and map reduce word count is exactly same")
    else:
        print("There is some error in the word count from map_reduce_word_count")

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
        file_path = os.path.abspath(os.getcwd()) + "/test_scripts/test_config_average.txt"
        file_name, mapper, reducer = read_configs(file_path)
        file_name = "test_scripts/" + file_name
        map_reduce_ouput = initialize_master(mapper, reducer, file_name, udf_mapper, udf_reducer)

        # map_reduce_ouput = master_thread.read_output()
        iterative_count = traditional_count()
        compare_results(iterative_count,map_reduce_ouput)
    except ValueError as v:
        sys.exit("Something went wrong in running test script 1")
