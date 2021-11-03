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

# Function to clean the text file ( Remove the punctuations upper or lower casing etc)
def clean_file():
    file_path = os.path.abspath(os.getcwd()) + "/test_scripts/hamlet.txt"
    test_file = open(file_path)
    st = test_file.read()
    test_file.close()
    exclude = set(string.punctuation)
    # exclude.add("/n")
    # print(exclude)
    st = ''.join(ch.lower() for ch in st if ch not in exclude)
    st_array = st.split()
    # print(type(st_array))
    res = []
    # for el in st_array:
        # res.append(el.replace("\n", ""))
    final_string = res[0]
    for i in range(1, len(res)):
        final_string = final_string + " " + res[i]
    file = open(os.path.abspath(os.getcwd()) + "/test_scripts/hamlet_formatted.txt", "w")
    file.write(final_string)
    file.close()

# Function to get the count from traditional method of iteration
def traditional_count():
    file_path = os.path.abspath(os.getcwd()) + "/test_scripts/hamlet_formatted.txt"
    text_file = open(file_path, "r")

    # read whole file to a string
    data = text_file.read()

    # close file
    text_file.close()

    # break the string into list of words
    str_list = data.split()

    # gives set of unique words
    unique_words = set(str_list)

    traditional_count = {}

    for words in unique_words:
        traditional_count[words] = str_list.count(words)

    file = open(os.path.abspath(os.getcwd()) + "/test_scripts/ActualOutputs/test_1.txt", "w")
    file.write(str(traditional_count))


    return traditional_count

def compare_results(traditional_count,map_reduce_word_count):
    mismatch_dict = {k: traditional_count[k] for k in traditional_count if k in map_reduce_word_count and traditional_count[k] != map_reduce_word_count[k]}
    if len(mismatch_dict) == 0:
        print("Traditional word count and map reduce word count is exactly same")
    else:
        print("There is some error in the word count from map_reduce_word_count")
def map_reduce_output():

    file_path = os.path.abspath(os.getcwd())+"/output/thread_outputs/test_1.txt"
    final_dict = {}
    try:
        with open(file_path, 'r') as outputfile:
            for item in outputfile:
                final_dict = json.loads(item)
        return final_dict
    except Exception as e:
        sys.exit("Something went wrong in fetching the output from final file")

if __name__ == '__main__':
    # Generate the cleaned file after removing all the punctuations and casing
    # clean_file()

    # User defined mapper
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


    try:
        # clean_file()
        file_path = os.path.abspath(os.getcwd()) + "/test_scripts/test_config.txt"
        file_name, mapper, reducer = read_configs(file_path)
        file_name = "/test_scripts/" + file_name
        initialize_master(mapper, reducer, file_name, udf_mapper, udf_reducer,None)
        map_reduce_word_count = map_reduce_output()
        # map_reduce_ouput = master_thread.read_output()
        iterative_count = traditional_count()
        
        compare_results(iterative_count,map_reduce_word_count)
    except ValueError as v:
        sys.exit("Something went wrong in running test script 1")


