import sys
import os
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../src")

from main import read_configs, initialize_master

def test_configs():

    file_path = os.path.abspath(os.getcwd()) + "/test_scripts/test_config.txt"

    file_name,mapper,reducer = read_configs(file_path)
    print(type(file_name),type(mapper),type(reducer))

# if __name__=='__main__':
#     test_configs()

if __name__ == '__main__':
#     mp.set_start_method("fork")
#     print("""
# TEST #1: Word Count
# -------------------
# """)


    # Define custom mapper function
    def udf_mapper(key, value, emitter):
        split_values = value.split(' ')
        for split in split_values:
            emitter((split, 1))


    # Define custom reducer function
    def udf_reducer(key, values, emitter):
        result = 0
        for v in values:
            result += v
        emitter(key, result)


    try:
        # import pdb
        # pdb.set_trace()
        # Execute MapReduce job
        file_path = os.path.abspath(os.getcwd()) + "/test_scripts/test_config.txt"
        file_name, mapper, reducer = read_configs(file_path)
        file_name = "test_scripts/" + file_name
        mapred = initialize_master(mapper, reducer, file_name, udf_mapper, udf_reducer)

    #     # Sequential test verification
    #     print("Verifying MapReduce results:")
    #     # Read MapReduce results
    #     output_arr = mapred.read_output()
    #     word_count_mapred = {}
    #     for line in output_arr:
    #         word, count = line.rsplit(':', 1)
    #     word_count_mapred[word] = int(count)
    #     # Compute sequential results
    #     word_count_seq = {}
    #     with open('data/hamlet.txt', 'r') as reader:
    #         input_data = reader.readlines()
    #     for idx, line in enumerate(input_data):
    #         line = line.rstrip('\n')
    #         for word in line.split(' '):
    #             if word not in word_count_seq:
    #                 word_count_seq[word] = 0
    #             word_count_seq[word] += 1
    #     for word in word_count_mapred:
    #         if word_count_mapred[word] != word_count_seq[word]:
    #             print('FAIL')
    #             exit()
    #     print('PASS')
    except ValueError as v:
        print(v)

