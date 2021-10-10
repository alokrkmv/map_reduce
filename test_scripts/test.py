import sys
import os
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../src")

from main import read_configs

def test_configs():

    file_path = os.path.abspath(os.getcwd()) + "/test_scripts/test_config.txt"

    file_name,mapper,reducer = read_configs(file_path)
    print(type(file_name),type(mapper),type(reducer))

if __name__=='__main__':
    test_configs()