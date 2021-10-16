
[![forthebadge made-with-python](http://ForTheBadge.com/images/badges/made-with-python.svg)](https://www.python.org/)

# map-reduce-status_200

# Authors
- [Alok Kumar]
- [Ritvika Pillai]
- [Siddharth Raman]

## Steps to run the Project

1. Clone the project using ````git clone https://github.com/cs532-2021-fall/map-reduce-status_200.git```` or download the zip file to your local machine.
2. Delete any existing **intermediate** or **final_output** directory if present.
3. Run the bash file **run_all_test_cases.sh** using ```` bash run_all_test_cases.sh````.
4. All the test cases should run along with all the comparator functions. For all the three test cases you will see the print statement stating that map reduce ouput and traditional output is exactly same.
5. Mappers will generate their output in ```` intermediate```` and reducers will generate their output in ``` final_output``` directory. The final output of reducers will be generated in  the path```final_output/<reducer_id>/0.txt```. The reducer id for each test case can be obtained from the print statement with intials ```` Reducer job finished successfully please find the final output in ./final_output/<reducer_id>```` from console for each test case.
6. The actual output for each test case is provide in the folder ````test_scripts/ActualOutputs```` . These ouputs can be used to verify the map reduce outputs for each test case.
