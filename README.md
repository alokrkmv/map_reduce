
[![forthebadge made-with-python](http://ForTheBadge.com/images/badges/made-with-python.svg)](https://www.python.org/)

# map-reduce-status_200

# Authors
- [Alok Kumar]
- [Ritvika Pillai]
- [Siddharth Raman]

## Steps to run the Project

1. Clone the project using ````git clone https://github.com/cs532-2021-fall/map-reduce-status_200.git```` or download the zip file to your local machine.
2. Run the bash file **run_all_test_cases.sh** using ```` bash run_all_test_cases.sh````.
3. All the test cases should run along with all the comparator functions. For all the four test cases you will see the print statement stating that map reduce ouput and traditional output is exactly same if map reduce output and output produced by traditional logic is same otherwise you will see a statement stating there is some error in map reduce output.
4. First three test cases should run without any fault, in the fourth test case we have introduced a fault by forcely killing one of the processes based on kill index. For this test case you will see output similar to ````Killing process 1 to simulate fault tolerence, Mapper 1  of 3 has crashed, generating a new worker````, which means that a process has been killed and respawned by master.
5. Mappers will generate their output in ```` tmp```` and reducers will generate their output in ``` output``` directory. We remove all the intermediate files after master finishes to avoid any cluttering in the next run. The final output of reducers will be generated in  the path```output/<job_id>/reducer_id.txt```. The job id for each test case can be obtained from the print statement with intials ```` Reducers Finished please find the ouput in ouput/1635986793```` from console for each test case.
6. The actual output for each test case is provide in the folder ````test_scripts/ActualOutputs```` . These ouputs can be used to verify the map reduce outputs for each test case.

## Architecture of the System.

Please follow the [documentation](https://github.com/cs532-2021-fall/map-reduce-status_200/blob/main/Documentation.pdf) provided to understand the architecture of the system.
