# The mapper class
# Contains the logic for generating the intermediate map output
import json
import time
from collections import defaultdict
from pathlib import Path 
from os import path 
class Mapper:
    def __init__(self,input_path,output_path,udf,i,number_of_reducers):
        self.id = i
        self.input_path = input_path
        self.output_path = output_path
        self.udf = udf
        self.number_of_reducers = number_of_reducers
        self.status = 'I'
        self.reducer_ids = []
        with open(self.input_path, 'r') as reader:
            self.input_data = reader.readlines()

    # Emitter function to emit the data into a dict
    def emit_intermediate(self,emmiter_tuple):
        # import pdb
        # pdb.set_trace()
        key,value = emmiter_tuple[0],emmiter_tuple[1]
        reducer_id = hash(key) % self.number_of_reducers
        self.map_data[reducer_id][key].append(value)
    # Dump the data into output.
    def write_data(self):
        Path(path.dirname(
			f'{self.output_path}/')).mkdir(parents=True, exist_ok=True)
         
        for reducer in self.map_data:
            out_file = f'{self.output_path}/m{self.id}r{reducer}.txt'
            self.reducer_ids.append(reducer)
            with open(out_file,'w') as outfile:
                json.dump(self.map_data[reducer],outfile)

    
    # Start the execution of map
    def start_mapper(self,active_reducers, update_status):
        self.map_data = defaultdict(list)
        update_status.put(['I', time.time()])
	
        self.map_data = defaultdict(lambda: defaultdict(list))
        counter = 0
        for dat in self.input_data:
            self.udf(counter, dat.rstrip('\n'), self.emit_intermediate)
            counter+=1
            update_status.put([self.status, True])
        self.reducer_ids.sort()
        active_reducers.put(self.reducer_ids) 

        update_status.put(['D', time.time()])

        self.write_data()




