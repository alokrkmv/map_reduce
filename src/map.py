# The mapper class
# Contains the logic for generating the intermediate map output
import json
import time
from collections import defaultdict
class Mapper:
    def __init__(self,input_path,output_path,udf):
        self.input_path = input_path
        self.output_path = output_path
        self.udf = udf
        with open(self.input_path, 'r') as reader:
            self.input_data = reader.readlines()

    # Emitter function to emit the data into a dict
    def emit_intermediate(self,emmiter_tuple):
        # import pdb
        # pdb.set_trace()
        key,value = emmiter_tuple[0],emmiter_tuple[1]
        self.map_data[key].append(value)
    # Dump the data into output (As we are only dealing with a single thread as of now so
    # we don't need to worry about dumping to multiple files
    def write_data(self):
        out_file = f"{self.output_path}/{'0.txt'}"
        with open(out_file,'w') as outfile:
            json.dump(self.map_data,outfile)

    # Start the execution of map
    def start_mapper(self):
        # intialized the empty data container
        self.map_data = defaultdict(list)
        counter = 0
        for dat in self.input_data:
            self.udf(counter, dat.rstrip('\n'), self.emit_intermediate)
            counter+=1

        self.write_data()




