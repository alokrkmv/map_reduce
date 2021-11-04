import json
from pathlib import Path 
from os import path 


class Reducer:
    def __init__(self,intermediate_file_path,final_output_path,udf,id,number_of_mappers):
       
        self.intermediate_file_path = intermediate_file_path
        self.final_output_path = final_output_path
        self.udf = udf
        self.id = id
        self.number_of_mappers = number_of_mappers
        self.input_data = []

        final_dict = {}

        # Read the data from the intermediate file
        for mapper in range(self.number_of_mappers):

            intermediate_file_path = f'{self.intermediate_file_path}/m{mapper}r{self.id}.txt'

            self.mapper_output_dict = {}
            with open(intermediate_file_path,'r') as intermediate_file:
                self.mapper_output_dict = json.load(intermediate_file)
                self.input_data.append(self.mapper_output_dict)
        self.final_dict = {k:v for x in self.input_data for k,v in x.items()}

    # Create the generate final output
    def emit_final(self, key, value):
     
        self.reduced_data[key] = value
    # Write the final output to the ouput file
    def write_data(self):
        Path(path.dirname(
            f'{self.final_output_path}/')).mkdir(parents=True, exist_ok=True)
        out_file = f'{self.final_output_path}/{self.id}.txt'
       
        with open(out_file, 'w') as outfile:
            json.dump(self.reduced_data, outfile)
    # Execute the reducer
    def start_reducer(self,update_status):
        self.status = 'D'
        self.status = 'R'
        update_status.put([self.status, True])
        self.reduced_data = {}
        for key,values in self.final_dict.items():
            self.udf(key, values, self.emit_final)
        self.status = 'D'
        update_status.put([self.status, True])
        self.write_data()


