import json


class Reducer:
    def __init__(self,intermediate_file_path,final_output_path,udf):
        self.intermediate_file_path = intermediate_file_path
        self.final_output_path = final_output_path
        self.udf = udf

        # Read the data from the intermediate file

        intermediate_file_path = f"{self.intermediate_file_path}/{'0.txt'}"

        self.mapper_output_dict = {}
        with open(intermediate_file_path,'r') as intermediate_file:
            self.mapper_output_dict = json.load(intermediate_file)

    # Create the generate final output
    def emit_final(self, key, value):
        # import pdb
        # pdb.set_trace()
        # key, value = reduced_tuple[0], reduced_tuple[1]
        self.reduced_data[key] = value
    # Write the final output to the ouput file
    def write_data(self):
        out_file = f"{self.final_output_path}/{'0.txt'}"
        with open(out_file, 'w') as outfile:
            json.dump(self.reduced_data, outfile)
    # Execute the reducer
    def start_reducer(self):
        self.reduced_data = {}
        for key,values in self.mapper_output_dict.items():
            self.udf(key, values, self.emit_final)
        self.write_data()


