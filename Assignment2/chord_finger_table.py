import json
import argparse # for argument parsing
import logging

# Should this class build records 
# Or should this class BE a Finger table
class FingerTableBuilder():

    def __init__(self, num_fingers, file_name):
        self.num_fingers = num_fingers
        self.file_name = file_name

    def build(self):
        # Load the file 
        dht_json_file = open(self.file_name)

        # Read the file contents as string
        dht_json_string = dht_json_file.read()

        # print(dht_json_string)
        # Turn the file contents into json data
        dht_json = json.loads(dht_json_string)

        # print(dht_json)

        # Load the DHT from the dict 

        # Sort the DHT list by hash value

        
# Create new finger table builder object
finger_table_builder = FingerTableBuilder(3, "dht.json")

# Build the finger table based on the above specified parameters
finger_table_builder.build()