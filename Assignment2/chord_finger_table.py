import json
import argparse # for argument parsing
import logging

# Should this class build records 
# Or should this class BE a Finger table
class FingerTableBuilder():

    DHT_KEY = "dht"

    def __init__(self):
        self.file_name =  "dht.json"
    
    def build_dht(self):
        # Load the file 
        dht_json_file = open(self.file_name)

        # Read the file contents as string
        dht_json_string = dht_json_file.read()

        # Turn the file contents into json data
        dht_json = json.loads(dht_json_string)

        # Load the DHT from the dict 
        dht = dht_json[self.DHT_KEY]

        # Sort the DHT list by hash value in ascending value
        sorted_dht = sorted(dht, key=lambda x: x['hash'])

        return sorted_dht

    def create_finger_table(self, node_id, m):
        finger_table = [ ]
        
        # Create m entries in the finger table
        for i in range(m):
            start = (node_id + (2^i)) % 2^m
            

            print(start)

        return finger_table

        
# Create new finger table builder object
finger_table_builder = FingerTableBuilder()

# Build the finger table based on the above specified parameters
finger_table = finger_table_builder.create_finger_table(275867163294784, 3)