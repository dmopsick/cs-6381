import json
import argparse # for argument parsing
import logging

# Should this class build records 
# Or should this class BE a Finger table
class FingerTableBuilder():

    DHT_KEY = "dht"

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
        dht = dht_json[self.DHT_KEY]

        # Sort the DHT list by hash value
        sorted_dht = sorted(dht, key=lambda x: x['hash'])

        print(sorted_dht[-1])


        
# Create new finger table builder object
finger_table_builder = FingerTableBuilder(3, "dht.json")

# Build the finger table based on the above specified parameters
finger_table_builder.build()