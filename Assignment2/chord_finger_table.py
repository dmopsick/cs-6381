import json
import argparse # for argument parsing
import logging

# Should this class build records 
# Or should this class BE a Finger table
class FingerTableBuilder():

    DHT_KEY = "dht"

    def __init__(self, m, file_name, entity_name, address_space_bytes):
        self.m = m
        self.file_name = file_name
        self.entity_name = entity_name
        self.address_space_bytes = address_space_bytes

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

        # print(sorted_dht[-1])

        entity_index = -1

        # Find the index of the entity we are building the finger table for
        for index, obj in enumerate(sorted_dht):
            if obj.id == self.entity_name:
                entity_index = index
                break

        # Only proceed with a valid entity that exists in the DHT
        if entity_index != -1:
            entity_id = sorted_dht[entity_index].id


            # Let's test on finding the next node based on Chord's algorithm
            i = 1

            # Attempt to use Chord's algorithm
            next_node_hash = (entity_id + 2^(i + 1)) % self.address_space_bytes)

            # Print here is the hash of next node

            # Need to find the successor of the next node

        else:
            print("ERROR: Provided entity {} does not exist in the DHT".format(self.entity_name))

        
# Create new finger table builder object
finger_table_builder = FingerTableBuilder(3, "dht.json", 'disc1', 48)

# Build the finger table based on the above specified parameters
finger_table_builder.build()