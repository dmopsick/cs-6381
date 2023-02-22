import json
import argparse # for argument parsing
import logging

# Should this class build records 
# Or should this class BE a Finger table
class FingerTableBuilder():

    DHT_KEY = "dht"

    def __init__(self):
        self.file_name =  "dht.json"
    
    # Build the DHT for use in finger table construction
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

    ##########################################
    # Create the finger table for a node 
    #
    # Use the provided dht.json
    ##########################################
    def create_finger_table(self, node_id, m, address_space_bits):
        finger_table = [ ]

        # Load our distributed hash table from json
        dht = self.build_dht()

        # Should I add successor field to the DHT?

        # print(dht)

        # print (node_id)
        
        # Create m entries in the finger table
        for i in range(m):
            # start = (node_id + (2^i)) % 2^m
            start = (node_id + 2^(i-1)) % address_space_bits
            # print("FLAG 1 ".format(start))
            
            
            # We determine the number of the next finger in the table
            # Now we need to find the successor of that key in the logical ring
            # successor = self.find_successor(start, dht)
            # finger_table.append({"start": start, "successor": successor})

            # print(start)

        # Test that I can load the next node 
        node = dht[1]

        print(node)

        immediate_successor = self.get_immediate_successor_of_node(node, dht)

        print(immediate_successor)
        

        return finger_table

    def get_immediate_successor_of_node(self, node, dht):
        successor = None

        # print("FLAG 10")
        # print(node)

        entity_index = -1

        # print(node['id'])

        # SHOULD I SORT BY ASCENDING EVERY TIME I CHECK
        # I suppose yes if things were going in out out
        # But we are fixed for this assignment

        # Find the index of this node in our dht 
        for index, obj in enumerate(dht):
            # print(obj)
            if obj['id'] == node['id']:
                entity_index = index
                break

        # print("FLAG 20: {}".format(entity_index))
        
        # Check to make sure it is not the last element in the array
        if entity_index == (len(dht) - 1):
            # We are working with last node in the logical ring
            # Point to the first node
            successor = dht[0]
        else:
            # Get the index + 1 as the immediate successor 
            successor = dht[entity_index + 1]
       
        return successor
        
# Create new finger table builder object
finger_table_builder = FingerTableBuilder()

# Build the finger table based on the above specified parameters
finger_table = finger_table_builder.create_finger_table(11905375445601, 3, 48)

print(finger_table)