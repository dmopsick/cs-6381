import json
import time # For using sleep for debug purposes
import random

# Should this class build records 
# Or should this class BE a Finger table
class DhtUtil():

    DHT_KEY = "dht"

    def __init__(self):
        pass
    
    # Build the DHT for use in finger table construction
    def build_dht(self, dht_file_name):
        # Load the file 
        dht_json_file = open(dht_file_name)

        # Read the file contents as string
        dht_json_string = dht_json_file.read()

        # Turn the file contents into json data
        dht_json = json.loads(dht_json_string)

        # Load the DHT from the dict 
        dht = dht_json[self.DHT_KEY]

        # Sort the DHT list by hash value in ascending value
        sorted_dht = sorted(dht, key=lambda x: x['hash'])

        return sorted_dht

    ###############################################
    # Select a random note in the DHT 
    #
    # This is for use in the pubs/subs
    ###############################################
    def get_random_node_from_dht_file_name(self, dht_file_name):
        # Load the DHT
        dht = self.build_dht(dht_file_name)

        # Select a random index from the entire DHT
        random_index = random.randint(0, len(dht) -1)

        # Get the node at the random index
        random_node = dht[random_index]

        # Return the randomly selected node
        return random_node

    ##########################################
    # Create the finger table for a node 
    #
    # Use the provided dht.json
    ##########################################
    def create_finger_table(self, node_id, dht, address_space_bits):
        finger_table = []

        # print(dht)
        # Default node variable
        node = None

        # Load the node we are working with based on id
        for node_obj in dht:
            # Check if the node has the id we are looking for
            # ex: disc1
            if node_obj["id"] == node_id:
                # This is the node we are looking for
                node = node_obj

        if node != None:
            # Using the number of entries in the finger table as 
            # The same value as the address space bits
            # But setting it to m so the formula looks more how I expect it
            m = address_space_bits

            # Create m entries in the finger table
            for i in range(address_space_bits):
                # Get the next value according the chord algorithm
                # Adding 1 to compensate for the range going from 0 - 7
                finger_value = (node["hash"] + 2**(i + 1 -1)) % 2**(m)
                # print("FLAG 1 ".format(start))

                # Find the node sucessor of the generated value
                finger_node = self.find_successor(finger_value, dht)

                # Add the object to our finger table  
                finger_table.append(finger_node)
        else:
            print("ERROR: Invalid node id provided: {}".format(node_id))

        return finger_table

    #######################################
    # Find the successor of a key from a node
    #
    # Taken from Week 4 async slides 
    ########################################
    def find_successor_from_node(self, key, node, dht):
        if self.is_between(key, node, self.get_immediate_successor_of_node(node, dht)):
            # The key is between the node and its successor
            # Return the successor of the node
            return self.get_immediate_successor_of_node(node, dht)
        else:
            # The key is not between the node and its successor
            # Need to go find the closest preceding node
            # Then select the successor of that node
            node_to_check = self.closest_preceding_node(key, dht)
            return self.get_immediate_successor_of_node(node_to_check, dht)

    #########################################
    # Find the successor of any key in a distributed hash table
    #
    #########################################
    def find_successor(self, key, dht):
        # To find the sucessor of a value on the logical ring
        # We can find the predecessor and then that node will
        # Know their direct successor 
        predecessor_node = self.find_predecessor(key, dht)
        
        return self.get_immediate_successor_of_node(predecessor_node, dht)

    ############################################
    # Find the predecssor node of any hash value
    #
    ############################################
    def find_predecessor(self, key, dht):
        node = self.closest_preceding_node(key, dht)

        # Check if the given key is between the node to check and its immediate successor
        while not (self.is_between(key, node["hash"], self.get_immediate_successor_of_node(node, dht)["hash"])):
            node = self.get_immediate_successor_of_node(node, dht)
            # print("Checking if {} is between {} and {}".format(key, node["hash"], self.get_immediate_successor_of_node(node, dht)["hash"]))
            # time.sleep(0.5)
        return node

    ######################################
    # Select the closest preceding node of a value on the logical ring
    #
    # Select the node that the key is between the node and its sucessor
    ######################################
    def closest_preceding_node(self, key, dht):
        # Select the first node of the DHT for the first comparison
        node = dht[0]

        # Iterate through the table, skip the first node
        for n in dht[1:]:
            # Check if the key is between that node and its successor
            if self.is_between(key, n["hash"], node["hash"]):
                # If the key is between the node and its successor
                # Make that the node that we are now checking
                # This will give us the closest preceding
                # If the key is between multiple sets of nodes, will take the closest gap
                node = n

            return node

    ###########################################
    # Perform the actual logic for checking if a key is between a value
    #
    # Account for the last element in the ring
    ###########################################
    def is_between(self, key, node_id, successor_id):

        # print("FLAG 10")
        # print(key)
        # print(node_id)
        # print(successor_id)

        # Check if the provided node_id is less than its successor
        # This checks for the case of where the provided node is the last node
        # And then points to the first node
        if node_id < successor_id:
            # Sucessor is bigger than the node, check for standard between
            if (node_id < key) and (key < successor_id):
                result = True
            else:
                result = False
        else:
            # Check if the key is larger than the node or smaller than the successor
            # if node was 10 o clock on a clock and successor would be 1
            # This would represent the space on the clock between 10 and 1
            if (node_id < key) or (key < successor_id):
                result = True
            else:
                result = False
        
        return result
    
    #############################################
    # Load the immediate successor of a node
    #
    # Each node will know about its immediate successor, this is how
    # We get the index of the node we are working with, and take the next one
    ##############################################
    def get_immediate_successor_of_node(self, node, dht):
        successor = None

        entity_index = -1

        # print("Checking next hash after {}".format(node["id"]))

        # SHOULD I SORT BY ASCENDING EVERY TIME I CHECK
        # I suppose yes if things were going in out out
        # But we are fixed for this assignment

        # Find the index of this node in our dht 
        for index, obj in enumerate(dht):
            # print(obj)
            if obj['id'] == node['id']:
                entity_index = index
                break
        
        
        # Check to make sure it is not the last element in the array
        if entity_index == (len(dht) - 1):
            # We are working with last node in the logical ring
            # Point to the first node
            successor = dht[0]
        else:
            # print("GRAB INDEX: {}".format(entity_index))
            # Get the index + 1 as the immediate successor 
            successor = dht[entity_index + 1]
       
        return successor
        
# Create new finger table builder object
# finger_table_builder = FingerTableBuilder()

# Build the finger table based on the above specified parameters
# finger_table = finger_table_builder.create_finger_table(11905375445601, 48)
# finger_table = finger_table_builder.create_finger_table(78, 8)
# finger_table = finger_table_builder.create_finger_table('disc15', "dht.json", 8)

# print(finger_table)
