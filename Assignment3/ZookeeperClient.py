#
# Author: Aniruddha Gokhale
# Vanderbilt University
# CS 6381 Distributed Systems Principles
# Created: Spring 2018
#
# This is a sample code showing a variety of commands 
# using a Python client for ZooKeeper. We assume that the
# ZooKeeper server is running.
#
# Modified: Spring 2019 (converted to Python3 compatible code using 2to3-2.7.py)
# Additional restructuring and commenting added. More comments Spring 2021.

# import some basic packages just in case we need these
import os
import sys
import time
import logging # for logging. Use it in place of print statements.

# argument parser
import argparse

# Now import the kazoo package that supports Python binding
# to ZooKeeper
from kazoo.client import KazooClient   # client API
from kazoo.client import KazooState    # for the state machine

from CS6381_MW import discovery_pb2

#--------------------------------------------------------------------------
# define a callback function to let us know what state we are in currently
# Kazoo is implemented in such a way that the system goes thru 3 states.
# ZooKeeper clients go thru 3 states:
#    LOST => when it is instantiated  or when not in a session with a server;
#    CONNECTED => when connected with server, and
#    SUSPENDED => when the connection  is lost or the server node is no
#                                longer part of the quorum
#
#--------------------------------------------------------------------------
def listener4state (state):
    if state == KazooState.LOST:
        print ("Current state is now = LOST")
    elif state == KazooState.SUSPENDED:
        print ("Current state is now = SUSPENDED")
    elif state == KazooState.CONNECTED:
        print ("Current state is now = CONNECTED")
    else:
        print ("Current state now = UNKNOWN !! Cannot happen")
        
# ------------------------------------------------------------------
# The driver class. Does not derive from anything
#
class ZK_Driver ():
    """ The ZooKeeper Driver Class """

    #################################################################
    # constructor
    #################################################################
    def __init__ (self, args, logger):
        self.zk = None  # session handle to the zookeeper server
        self.zkIPAddr = args.zkIPAddr  # ZK server IP address
        self.zkPort = args.zkPort # ZK server port num
        self.zkName = args.zkName # refers to the znode path being manipulated
        self.zkVal = args.zkVal # refers to the znode value
        self.logger = logger  # internal logger for print statements
        self.server_active = True
        # Watch mechanism variables
        self.total_entities = args.totalEntities # Keep track of how many total entities should be in the system
        self.current_num_entities = 0

    #-----------------------------------------------------------------------
    # Debugging: Dump the contents

    def dump (self):
        """dump contents"""
        print ("=================================")
        print ("Server IP: {}, Port: {}; Path = {} and Val = {}".format (self.zkIPAddr, self.zkPort, self.zkName, self.zkVal))
        print ("=================================")

    # -----------------------------------------------------------------------
    # Initialize the driver
    # -----------------------------------------------------------------------
    def init_driver (self):
        """Initialize the client driver program"""

        try:
            # debug output
            self.dump ()

            # Init the num entities 
            self.current_num_entities = 0

            # instantiate a zookeeper client object
            # right now only one host; it could be the ensemble
            hosts = self.zkIPAddr + str (":") + str (self.zkPort)
            print ("Driver::init_driver -- instantiate zk obj: hosts = {}".format(hosts))

            # instantiate the kazoo client object
            self.zk = KazooClient (hosts)

            # register it with the state listener.
            # recall that the "listener4state" is a callback method
            # we defined above and so we are just passing the pointer
            # to this callback to the listener method on kazoo client.
            self.zk.add_listener (listener4state)
            print ("Driver::init_driver -- state after connect = {}".format (self.zk.state))
            
        except:
            print ("Unexpected error in init_driver:", sys.exc_info()[0])
            raise


    # -----------------------------------------------------------------------
    # A watcher function to see if value for a node in the znode tree
    # has changed
    # -----------------------------------------------------------------------
    def watch_znode_data_change (self):

        # we don't do anything inside this function but rather set an
        # actual watch function
        
        #*****************************************************************
        # This is the watch callback function that is supposed to be invoked
        # when changes get made to the znode of interest. Note that a watch is
        # effective only once. So the client has to set the watch every time.
        # To overcome the need for this, Kazoo has come up with a decorator.
        # Decorators can be of two kinds: watching for data on a znode changing,
        # and children on a znode changing
        @self.zk.DataWatch(self.zkName)
        def dump_data_change (data, stat):
            print ("\n*********** Inside watch_znode_data_change *********")
            print(("Data changed for znode: data = {}, stat = {}".format (data,stat)))
            print ("*********** Leaving watch_znode_data_change *********")

    

    # -----------------------------------------------------------------------
    # start a session with the zookeeper server
    #
    def start_session (self):
        """ Starting a Session """
        try:
            # now connect to the server
            self.zk.start ()

        except:
            print("Exception thrown in start (): ", sys.exc_info()[0])
            return

    # -----------------------------------------------------------------------
    # stop a session with the zookeeper server
    #
    def stop_session (self):
        """ Stopping a Session """
        try:
            #
            # now disconnect from the server
            self.zk.stop ()

        except:
            print("Exception thrown in stop (): ", sys.exc_info()[0])
            return

    # -----------------------------------------------------------------------
    # create a znode
    #
    def create_znode (self, zk_name, zk_value):
        """ ******************* znode creation ************************ """
        try:
            print ("Creating an ephemeral znode {} with value {}".format(self.zkName,self.zkVal))
            self.zk.create (self.zkName, value=self.zkVal, ephemeral=True, makepath=True)

        except:
            print("Exception thrown in create (): ", sys.exc_info()[0])
            return
        
     # -----------------------------------------------------------------------
    # Retrieve the value stored at a znode
    def get_znode_value (self, zk_name):
        
        """ ******************* retrieve a znode value  ************************ """
        try:

            # Now we are going to check if the znode that we just created
            # exists or not. Note that a watch can be set on create, exists
            # and get/set methods
            print ("Checking if {} exists (it better be)".format(self.zkName))
            if self.zk.exists(zk_name):
                print ("{} znode indeed exists; get value".format(self.zkName))

                # Now acquire the value and stats of that znode
                #value,stat = self.zk.get (self.zkName, watch=self.watch)
                value,stat = self.zk.get(zk_name)
                print(("Details of znode {}: value = {}, stat = {}".format (zk_name, value, stat)))

            else:
                print ("{} znode does not exist, why?".format(self.zkName))

        except:
            print("Exception thrown checking for exists/get: ", sys.exc_info()[0])
            return

    # -----------------------------------------------------------------------
    # Modify the value stored at a znode
    def modify_znode_value (self, zk_name, new_val):
        
        """ ******************* modify a znode value  ************************ """
        try:
            # Now let us change the data value on the znode and see if
            # our watch gets invoked
            print ("Setting a new value = {} on znode {}".format (new_val, self.zkName))

            # make sure that the znode exists before we actually try setting a new value
            if self.zk.exists (zk_name):
                print ("{} znode still exists :-)".format(self.zkName))

                print ("Setting a new value on znode")
                self.zk.set (zk_name, new_val)

                # Now see if the value was changed
                value,stat = self.zk.get (self.zkName)
                print(("New value at znode {}: value = {}, stat = {}".format (zk_name, value, stat)))

            else:
                print ("{} znode does not exist, why?".format(zk_name))

        except:
            print("Exception thrown checking for exists/set: ", sys.exc_info()[0])
            return

    # -----------------------------------------------------------------------
    # -----------------------------------------------------------------------
    # -----------------------------------------------------------------------
    # Run the driver
    #
    # We do a whole bunch of things to demonstrate the use of ZooKeeper
    # Note that as you are trying this out, use the ZooKeeper CLI to verify
    # that indeed these things are happening on the server (just as a validation)
    # -----------------------------------------------------------------------
    def run_driver (self):
        """The actual logic of the driver program """
        try:
            # now start playing with the different CLI commands programmatically

            # first step is to start a session
            self.start_session ()
 
            while (self.server_active):
                pass
           
            # disconnect once again
            self.stop_session ()

            # cleanup
            self.zk.close ()

        except:
            print("Exception thrown: ", sys.exc_info()[0])

    ##############################
    # Store info on an entity in the systen
    #
    ##############################
    def add_entity(self, entity_to_write):
        result = False

        try:
            self.logger.info("ZookeeperClient::add_entity")

            # Check the role of the entity we are registering
            node_directory = self.get_node_directory(entity_to_write)

            # Create the new node
            self.create_znode(node_directory + entity_to_write.name, entity_to_write)
            
            # Increment number of nodes
            self.current_num_entities = self.current_num_entities + 1

            # Return true if the create works as expected
            result = True
            self.logger.info("ZookeeperClient::add_entity - Created node {}".format(node_directory + entity_to_write.name))

        except Exception as e:
            result = False
            raise e

        return result

    ##############################
    # Read info on an entity in the systen
    #
    ##############################
    def read_entity(self, entity_to_read):
        try:
            self.logger.info("ZookeeperClient::read_entity")

             # Check the role of the entity we are reading
            node_directory = self.get_node_directory(entity_to_read)

            entity = self.read_entity(node_directory + entity_to_read.name)

            self.logger.info("ZookeeperClient::read_entity - Read success")
        except Exception as e:
            entity = None
            raise e

        return entity

    ##############################
    # Delete entity in the systen
    #
    ##############################
    def delete_entity(self, entity_to_delete):
        result = False

        try:
            self.logger.info("ZookeeperClient::delete_entity")

            # Check the role of the entity we are reading
            node_directory = self.get_node_directory(entity_to_delete)

            self.delete_entity(node_directory + entity_to_delete)

            # Decrement number of nodes
            self.current_num_entities = self.current_num_entities -1

            # The result is true if the action happens with no error
            result = True
            self.logger.info("ZookeeperClient::read_entity - Deleted {} successfully".format(entity_to_delete))

        except Exception as e:
            raise e

        return result

    def get_node_directory(entity): 
        # We will be grouping the nodes based on role
        if entity.role == discovery_pb2.ROLE_SUBSCRIBER:
            node_directory = "/sub/"
        elif entity.role == discovery_pb2.ROLE_PUBLISHER:
            node_directory = "/pub/"
        elif entity.role == discovery_pb2.ROLE_BOTH:
            node_directory = "/broker/"

        return node_directory

##################################
# Command line parsing
##################################
def parseCmdLineArgs ():
    # parse the command line
    parser = argparse.ArgumentParser ()

    # add optional arguments
    parser.add_argument ("-a", "--zkIPAddr", default="10.0.0.1", help="ZooKeeper server ip address, default 127.0.0.1")
    parser.add_argument ("-p", "--zkPort", type=int, default=2181, help="ZooKeeper server port, default 2181")
    parser.add_argument ("-n", "--zkName", default="/foo", help="ZooKeeper znode name, default /foo")
    parser.add_argument ("-v", "--zkVal", default=b"bar", help="ZooKeeper znode value at that node, default 'bar'")
    parser.add_argument ("-l", "--loglevel", type=int, default=logging.DEBUG, choices=[logging.DEBUG,logging.INFO,logging.WARNING,logging.ERROR,logging.CRITICAL], help="logging level, choices 10,20,30,40,50: default 20=logging.INFO")
    parser.add_argument ("-e", "--totalEntities", type=int, default=14, help="Provide the number of entities expected in the system")

    # parse the args
    return parser.parse_args ()
    
#*****************************************************************
# main function
def main ():
    """ Main program """

     # obtain a system wide logger and initialize it to debug level to begin with
    logging.info ("Main - acquire a child logger and then log messages in the child")
    logger = logging.getLogger("ZooKeeperClient")

    print ("Zookeeper Client for PA 3")
    parsed_args = parseCmdLineArgs ()

    # reset the log level to as specified
    logger.debug("Main: resetting log level to {}".format(parsed_args.loglevel))
    logger.setLevel(parsed_args.loglevel)
    logger.debug("Main: effective log level is {}".format(logger.getEffectiveLevel ()))
    
    # invoke the driver program
    driver = ZK_Driver (parsed_args, logger)

    # initialize the driver
    driver.init_driver ()
    
    # This is the test code
    # start the driver
    # driver.run_driver ()

#----------------------------------------------
if __name__ == '__main__':


    # set underlying default logging capabilities
    logging.basicConfig (level=logging.DEBUG,
                       format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


    main ()