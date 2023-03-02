###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the Discovery application
#
# Created: Spring 2023
#
###############################################


# This is left as an exercise for the student.  The Discovery service is a server
# and hence only responds to requests. It should be able to handle the register,
# is_ready, the different variants of the lookup methods. etc.
#
# The key steps for the discovery application are
# (1) parse command line and configure application level parameters. One
# of the parameters should be the total number of publishers and subscribers
# in the system.
# (2) obtain the discovery middleware object and configure it.
# (3) since we are a server, we always handle events in an infinite event loop.
# See publisher code to see how the event loop is written. Accordingly, when a
# message arrives, the middleware object parses the message and determines
# what method was invoked and then hands it to the application logic to handle it
# (4) Some data structure or in-memory database etc will need to be used to save
# the registrations.
# (5) When all the publishers and subscribers in the system have registered with us,
# then we are in a ready state and will respond with a true to is_ready method. Until then
# it will be false.

# import the needed packages
import os     # for OS functions
import sys    # for syspath and system exception
import time   # for sleep
import argparse # for argument parsing
import configparser # for configuration parsing
import logging # for logging. Use it in place of print statements.
import zmq  # ZMQ sockets

# Now import our CS6381 Middleware
from CS6381_MW.DiscoveryMW import DiscoveryMW
# We also need the message formats to handle incoming responses.
from CS6381_MW import discovery_pb2

# import any other packages you need.
from enum import Enum  # for an enumeration we are using to describe what state we are in

# Import the constants for the dissemination strategy
from CS6381_MW.Common import Constants

# Simple data models I created to hold info about publishers and subscribers
from CS6381_MW.Common import Entity

from chord_finger_table import FingerTableBuilder

ADDRESS_SPACE = 8

##################################
#       DiscoveryAppln class
##################################
class DiscoveryAppln():

    # At this time I only want one broker, maybe one day I want more
    DEFAULT_NUM_BROKERS = 1
    
    class State (Enum):
        INITIALIZE = 0,
        CONFIGURE = 1,
        REGISTER = 2,
        COMPLETED = 3

    def __init__ (self, logger):
        self.name = None
        self.specified_num_publishers = None
        self.specified_num_subscribers = None
        self.specified_num_brokers = None
        self.mw_obj = None # handle to the underlying Middleware object
        self.logger = logger  # internal logger for print statements
        self.publisher_list = [] # Discovery
        self.subscriber_list = []
        self.broker_list = []
        self.lookup = None
        self.dissemination = None
        self.dht_file_name = None
        self.finger_table = None

    def configure(self, args):
        ''' Initialize the object '''

        try:
            # Here we initialize any internal variables
            self.logger.info ("DiscoveryAppln::configure")

            # Set our current state to Configure state
            self.state = self.State.CONFIGURE

            # Initialize our variables
            self.name = args.name
            self.specified_num_publishers = args.num_publishers
            self.specified_num_subscribers = args.num_subscribers
            self.specified_num_brokers = self.DEFAULT_NUM_BROKERS
            self.dht_file_name = args.dht_name
            
            # Now, get the configuration object
            self.logger.debug ("DiscoveryAppln::configure - parsing config.ini")
            config = configparser.ConfigParser ()
            config.read (args.config)
            # What do these values mean and where do they come from?
            # They come from our config.ini
            self.lookup = config["Discovery"]["Strategy"]
            self.dissemination = config["Dissemination"]["Strategy"]

            # Set up underlying middleware object
            self.logger.debug("DiscoveryAppln::configure - initialize the middleware object")
            self.mw_obj = DiscoveryMW(self.logger)
            self.mw_obj.configure(args) # pass remainder of the args to the m/w object

            # Create a finger table for this discovery service
            self.finger_table = FingerTableBuilder.create_finger_table(self.name, self.dht_file_name, ADDRESS_SPACE)

            self.logger.debug("DiscoveryAppln::configure - created Finger table: ")
            self.logger.debug(self.finger_table)
            
            self.logger.info("DiscoveryAppln::configure - configuration complete")
      
        except Exception as e:
            raise e
    
    ########################################
    # Dump the contents of the object 
    #
    ########################################
    def dump (self):
        ''' Pretty print '''
        # What else do I want to print out here?
        try:
            self.logger.info ("**********************************")
            self.logger.info ("DiscoveryAppln::dump")
            self.logger.info ("------------------------------")
            self.logger.info ("     Num Publishers: {}".format (self.name))
            self.logger.info ("     Num Publishers: {}".format (self.specified_num_publishers))
            self.logger.info ("     Num Subscribers: {}".format (self.specified_num_subscribers))
            self.logger.info ("**********************************")

        except Exception as e:
            raise e


    def driver(self):
        ''' Discovery driver program '''
        try:
            self.logger.info("DiscoveryAppln::driver")

            # dump our contents (debugging purposes)
            self.dump()

            # First ask our middleware to keep a handle to us to make upcalls.
            # This is related to upcalls. By passing a pointer to ourselves, the
            # middleware will keep track of it and any time something must
            # be handled by the application level, invoke an upcall.
            self.logger.debug ("DiscoveryAppln::driver - upcall handle")
            self.mw_obj.set_upcall_handle(self)

            # Set to the register state
            # We want to accept registrations from pubs and subs
            self.state = self.State.REGISTER

            # Start the event loop in the MW to handle events
            self.mw_obj.event_loop (timeout=0)  # start the event loop
        
            self.logger.info ("PublisherAppln::driver completed")
        except Exception as e:
            raise e

    ########################################
    # Handle the register request function as part of the upcall
    #
    # Here is where the meat and potatoes of the registering subs and pubs go
    #######################################
    def register_request(self, reg_req):
        ''' Handle register request '''

        try:
            self.logger.info("DiscoveryAppln::register_request")

            # Load the role of the entity attempting to register
            role = reg_req.role

            # Handle registration differently based on the role of the entity attempting to register
            if (role == discovery_pb2.ROLE_PUBLISHER):
                self.logger.info("DiscoveryAppln::register_request Registering a publisher")

                # Verify that there is still room for publishers in the system
                if (len(self.publisher_list) < self.specified_num_publishers):
                    self.logger.debug("DiscoveryAppln::register_request Creating a new publisher record")
                   
                    # Create a new publisher record
                    publisher = Entity()

                    # Load the publisher with values from RegistrantInfo
                    publisher.role = discovery_pb2.ROLE_BOTH
                    publisher.name = reg_req.info.id
                    publisher.ip_address = reg_req.info.addr
                    publisher.port = reg_req.info.port
                    publisher.topic_list = reg_req.topiclist

                    # Add the created object to the list of publishers registered
                    self.publisher_list.append(publisher)

                    # Set status to success if we have gotten this far
                    status = discovery_pb2.STATUS_SUCCESS

                    # No reason to send
                    reason = None
                    
                    self.logger.debug("DiscoveryAppln::register_request Done creating a new publisher record")
                   
                else:
                    self.logger.info("DiscoveryAppln::register_request Publisher attempting to register, but no more publisher roles are allocated")

                    # Set status to failure
                    status = discovery_pb2.STATUS_FAILURE

                    # Pass in a reason to let the registrant know why it failed
                    reason = "Max publishers already reached for this system"

                # Send a register reply with the MW
                self.mw_obj.send_register_response(status, reason)

                self.logger.info("DiscoveryAppln::register_request Done registering a publisher")

            elif (role == discovery_pb2.ROLE_SUBSCRIBER):
                self.logger.info("DiscoveryAppln::register_request Registering a subscriber")

                # Verify that there is still room for subscribers in the system
                if (len(self.subscriber_list) < self.specified_num_subscribers):
                    self.logger.debug("DiscoveryAppln::register_request Creating a new subscriber record")
                    # Create new subscriber object
                    subscriber = Entity()

                    # Load the subscriber values from registrant info
                    subscriber.role = discovery_pb2.ROLE_SUBSCRIBER
                    subscriber.name = reg_req.info.id
                    subscriber.ip_address = reg_req.info.addr
                    subscriber.port = reg_req.info.port
                    subscriber.topic_list = reg_req.topiclist

                    # Add the created object to the list of publishers registered
                    self.subscriber_list.append(subscriber)

                    # Set status to success if we have gotten this far
                    status = discovery_pb2.STATUS_SUCCESS

                    # No reason to send
                    reason = None
                    
                    self.logger.debug("DiscoveryAppln::register_request Done creating a new subscriber record")

                else:
                    self.logger.info("DiscoveryAppln::register_request Subscriber attempting to register, but no more subscriber roles are allocated")

                    # Set status to failure
                    status = discovery_pb2.STATUS_FAILURE

                    # Pass in a reason to let the registrant know why it failed
                    reason = "Max subscribers already reached for this system"
                
                # Send a register reply with the MW
                self.mw_obj.send_register_response(status, reason)

                self.logger.info("DiscoveryAppln::register_request Done registering a subscriber")
            elif (role == discovery_pb2.ROLE_BOTH):
                self.logger.info("DiscoveryAppln::register_request Registering a broker")

                # Check if specified number of brokers is met 
                # For now hard coding one broker but perhaps one day we want multiple
                if (len(self.broker_list) < self.specified_num_brokers):
                    self.logger.debug("DiscoveryAppln::register_request Creating a new broker record")
                    # Create new Entity object
                    broker = Entity()

                    # Load the subscriber values from registrant info
                    broker.role = discovery_pb2.ROLE_BOTH
                    broker.name = reg_req.info.id
                    broker.ip_address = reg_req.info.addr
                    broker.port = reg_req.info.port
                    broker.topic_list = reg_req.topiclist

                    # Add the created object to the list of publishers registered
                    self.broker_list.append(broker)

                    # Set status to success if we have gotten this far
                    status = discovery_pb2.STATUS_SUCCESS

                    # No reason to send
                    reason = None
                    
                    self.logger.debug("DiscoveryAppln::register_request Done creating a new broker record")

                else:
                    self.logger.info("DiscoveryAppln::register_request Broker attempting to register, but no more brokers roles are allocated")

                    # Set status to failure
                    status = discovery_pb2.STATUS_FAILURE

                    # Pass in a reason to let the registrant know why it failed
                    reason = "Max brokers already reached for this system"

                # Send a register reply with the MW
                self.mw_obj.send_register_response(status, reason)

                self.logger.info("DiscoveryAppln::register_request Done registering a broker")

            else:
                self.logger.debug ("DiscoveryAppln::register_request - registration is a failure because invalid role provided")
                raise ValueError("Invalid role provided for registration request to Discovery server")

            # This register request has been handled 
            # We are not awaiting any incoming call for this logic
            # Ready to move on, so return 0
            return 0

        except Exception as e:
            raise e
    
    ############################################
    # Handle an incoming isready request
    #
    # Let whoever is asking know if the system is ready or not
    ############################################
    def isready_request(self, isready_req):
        ''' Handle isready request '''

        try:
            self.logger.info("DiscoveryAppln::is_ready_request")
            
            # No input to account for when handling an isready_request

            # Check if there required number of pubs and subs is met
            if ((len(self.subscriber_list) == self.specified_num_subscribers) and (len(self.publisher_list) ==  self.specified_num_publishers)):
                # The system is only ready when we have the specified amount of subscribers and publishers
                isready = True
            else:
                # The specified number of subscribers and publishers has not been reached
                isready = False

            # Send the isready response in the MW
            self.mw_obj.send_isready_response(isready)

            self.logger.info("DiscoveryAppln::is_ready_request Done handling isready request")

            # isready request has been handled
            # Not awaiting any incoming logic, ready to move on, return 0
            return 0

        except Exception as e:
            raise e

    ###################################################
    # Handle a look up publisher list by topic list request
    #
    ###################################################
    def lookup_pub_by_topiclist_request(self, lookup_req):
        ''' Handle a lookup pub by topic request '''

        try:
            self.logger.info("DiscoveryAppln::lookup_pub_by_topiclist_request")

            # Init the publisher by topic list 
            publisher_by_topic_list = []

            # Check the dissemination method
            if (self.dissemination == Constants.DISSEMINATION_STRATEGY_DIRECT):
                self.logger.debug("DiscoveryAppln::lookup_pub_by_topiclist_request -- Using Direct strategy")
                # Check if all the publishers have been added to the system
                if (len(self.publisher_list) == self.specified_num_publishers):
                    # Parse out the topic list from the lookup req
                    topic_list = lookup_req.topiclist  

                    # Build out the publisher list
                    for pub in self.publisher_list:
                        # Check if the publisher has any topics that match the topic list
                        # Use the any function to avoid loading duplicate publsihers
                        if (any(topic in topic_list for topic in pub.topic_list)):
                            publisher_by_topic_list.append(pub)

                    # self.logger.debug("DiscoveryAppln::lookup_pub_by_topiclist_request - Built out the following list of pubs: {}".format(publisher_by_topic_list))

                    # If the publisher list has been built, status is success
                    # Should it only be success if there is one or more pubs that match specifications?
                    # I feel like no, we have talked about scenarios when no pub for a topic
                    status = discovery_pb2.STATUS_SUCCESS
                else:
                    # Publishers not ready, check again
                    status = discovery_pb2.STATUS_CHECK_AGAIN
            elif (self.dissemination == Constants.DISSEMINATION_STRATEGY_BROKER):
                self.logger.debug("DiscoveryAppln::lookup_pub_by_topiclist_request -- Using broker strategy")
                # Make sure the broker has been added 
                if (len(self.broker_list) == self.specified_num_brokers):
                    # The broker(s) is the only thing subscribers need to describe to for 
                    # Broker dissemination
                    for broker in self.broker_list:
                        publisher_by_topic_list.append(broker)

                    self.logger.debug("DiscoveryAppln::lookup_pub_by_topiclist_request - Sending the broker list as publisher list")
                    # self.logger.debug(publisher_by_topic_list[0])

                    # The call was made succesfully 
                    status = discovery_pb2.STATUS_SUCCESS
                else:
                    self.logger.debug("DiscoveryAppln::lookup_pub_by_topiclist_request - Broker not registered check again FLAGFLAGFLAGFLAGFLAGFLAG")
                    
                    # Broker not registered, check again
                    status = discovery_pb2.STATUS_CHECK_AGAIN
            else:
                raise ValueError("ERROR: Invalid dissemination provided in the config: {}".format(self.dissemination))

            # Send the lookup_pub_by_topiclist response in the MW
            self.mw_obj.send_lookup_pub_by_topiclist_response(status, publisher_by_topic_list)

            self.logger.info("DiscoveryAppln::lookup_pub_by_topiclist_request Done handling a lookup pub list by topic list request")

            # Return timeout of 0 to return to the event loop
            return 0
        except Exception as e:
            raise e

    ################################################
    # Look up all of the publishers in the system
    #
    # Only should be usable by broker
    ################################################
    def lookup_all_publishers(self, lookup_all_resp):
        ''' Look up all publishers '''

        try:
            self.logger.info("DiscoveryAppln::lookup_all_publishers")

            all_publisher_list = []

            # Check if all the publishers have been added to the system
            if (len(self.publisher_list) == self.specified_num_publishers):
                # Return all of the publishers
                all_publisher_list = self.publisher_list

                # We got what we needed 
                status = discovery_pb2.STATUS_SUCCESS
            else:
                status = discovery_pb2.STATUS_CHECK_AGAIN

            self.logger.debug("DiscoveryAppln::lookup_all_publishers Done looking up all publishers")

            # Send a response to the look up all publisher request
            self.mw_obj.send_lookup_all_publisher_response(status, all_publisher_list)
            
        except Exception as e:
            raise e

###################################
#
# Parse command line arguments
#
###################################
def parseCmdLineArgs ():
    # instantiate a ArgumentParser object
    parser = argparse.ArgumentParser (description="Publisher Application")

    parser.add_argument("-n", "--name", default="disc", help="Some name assigned to us. Keep it unique per discovery")

    parser.add_argument("-a", "--addr", default="localhost", help="IP addr of this publisher to advertise (default: localhost)")

    parser.add_argument("-p", "--port", type=int, default=5556, help="Port number on which our underlying publisher ZMQ service runs, default=5556")

    parser.add_argument("-P", "--num_publishers", type=int, choices=range(1,50), default=1, help="Number of publishers to build for the system")

    parser.add_argument("-S", "--num_subscribers", type=int, choices=range(1,50), default=1, help="Number of subscribers to build for the system")

    parser.add_argument("-l", "--loglevel", type=int, default=logging.INFO, choices=[logging.DEBUG,logging.INFO,logging.WARNING,logging.ERROR,logging.CRITICAL], help="logging level, choices 10,20,30,40,50: default 20=logging.INFO")
    
    parser.add_argument("-c", "--config", default="config.ini", help="configuration file (default: config.ini)")

    parser.add_argument ("-j", "--dht_name", default="dht.json", help="Enter the name of the distributed hash table to use")

    return parser.parse_args()

def main():
    try:
        # obtain a system wide logger and initialize it to debug level to begin with
        logging.info("Main - acquire a child logger and then log messages in the child")
        logger = logging.getLogger("DiscoveryAppln")

        # first parse the arguments
        logger.debug("Main: parse command line arguments")
        args = parseCmdLineArgs()

        # reset the log level to as specified
        logger.debug("Main: resetting log level to {}".format(args.loglevel))
        logger.setLevel(args.loglevel)
        logger.debug("Main: effective log level is {}".format(logger.getEffectiveLevel ()))

        # Obtain a discovery application
        logger.debug("Main: obtain the discovery appln object")
        discovery_app = DiscoveryAppln(logger)

        # Configure the discovery application
        logger.debug("Main: configure the discovery appln object")
        discovery_app.configure(args)
        
        # Invoke the driver program
        logger.debug ("Main: invoke the discovery appln driver")
        discovery_app.driver ()

    except Exception as e:
        logger.error ("Exception caught in main - {}".format (e))
        return

###################################
#
# Main entry point
#
###################################
if __name__ == "__main__":

    # set underlying default logging capabilities
    logging.basicConfig (level=logging.DEBUG,
                       format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    main()
  