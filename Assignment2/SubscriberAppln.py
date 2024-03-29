###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the subscriber application
#
# Created: Spring 2023
#
###############################################


# This is left as an exercise to the student. Design the logic in a manner similar
# to the PublisherAppln. As in the publisher application, the subscriber application
# will maintain a handle to the underlying subscriber middleware object.
#
# The key steps for the subscriber application are
# (1) parse command line and configure application level parameters
# (2) obtain the subscriber middleware object and configure it.
# (3) As in the publisher, register ourselves with the discovery service
# (4) since we are a subscriber, we need to ask the discovery service to
# let us know of each publisher that publishes the topic of interest to us. Then
# our middleware object will connect its SUB socket to all these publishers
# for the Direct strategy else connect just to the broker.
# (5) Subscriber will always be in an event loop waiting for some matching
# publication to show up. We also compute the latency for dissemination and
# store all these time series data in some database for later analytics.

# import the needed packages
import os     # for OS functions
import sys    # for syspath and system exception
import time   # for sleep
import argparse # for argument parsing
import configparser # for configuration parsing
import logging # for logging. Use it in place of print statements.
import datetime
import csv

from topic_selector import TopicSelector

from CS6381_MW.SubscriberMW import SubscriberMW 

# We also need the message formats to handle incoming responses.
from CS6381_MW import discovery_pb2

# Import the constants for the dissemination strategy
from CS6381_MW.Common import Constants

from enum import Enum  # for an enumeration we are using to describe what state we are in

class SubscriberAppln():

    # these are the states through which our subscriber appln object goes thru.
    # We maintain the state so we know where we are in the lifecycle and then
    # take decisions accordingly
    class State (Enum):
        INITIALIZE = 0,
        CONFIGURE = 1,
        REGISTER = 2,
        QUERY_PUBS = 3,
        CONSUME = 4,
        COMPLETED = 5

    ########################################
    # Constructor
    #
    ########################################
    def __init__(self, logger):
        self.state = self.State.INITIALIZE # state that are we in
        self.name = None # our name (some unique name)
        self.lookup = None # one of the diff ways we do lookup
        self.mw_obj = None # handle to the underlying Middleware object
        self.logger = logger  # internal logger for print statements
        self.num_topics = None # total num of topics the subscriber is interested in
        self.topiclist = None # the different topics that the subscriber is interested in
        self.frequency = None # rate at which consumption takes place
        self.receivedPublicationList = []
        self.dissemination = None # Hold the dissemination strategy
        self.iters = None # Number of iterations to receive data
        self.dht_file_name = None
        # For logging for graphing purposes
        self.register_send_time = None
        self.register_response_time = None

    ########################################
    # Set up initial configuration for our subscriber
    # 
    ########################################
    def configure (self, args):
        ''' Initialize subscriber object '''

        try:
            # Initialize internal variables
            self.logger.debug("SubscriberAppln::configure")

            # set our current state to CONFIGURE state
            self.state = self.State.CONFIGURE

            # Initialize our variables
            self.name = args.name # our name
            self.iters = args.iters # Number of iterations
            self.frequency = args.frequency
            self.num_topics = args.num_topics
            self.dht_file_name = args.dht_name

            # Get the configuration object
            self.logger.debug("SubscriberAppln::configure - parsing config.ini")
            config = configparser.ConfigParser ()
            config.read(args.config)
            # Load the specified lookup and dissemination strategy
            self.lookup = config["Discovery"]["Strategy"]
            self.dissemination = config["Dissemination"]["Strategy"]

            # Now get the list of topics that this subscriber will be interested in
            self.logger.debug ("SubscriberAppln::configure - selecting our topic list")
            ts = TopicSelector()
            self.topiclist = ts.interest(self.num_topics)  # let topic selector give us the desired num of topics

            # Setup the underlying middleware object to delegate everything
            self.logger.debug ("SubscriberAppln::configure - initialize the middleware object")
            self.mw_obj = SubscriberMW(self.logger)
            self.mw_obj.configure(args) # pass remainder of the args to the m/w object
      
            self.logger.debug("SubscriberAppln::configure - configuration complete")

            pass
        except Exception as e:
            raise e

    ########################################
    # driver program
    #
    ########################################
    def driver (self):
        ''' Driver program '''

        try:
            self.logger.debug("SubscriberAppln::driver")

            # Dump out comments for debugging purposes
            self.dump()

            # Ask the middleware to keep a handle on us to make upcalls
            # Pass a point of this object to the middleware
            self.logger.debug("SubscriberAppln::driver - Set the upcall handle")
            self.mw_obj.set_upcall_handle(self)

            # Enter the register state
            # Must register the subscriber with the Discovery service
            self.state = self.State.REGISTER

            # Pass control to the event loop
            # When the event loops is done with what it's doing, it will call back
            # To the application code
            self.mw_obj.event_loop(timeout=0)  # start the event loop

            self.logger.info("SubscriberAppln::driver completed")

        except Exception as e:
            raise e

    ########################################
    # Dump the contents of the object 
    #
    ########################################
    def dump (self):
        ''' Pretty print '''
        try:
            self.logger.info ("**********************************")
            self.logger.info ("SubscriberAppln::dump")
            self.logger.info ("------------------------------")
            self.logger.info ("     Name: {}".format (self.name))
            self.logger.info ("     Lookup: {}".format (self.lookup))
            self.logger.info ("     Num Topics: {}".format (self.num_topics))
            self.logger.info ("     TopicList: {}".format (self.topiclist))
            self.logger.info ("**********************************")

        except Exception as e:
            raise e
        
    ########################################
    # generic invoke method called as part of upcall
    #
    # This method will get invoked as part of the upcall made
    # by the middleware's event loop after it sees a timeout has
    # occurred.
    ########################################
    def invoke_operation(self):
        ''' Invoke operating depending on state '''
        try:
            self.logger.info("SubscriberAppln::invoke_operation")

            # check what state are we in. If we are in REGISTER state,
            # we send register request to discovery service. If we are in
            # ISREADY state, then we keep checking with the discovery
            # service.
            if (self.state == self.State.REGISTER):
                # send a register msg to discovery service 
                # Include the list of topics the subscriber is interested in
                self.logger.debug ("SubscriberAppln::invoke_operation - register with the discovery service")
                self.mw_obj.register (self.name, self.topiclist)

                # Remember that we were invoked by the event loop as part of the upcall.
                # So we are going to return back to it for its next iteration. Because
                # we have just now sent a register request, the very next thing we expect is
                # to receive a response from remote entity. So we need to set the timeout
                # for the next iteration of the event loop to a large num and so return a None.
                return None
            elif (self.state == self.State.QUERY_PUBS):
                self.logger.debug ("SubscriberAppln::invoke_operation - Query for a list of publishers based on our topic list")
                
                # Use the MW object to send a look up publishers by topic list request
                self.mw_obj.lookup_publishers_by_topiclist(self.topiclist)

                # Remember that we were invoked by the event loop as part of the upcall.
                # So we are going to return back to it for its next iteration. Because
                # we have just now sent a isready request, the very next thing we expect is
                # to receive a response from remote entity. So we need to set the timeout
                # for the next iteration of the event loop to a large num and so return a None.
                return None
            elif (self.state == self.State.CONSUME):
                # We are connected... now we CONSUME the data
                self.logger.debug ("SubscriberAppln::invoke_operation - start Consuming data")

                # Only want to consume the defined amount of times
                for i in range(self.iters):

                    publication = self.mw_obj.consume()

                    # self.logger.info("Received data: {}".format(publication))

                    # Timestamp of receiving the message
                    receivedTimestamp = datetime.datetime.now().timestamp()

                    # Find the duration between the timestamps
                    # latency = publication.tstamp - receivedTimestamp
                    latency = receivedTimestamp - publication.tstamp

                    # self.logger.info("PublisherAppln Latency: {}".format(latency))

                    # Change the publication's timestamp back into a datetime to display
                    # publicationTimestamp = datetime.datetime.fromtimestamp(publication.tstamp)
                    # self.logger.debug(publicationTimestamp)
                    # Store the converted timestamp in the message
                    # publication.tstamp = publicationTimestamp

                    # Make the publication and latency a set
                    publicationTuple = (publication, latency)

                    # Add the data to some list to export to a csv?
                    self.receivedPublicationList.append(publicationTuple)

                    self.logger.info("SubscriberAppln::invoke_operation -  Received Data: {}".format(publicationTuple))

                    # self.logger.debug ("SubscriberAppln::invoke_operation - Consumption completed")

                    # Now sleep for an interval of time to ensure we consume at the
                    # frequency that was configured.
                    time.sleep (1/float (self.frequency)) 

                self.logger.debug("SubscriberAppln::invoke_operation - Consumption completed")

                # We are done consuming
                self.state = self.State.COMPLETED

                # Timeout after the sleep is done
                return 0
            elif (self.state == self.State.COMPLETED):
                # At this time the consumer will never know when it ends up being done
                # Perhaps in a later iteration
                
                self.logger.debug ("SubscriberAppln::invoke_operation - Subscriber lifecycle completed. Writing CSV")

                # Write out the list of the publications received into a a csv for graphing
                with open("./csv/" + self.name + "_" + self.dissemination.lower() + "_output.csv", "w", newline="") as f:
                    # Create write variable 
                    writer = csv.writer(f)

                    # Write the header for the csv
                    rowHeaders = ['topic', 'content', 'publisher_id', 'timestamp', 'latency']

                    # Write the header for the csv 
                    writer.writerow(rowHeaders)

                    # Write each tuple as a row in the csv 
                    for publicationTuple in self.receivedPublicationList:
                        # Get the publication record, first record in the tuple we build
                        publication = publicationTuple[0]

                        # Get date string from the timestamp
                        timestampString = datetime.datetime.fromtimestamp(publication.tstamp).isoformat()

                        # Get the latency, second record in the tuple we build
                        latency = publicationTuple[1]

                        # Build the string to write
                        rowToWrite = [publication.topic,  publication.content, publication.pub_id, timestampString, str(latency)]

                        # Turn each element in our list to a row in the csv
                        writer.writerow(rowToWrite)

                self.logger.debug ("SubscriberAppln::invoke_operation - CSV written")
                    
                # we are done. Time to break the event loop. So we created this special method on the
                # middleware object to kill its event loop
                self.mw_obj.disable_event_loop()
                return None
            else:
                raise ValueError("Undefined state of the appln object")

        except Exception as e:
            raise e
    
    ########################################
    # Handle register response method called as part of upcall
    #
    ########################################
    def register_response(self, reg_resp):
        ''' Handle register response from discovery server '''
    
        try:
            self.logger.info("SubscriberAppln::register_response")

            if (reg_resp.status == discovery_pb2.STATUS_SUCCESS):
                self.logger.debug("SubscriberAppln::register_response - registration is a success")

                # Mark the time the register response was collected
                self.register_response_time = datetime.datetime.now().timestamp()

                # Calculate latency
                latency = self.register_response_time - self.register_send_time

               # Write out the list of the publications received into a a csv for graphing
                with open("./csv/" + self.name + "_dht_disc_output.csv", "w", newline="") as f:
                    # Create write variable 
                    writer = csv.writer(f)

                    # Write the header for the csv
                    rowHeaders = ['latency']

                    # Write the header for the csv 
                    writer.writerow(rowHeaders)

                    # Turn each element in our list to a row in the csv
                    writer.writerow([latency])

                # Invoke the MW logic to subscribe to our list of topics now that we are registered
                # I do not think I actually need to do this? 
                # I thikn I can subscribe when I connect to each publisher
                self.mw_obj.subscribe(self.topiclist)

                # Now that we are connected we must look up a list
                # of publishers based on our topics we are interested in
                self.state = self.State.QUERY_PUBS

                # Return a timeout of zero to the event loop 
                return 0
            else:
                self.logger.debug("SubscriberAppln::register_response - registration failed for the following reason: {}".format(reg_resp.reason))

        except Exception as e:
            raise e

    ########################################
    # Handle lookup publisher list by topic list response 
    #
    ########################################
    def lookup_publisher_list_response(self, lookup_resp):
        ''' Handle the response to a lookup publisher list by topic list request '''

        try:
            self.logger.info("SubscriberAppln::lookup_publisher_list_response")

            if (lookup_resp.status == discovery_pb2.STATUS_SUCCESS):
                self.logger.debug("SubscriberAppln::lookup_publisher_list_response - Success! List of publishers provided from Discovery")

                # Connect to each of list of publishers 
                for publisher in lookup_resp.publisher_list:
                    self.logger.debug("SubscriberAppln::lookup_publisher_list_response - Connecting to publisher {} {}:{}".format(publisher.id, publisher.addr, publisher.port))
                    
                    # Connect to this publisher for the topics we are interested in via MW
                    self.mw_obj.connect_to_publisher(publisher.addr, publisher.port, self.topiclist)

                self.logger.debug("SubscriberAppln::lookup_publisher_list_response - Done connecting to publishers")
       
                # Change the state to CONSUME time for us to just accept data
                self.state = self.State.CONSUME

            elif (lookup_resp.status == discovery_pb2.STATUS_CHECK_AGAIN):
                 # Discovery service is not ready yet to give out list of pubs yet
                self.logger.debug ("SubscriberAppln::lookup_publisher_list_response - Not ready yet; check again")
                time.sleep(10)  # sleep between calls so that we don't make excessive calls

            else:
                raise ValueError ("Unexpected status provided from Discovery for the lookup publisher list request")

            # Return time out 0 to continue the logic
            return 0        

        except Exception as e:
            raise e
            
###################################
# Parse command line arguments
#
###################################
def parseCmdLineArgs ():
    parser = argparse.ArgumentParser (description="Publisher Application")

    # Now specify all the optional arguments we support
    # At a minimum, you will need a way to specify the IP and port of the lookup
    # service, the role we are playing, what dissemination approach are we
    # using, what is our endpoint (i.e., port where we are going to bind at the
    # ZMQ level)

    parser.add_argument("-n", "--name", default="pub", help="Some name assigned to us. Keep it unique per publisher")

    parser.add_argument("-a", "--addr", default="localhost", help="IP addr of this publisher to advertise (default: localhost)")

    parser.add_argument("-p", "--port", default=5566, help="Port number on which our underlying publisher ZMQ service runs, default=5566")
        
    parser.add_argument("-d", "--discovery", default="localhost:5556", help="IP Addr:Port combo for the discovery service, default localhost:5556")

    parser.add_argument ("-T", "--num_topics", type=int, choices=range(1,10), default=1, help="Number of topics to publish, currently restricted to max of 9")

    parser.add_argument("-c", "--config", default="config.ini", help="configuration file (default: config.ini)")

    parser.add_argument("-f", "--frequency", type=int,default=1, help="Rate at which topics disseminated: default once a second - use integers")

    parser.add_argument("-i", "--iters", type=int, default=1000, help="number of publication iterations (default: 1000)")

    parser.add_argument("-l", "--loglevel", type=int, default=logging.DEBUG, choices=[logging.DEBUG,logging.INFO,logging.WARNING,logging.ERROR,logging.CRITICAL], help="logging level, choices 10,20,30,40,50: default 10=logging.DEBUG")
  
    parser.add_argument ("-j", "--dht_name", default="dht.json", help="Enter the name of the distributed hash table to use")

    return parser.parse_args()

###################################
# Main program
#
###################################
def main():
    try:
        # obtain a system wide logger and initialize it to debug level to begin with
        logging.debug("Main - acquire a child logger and then log messages in the child")
        logger = logging.getLogger("SubAppln")

        # Parse the provided command line arguments
        logger.debug("Main: parse command line arguments")
        args = parseCmdLineArgs()

        # Reset the log level to to the specified level in the arguments
        logger.debug ("Main: resetting log level to {}".format (args.loglevel))
        logger.setLevel(args.loglevel)
        logger.debug ("Main: effective log level is {}".format (logger.getEffectiveLevel ()))

        # Obtain a subscriber application 
        logger.debug("Main: obtain the object")
        sub_app = SubscriberAppln(logger)

        # Configure the objject
        sub_app.configure(args)

        # Invoke the driver program
        sub_app.driver()

    except Exception as e:
       logger.error("Exception caught in main - {}".format (e)) 

###################################
# Main entry point
#
###################################
if __name__ == "__main__":
    # set underlying default logging capabilities
  logging.basicConfig (level=logging.DEBUG,
                       format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

  main()
