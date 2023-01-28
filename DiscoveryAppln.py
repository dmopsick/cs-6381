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

##################################
#       DiscoveryAppln class
##################################
class DiscoveryAppln():
    
      # these are the states through which our publisher appln object goes thru.
    # We maintain the state so we know where we are in the lifecycle and then
    # take decisions accordingly
    class State (Enum):
        INITIALIZE = 0,
        CONFIGURE = 1,
        REGISTER = 2,
        ISREADY = 3,
        COMPLETED = 4

    def __init__ (self, logger):
        self.totalPublishers = None
        self.totalSubscribers = None
        self.mw_obj = None # handle to the underlying Middleware object
        self.logger = logger  # internal logger for print statements
        self.res = None
        self.publisherList = []
        self.subscriberList = []

    def configure(self, args):
        ''' Initialize the object '''

        try:
            # Here we initialize any internal variables
            self.logger.info ("DiscoveryAppln::configure")

            # Set our current state to Configure state
            self.state = self.State.CONFIGURE

            # Initialize our variables
            self.totalPublishers = args.totalPublishers
            self.totalSubscribers = args.totalSubscribers
            
            # Now get the configuration object
            self.logger.debug("DiscoveryAppln::configure - parsing config.ini")
            config = configparser.ConfigParser ()
            config.read (args.config)

            # Set up underlying middleware object
            self.logger.debug("DiscoveryAppln::configure - initialize the middleware object")
            self.mw_obj = DiscoveryMW(self.logger)
            self.mw_obj.configure(args) # pass remainder of the args to the m/w object
            
            self.logger.info("DiscoveryAppln::configure - configuration complete")
      
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


    def invoke_operation(self):
        ''' Invoke operating depending on the state '''

        try:
            self.logger.info ("DiscoveryAppln::invoke_operation")

            if (self.state == self.State.REGISTER):
                # Need to start the event loop to take in registrations of pubs and subs
                # Until we reach the defined amount of pubs and subs
                # Do we do that here?

                # Need to have some info here from the MW level that has the 
                # Type of entity we are registering (sub or pub)
                # Address, port, and topic list

                # Need to store the entity in the appropriate list

                # Check if we have the specified amount of publishers and subscribers
                if (len(self.subscriberList) == self.totalSubscribers and len(self.publisherList) == self.totalPublishers):
                    # We are ready, so return true
                    return True
                else:
                    # We must continue looping and waiting for more connections
                    return False

            elif (self.state == self.State.ISREADY):
                # Send the is_ready response to users?
                pass

            # Need to handle the actual registration

            # increment the total of sub or pub based on what we are adding

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
  
    parser.add_argument ("-a", "--addr", default="localhost", help="IP addr of this publisher to advertise (default: localhost)")

    parser.add_argument ("-p", "--port", type=int, default=5577, help="Port number on which our underlying publisher ZMQ service runs, default=5577")

    parser.add_argument ("-P", "--num_publishers", type=int, choices=range(1,50), default=1, help="Number of publishers to build for the system")

    parser.add_argument ("-S", "--num_subscribers", type=int, choices=range(1,50), default=1, help="Number of subscribers to build for the system")

def main():
    try:
        # obtain a system wide logger and initialize it to debug level to begin with
        logging.info ("Main - acquire a child logger and then log messages in the child")
        logger = logging.getLogger("DiscoveryAppln")

        # obtain a system wide logger and initialize it to debug level to begin with
        logging.info ("Main - acquire a child logger and then log messages in the child")
        logger = logging.getLogger("DiscoveryAppln")

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
  