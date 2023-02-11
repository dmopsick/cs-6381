###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the Broker application
#
# Created: Spring 2023
#
###############################################


# This is left as an exercise for the student.  The Broker is involved only when
# the dissemination strategy is via the broker.
#
# A broker is an intermediary; thus it plays both the publisher and subscriber roles
# but in the form of a proxy. For instance, it serves as the single subscriber to
# all publishers. On the other hand, it serves as the single publisher to all the subscribers. 
# import the needed packages
import os     # for OS functions
import sys    # for syspath and system exception
import time   # for sleep
import argparse # for argument parsing
import configparser # for configuration parsing
import logging

from topic_selector import TopicSelector

# Now import our CS6381 Middleware
from CS6381_MW.BrokerMW import BrokerMW
# We also need the message formats to handle incoming responses.
from CS6381_MW import discovery_pb2

# import any other packages you need.
from enum import Enum  # for an enumeration we are using to describe what state we are in

class BrokerAppln():

    # Now import our CS6381 Middleware
    from CS6381_MW.PublisherMW import PublisherMW
    # We also need the message formats to handle incoming responses.
    from CS6381_MW import discovery_pb2

    # import any other packages you need.
    from enum import Enum  # for an enumeration we are using to describe what state we are in

    class State (Enum):
        INITIALIZE = 0,
        CONFIGURE = 1,
        REGISTER = 2,
        ISREADY = 3,
        QUERY_PUBS = 4,
        ACTIVE = 5,
        COMPLETED = 6

    ########################################
    # Constructor
    ########################################
    def __init__(self, logger):
        self.logger = logger
        self.mw_obj = None
        self.state = self.State.INITIALIZE
        self.name = None
        self.config = None

    ########################################
    # Configure/initialize
    ########################################
    def configure(self, args):
        ''' Initialize the object '''

        try:
            # Here we initialize any internal variables
            self.logger.info ("BrokerAppln::configure")

            # set our current state to CONFIGURE state
            self.state = self.State.CONFIGURE

            # Initialize our variables
            self.name = args.name

            # Now, get the configuration object
            self.logger.debug ("BrokerAppln::configure - parsing config.ini")
            config = configparser.ConfigParser ()
            config.read (args.config)

            # Get the config | Do we need to do this?
            # Isn't this file ONLY started when broker specified?
            self.lookup = config["Discovery"]["Strategy"]
            self.dissemination = config["Dissemination"]["Strategy"]

            # The broker subscribes to all topics
            self.logger.debug ("BrokerAppln::configure - selecting our topic list")
            ts = TopicSelector()
            self.topiclist = ts.interest(self.num_topics)  # let topic selector give us the desired num of topics


            # Now setup up our underlying middleware object to which we delegate
            # everything
            self.logger.debug ("BrokerAppln::configure - initialize the middleware object")
            self.mw_obj = BrokerMW(self.logger)
            self.mw_obj.configure(args) # pass remainder of the args to the m/w object
            
            self.logger.info ("PublishBrokerApplnerAppln::configure - configuration complete")
            
        except Exception as e:
            raise e

    ########################################
    # Driver program
    ########################################
    def driver(self):
        ''' Driver program '''

        try:
            self.logger.info("BrokerAppln::driver")

             # dump our contents (debugging purposes)
            self.dump()

            # Set the upcall handle on our MW
            self.logger.debug("BrokerAppln::driver - upcall handle")
            self.mw_obj.set_upcall_handle(self)

            self.state = self.State.REGISTER

            self.mw_obj.event_loop(timeout=0)

            self.logger.info("BrokerAppln::driver completed")

        except Exception as e:
            raise e

    ########################################
    # generic invoke method called as part of upcall
    #
    # This method will get invoked as part of the upcall made
    # by the middleware's event loop after it sees a timeout has
    # occurred.
    ########################################
    def invoke_operation (self):
        ''' Invoke operation depending on state '''

        try:
            self.logger.info ("BrokerAppln::invoke_operation")

            if (self.state == self.State.REGISTER):
                self.logger.debug ("BrokerAppln::invoke_operation - register with the discovery service")
                self.mw_obj.register (self.name, self.topiclist)

                # We are waiting for a reply
                return None
            elif (self.state == self.State.ISREADY):
                # Check if the system is all set up
                pass
            elif (self.state == self.State.QUERY_PUBS):
                # Load the publishers for all topics
                # Subscribe to all topics
                # Connect to all publishers
                pass
            elif (self.state == self.State.ACTIVE):
                # The system is ready
                # We have the publishers
                # Time to receive 

                # once we receive, we turn around and publish

                pass
            elif (self.state == self.State.COMPLETED):
                pass
            else:
                raise ValueError("Undefined state of the appln object")
        except Exception as e:
            raise e
    ########################################
    # dump the contents of the object 
    ########################################
    def dump (self):
        ''' Pretty print '''

        try:
            self.logger.info ("**********************************")
            self.logger.info ("PublisherAppln::dump")
            self.logger.info ("------------------------------")
            self.logger.info ("     Name: {}".format (self.name))
            self.logger.info ("     Lookup: {}".format (self.lookup))
            self.logger.info ("     Dissemination: {}".format (self.dissemination))
            self.logger.info ("**********************************")

        except Exception as e:
            raise e

    def register_response(self, reg_resp):
        ''' Handle register response '''

        try:
            self.logger.info("BrokerAppln::register_response")

            # Check the status of the response
            if (reg_resp.status == discovery_pb2.STATUS_SUCCESS):
                # We have registered, now let's see if the system is ready
                self.state = self.State.ISREADY

                # Not immediately waiting for call back
                # Return 0 and keep it moving
                return 0
            else:
                self.logger.debug ("BrokerAppln::register_response - registration is a failure with reason {}".format (reg_resp.reason))
                raise ValueError ("Broker needs to have unique id")

        except Exception as e:
            raise e

    def isready_response(self, isready_resp):
        ''' Handle isready response '''

        try:
            self.logger.info("BrokerAppln::isready_response")

            # Check the the status is true, meaning it is ready
            if not isready_resp.status:
                self.logger.debug("BrokerAppln::isready_response - Not ready yet; check again")
                time.sleep(10)  # sleep between calls so that we don't make excessive calls
            else:
                # Set to is acive
                # Time for broker to be the publisher and subscriber
                self.state = self.State.ACTIVE

            # Return a timeout of 0 so event loop can continue
            return 0

        except Exception as e:
            raise e

###################################
# Parse command line arguments
#
###################################
def parseCmdLineArgs ():
    parser = argparse.ArgumentParser (description="Broker Application")

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
  
    return parser.parse_args()

###################################
# Main program
#
###################################
def main():
    try:
        # obtain a system wide logger and initialize it to debug level to begin with
        logging.debug("Main - acquire a child logger and then log messages in the child")
        logger = logging.getLogger("BrokerAppln")

        # Parse the provided command line arguments
        logger.debug("Main: parse command line arguments")
        args = parseCmdLineArgs()

        # Reset the log level to to the specified level in the arguments
        logger.debug ("Main: resetting log level to {}".format (args.loglevel))
        logger.setLevel(args.loglevel)
        logger.debug ("Main: effective log level is {}".format (logger.getEffectiveLevel ()))

        # Obtain a subscriber application 
        logger.debug("Main: obtain the object")
        sub_app = BrokerAppln(logger)

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