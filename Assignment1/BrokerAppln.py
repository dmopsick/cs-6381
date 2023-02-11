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
import logging # for logging. Use it in place of print statements.

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
        ACTIVE = 4,
        COMPLETED = 5

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

            # Does broker need topic list? I feel like no

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
                pass
            elif (self.state == self.State.ACTIVE):
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
            if (reg_resp.status == discovery_pb2.STATUS_SUCCESS)

        except Exception as e:
            raise e
