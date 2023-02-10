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
from CS6381_MW.PublisherMW import PublisherMW
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
        DISSEMINATE = 4,
        COMPLETED = 5

    ########################################
    # Constructor
    ########################################
    def __init__(self, logger):
        self.logger = logger
        self.mw_obj = None
        self.state = self.State.I

    ########################################
    # Configure/initialize
    ########################################
    def configure(self, args):
        ''' Initialize the object '''
        pass

    ########################################
    # Driver program
    ########################################
    def driver(self):
        ''' Driver program '''
        pass

    ########################################
    # generic invoke method called as part of upcall
    #
    # This method will get invoked as part of the upcall made
    # by the middleware's event loop after it sees a timeout has
    # occurred.
    ########################################
    def invoke_operation (self):
        pass
