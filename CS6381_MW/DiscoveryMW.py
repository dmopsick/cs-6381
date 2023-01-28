###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the discovery middleware code
#
# Created: Spring 2023
#
###############################################

# Designing the logic is left as an exercise for the student.
#
# The discovery service is a server. So at the middleware level, we will maintain
# a REP socket binding it to the port on which we expect to receive requests.
#
# There will be a forever event loop waiting for requests. Each request will be parsed
# and the application logic asked to handle the request. To that end, an upcall will need
# to be made to the application logic.

# import the needed packages
import os     # for OS functions
import sys    # for syspath and system exception
import time   # for sleep
import logging # for logging. Use it in place of print statements.
import zmq  # ZMQ sockets

# import serialization logic
from CS6381_MW import discovery_pb2

class DiscoveryMW():
    
    ########################################
    # constructor
    ########################################
    def __init__(self, logger):
        self.logger = logger # internal logger for print statements
        self.res = None # ZMQ RES socket used to receive requests from pubs and subs
        self.poller = None # used to wait on incoming replies
        self.addr = None # Advertised IP address
        self.port = None # The port num where we are going to publish our topic
        self.upcall_obj = None # handle to appln obj to handle appln-specific data
        self.handle_events = True # in general we keep going thru the event loop

    ########################################
    # configure/initialize
    ########################################
    def configure(self, args):

        try:
            # Here we initialize any internal variables
            self.logger.info ("DiscoveryMW::configure")

            # First retrieve our advertised IP addr and the publication port num
            self.port = args.port
            self.addr = args.addr

            # Next get the ZMQ context
            self.logger.debug ("DiscoveryMW::configure - obtain ZMQ context")
            context = zmq.Context()  # returns a singleton object

             # get the ZMQ poller object
            self.logger.debug ("PublisherMW::configure - obtain the poller")
            self.poller = zmq.Poller ()

            # Open the RES socket to allow for pubs and subs to register
            self.res = context.socket(zmq.RES) 

            # note that we publish on any interface hence the * followed by port number.
            # We always use TCP as the transport mechanism (at least for these assignments)
            # Since port is an integer, we convert it to string to make it part of the URL
            bind_string = "tcp://*:" + str(self.port)
            self.res.bind(bind_string)

            self.logger.info ("DiscoveryMW::configure completed")
        except Exception as e:
            raise e

    #################################################################
    # run the event loop where we expect to receive a reply to a sent request
    #################################################################
    def event_loop(self, timeout=None):
        
        try:
            self.logger.info ("DiscoveryMW::event_loop - run the event loop")

            while self.handle_events:
                # poll for events. We give it an infinite timeout.
                # The return value is a socket to event mask mapping
                events = dict (self.poller.poll (timeout=timeout))

                if not events:
                    timeout = self.upcall_obj.invoke_operation()
                elif self.req in events:
                    timeout = self.handle_reply()
                else:
                    raise Exception("Unknown event after poll")

            self.logger.info ("DiscoveryMW::event_loop - out of the event loop")
        except Exception as e:
            raise e
