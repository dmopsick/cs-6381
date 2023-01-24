###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the subscriber middleware code
#
# Created: Spring 2023
#
###############################################

# Designing the logic is left as an exercise for the student. Please see the
# PublisherMW.py file as to how the middleware side of things are constructed
# and accordingly design things for the subscriber side of things.
#
# Remember that the subscriber middleware does not do anything on its own.
# It must be invoked by the application level logic. This middleware object maintains
# the ZMQ sockets and knows how to talk to Discovery service, etc.
#
# Here is what this middleware should do
# (1) it must maintain the ZMQ sockets, one in the REQ role to talk to the Discovery service
# and one in the SUB role to receive topic data
# (2) It must, on behalf of the application logic, register the subscriber application with the
# discovery service. To that end, it must use the protobuf-generated serialization code to
# send the appropriate message with the contents to the discovery service.
# (3) On behalf of the subscriber appln, it must use the ZMQ setsockopt method to subscribe to all the
# user-supplied topics of interest. 
# (4) Since it is a receiver, the middleware object will maintain a poller and even loop waiting for some
# subscription to show up (or response from Discovery service).
# (5) On receipt of a subscription, determine which topic it is and let the application level
# handle the incoming data. To that end, you may need to make an upcall to the application-level
# object.
#

# import the needed packages
import os     # for OS functions
import sys    # for syspath and system exception
import time   # for sleep
import argparse # for argument parsing
import configparser # for configuration parsing
import logging # for logging. Use it in place of print statements.
import zmq  # ZMQ sockets

# import serialization logic
from CS6381_MW import discovery_pb2

class SubscriberMW ():

    def __init__(self, logger):
        self.logger = logger  # internal logger for print statements
        self.sub = None # will be a ZMQ SUB socket for receiving information/topics
        self.req = None # will be a ZMQ REQ socket to talk to Discovery service
        self.poller = None # used to wait on incoming replies
        self.addr = None # our advertised IP address
        self.port = None # port num where we are going to publish our topics

    def configure(self, args):
        ''' Initialize the subscriber middleware object '''

        try:
            # Initialize any internal variables
            self.logger.debug("SubscriberMW::configure")

            # Retrieve advertised IP address and subscriber port num
            self.port = args.port
            self.addr = args.addr

            # Get the ZMQ context
            self.logger.debug("SubscriberMW::configure - obtain ZMQ context")
            context = zmq.Context()

            # Get the ZMQ Poller object
            self.logger.debug("SubscriberMW::configure - obtain poller")
            self.poller = zmq.poller()
            
            # Acquire the REQ and SUB sockets
            self.logger.debug ("SubscriberMW::configure - obtain REQ and SUB sockets")
            self.req = context.socket (zmq.REQ)
            self.pub = context.socket (zmq.SUB)

            # Register the req socket for incoming request
            self.logger.debug ("SubscriberMW::configure - register the REQ socket for incoming replies")
            self.poller.register(self.req, zmq.POLLIN)

            # Connect to the discovery service 
            # Use TCP followed by Ip addr:port number
            self.logger.debug ("SubscriberMW::configure - connect to Discovery service")
            connect_str = "tcp://" + args.discovery
            self.req.connect (connect_str)

            # "Connect" to the SUB socket
            sub_connect_string = "tcp://*:" + self.port
            self.sub.connect(sub_connect_string)

        except Exception as e:
            raise e

    ########################################
    # register with the discovery service
    ########################################
    def register (self, name):
        ''' Register the AppLn with the discovery service '''
        try:
            pass
        except Exception as e:
            raise e

        pass

    def is_ready (self):

        pass

    #################################################################
    # run the event loop where we expect to receive a reply to a sent request
    #################################################################
    def event_loop (self):
      pass

    #################################################################
    # handle an incoming reply
    #################################################################
    def handle_reply (self):
        pass

    #################################################################
    # receive data on our sub socket
    #################################################################
    def receive(self, data):
        pass

    pass
