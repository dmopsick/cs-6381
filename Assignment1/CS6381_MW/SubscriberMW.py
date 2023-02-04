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
        self.upcall_obj = None # handle to appln obj to handle appln-specific data
        self.handle_events = True # in general we keep going thru the event loop
        self.lookup = None # one of the diff ways we do lookup
        self.dissemination = None # direct or via broker

    def configure(self, args):
        ''' Initialize the subscriber middleware object '''

        try:
            # Initialize any internal variables
            self.logger.info("SubscriberMW::configure")

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
            # REQ needed because we are client of the Discovery service
            # SUB needed because we subscribe to publisher's topic data
            self.logger.debug("SubscriberMW::configure - obtain REQ and SUB sockets")
            self.req = context.socket(zmq.REQ)
            self.pub = context.socket(zmq.SUB)

            # Register the req socket for incoming request
            self.logger.debug ("SubscriberMW::configure - register the REQ socket for incoming replies")
            self.poller.register(self.req, zmq.POLLIN)

            # Connect to the discovery service 
            # Use TCP followed by Ip addr:port number
            self.logger.debug("SubscriberMW::configure - connect to Discovery service")
            connect_str = "tcp://" + args.discovery
            self.req.connect(connect_str)

            # "Connect" to the SUB socket
            sub_connect_string = "tcp://*:" + str(self.port)
            self.sub.connect(sub_connect_string)

            self.logger.info("SubscriberMW::configure completed")

        except Exception as e:
            raise e

    ########################################
    # register with the discovery service
    ########################################
    def register(self, name, topicList):
        ''' Register the AppLn with the discovery service '''
        try:
            self.logger.debug("SubscriberMW::register")

            # Build the Registrant Info message first.
            self.logger.debug("SubscriberMW::register - populate the nested register req")
            reg_info = discovery_pb2.RegistrantInfo () # allocate
            reg_info.id = name  # our id
            reg_info.addr = self.addr  # our advertised IP addr where we are publishing
            reg_info.port = self.port # port on which we are publishing
            self.logger.debug ("SubscriberMW::register - done populating nested RegisterReq")

            # Build a RegisterReq message 
            self.logger.debug ("SubscriberMW::register - populate the nested register req")
            register_req = discovery_pb2.RegisterReq ()  # allocate 
            register_req.role = discovery_pb2.ROLE_SUBSCRIBER  # we are a publishe
            register_req.info.CopyFrom(reg_info)  # copy contents of inner structure
            register_req.topiclist[:] = topicList   # this is how repeated entries are added (or use append() or extend ()
      
            self.logger.debug ("SubscriberMW::register - done populating nested RegisterReq")

            # Build the outer layer Discovery message
            self.logger.debug("SubscriberMW::register - build the outer DiscoveryReq message")
            disc_req = discovery_pb2.DiscoveryReq ()
            disc_req.msg_type = discovery_pb2.REGISTER
            disc_req.register_req.CopyFrom (reg_info)
            self.logger.debug ("SubscriberMW::register - done building the outer message")
            
            # Stringify the buffer and print it 
            buf2send = disc_req.SerializeToString ()
            self.logger.debug("Stringified serialized buf = {}".format (buf2send))

            # Send this to our discovery service
            self.logger.debug("SubscriberMW::register - send stringified buffer to Discovery service")
            self.req.send(buf2send)  # we use the "send" method of ZMQ that sends the bytes

            # Now go to our event loop to receive a response to this request
            self.logger.debug("SubscriberMW::register - now wait for reply")

        except Exception as e:
            raise e

    def is_ready (self):
        ''' Register the AppLn with the discovery service '''

        try:
            self.logger.debug("SubscriberMW::is_ready")

            # Build an isReady message
            self.logger.debug("SubscriberMW::is_ready - populate the nested IsReady msg")
            isready_msg = discovery_pb2.IsReadyReq()  # allocate 
            self.logger.debug("SubscriberMW::is_ready - done populating nested IsReady msg")

            # Build the outer layer Discovery message
            self.logger.debug ("SubscriberMW::is_ready - build the outer DiscoveryReq message")
            disc_req = discovery_pb2.DiscoveryReq()
            disc_req.msg_type = discovery_pb2.TYPE_ISREADY

            disc_req.isready_req.CopyFrom (isready_req)
            self.logger.debug ("SubscriberMW::is_ready - done building the outer message")
      

            # Stringify the buffer and print it
            # Actually a sequence not a string
            buf2send = disc_req.SerializeToString()
            self.logger.debug ("Stringified serialized buf = {}".format(buf2send))

            # Send this to our discovery service
            self.logger.debug("SubscriberMW::is_ready - send stringified buffer to Discovery service")
            self.req.send(buf2send)  # we use the "send" method of ZMQ that sends the bytes
      
            # Now go to our event loop to receive a response to this request
            self.logger.debug("SubscriberMW::is_ready - now wait for reply")

        except Exception as e:
            raise e

    #################################################################
    # Run the event loop where we expect to receive a reply to a sent request
    #################################################################
    def event_loop (self):

        try:
            self.logger.debug("SubscriberMW::event_loop - Run the event loop")

            while self.handle_events:
                # Poll for events with an infinite timeout
                # The return value is a socket to event mask mapping
                events = dict(self.poller.poll(timeout=timeout))

                # Check if the timeout occurred
                if not events:
                    timeout = self.upcall_obj.invoke_operation()

                # Only should be receiving messages in the req socket
                elif self.req in events: 
                    timeout = self.handle_reply()

                else:
                    raise Exception("Unknown event after poll")
        
        except Exception as e:
            raise e

    #################################################################
    # Handle an incoming reply
    #################################################################
    def handle_reply (self):

        try:
            self.logger.debug("SubscriberMW::handle_reply")

            # Receive all the bytes
            bytesRcvd = self.req.recv()

            # Use the protobuf to deserialize the bytes
            disc_resp = discovery_pb2.DiscoveryResp()
            disc_resp.ParseFromString(bytesRcvd)

            if (disc_resp.msg_type == discovery_pb2.REGISTER):
                timeout = disc_resp.register_resp.result
            elif (disc_resp.msg_type == discovery_pb2.ISREADY):
                timeout = disc_resp.is_ready.reply
            else: 
                raise ValueError("Unrecognized response message provided")
            
            return timeout 


        except Exception as e:
            raise e

    #################################################################
    # receive data on our sub socket
    #################################################################
    def receive(self, data):
        pass

    #############################################################
    # Subscribe to an list of topics
    #############################################################
    def subscribe(self, topic_list):
        ''' Subscribe to a list of topics '''

        for topic in topic_list:
            self.logger.debug("SubscriberMW::subscribe - Subscribing to topic {}".format(topic))
            # Pass in the binary representation of the topic name to the subscribe socket
            # Use UTF-8 encoding
            self.sub.setsockopt(zmq.SUBSCRIBE, bytes(topic, "utf-8"))

    ########################################
    # set upcall handle
    #
    # here we save a pointer (handle) to the application object
    ########################################
    def set_upcall_handle (self, upcall_obj):
        ''' set upcall handle '''
        self.upcall_obj = upcall_obj

    ########################################
    # disable event loop
    #
    # here we just make the variable go false so that when the event loop
    # is running, the while condition will fail and the event loop will terminate.
    ########################################
    def disable_event_loop (self):
        ''' disable event loop '''
        self.handle_events = False

    ##################################################
    # Connect to a publisher
    ##################################################
    def connectToPublisher(self, ip_address, port):
        # Build connection string
        connect_str = "tcp://" + ip_address + ":" + str(port)

        self.logger.debug("SubscriberMW::connectToPublisher - connecting to {}".format(connect_str))
        self.sub.connect(connect_str)
