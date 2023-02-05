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
        self.rep = None # ZMQ RES socket used to receive requests from pubs and subs
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
            self.logger.debug("DiscoveryMW::configure - obtain ZMQ context")
            context = zmq.Context()  # returns a singleton object

             # get the ZMQ poller object
            self.logger.debug("DiscoveryMW::configure - obtain the poller")
            self.poller = zmq.Poller()

            # Open the RES socket to allow for pubs and subs to register
            self.rep = context.socket(zmq.REP) 

            # Register the RES socket to poll for incoming messages 
            self.logger.debug("DiscoveryMW::configure - register the REP socket for incoming replies")
            self.poller.register(self.rep, zmq.POLLIN)

            self.logger.debug("DiscoveryMW::configure - REP socket registered")

            # note that we publish on any interface hence the * followed by port number.
            # We always use TCP as the transport mechanism (at least for these assignments)
            # Since port is an integer, we convert it to string to make it part of the URL
            bind_string = "tcp://*:" + str(self.port)
            self.logger.debug("DiscoveryMW::configure - attempting to bind to ".format(bind_string))
            self.rep.bind(bind_string)

            self.logger.info ("DiscoveryMW::configure completed")
        except Exception as e:
            raise e

    #################################################################
    # run the event loop where we expect to receive a reply to a sent request
    #################################################################
    def event_loop(self, timeout=None):
        
        try:
            self.logger.info("DiscoveryMW::event_loop - run the event loop")

            while self.handle_events:
                # poll for events. We give it an infinite timeout.
                # The return value is a socket to event mask mapping
                events = dict(self.poller.poll(timeout=timeout))

                # if not events:
                #     timeout = self.upcall_obj.invoke_operation()
                if self.rep in events:
                    timeout = self.handle_request()
                else:
                    pass
                    # raise Exception("Unknown event after poll")

            self.logger.info ("DiscoveryMW::event_loop - out of the event loop")
        except Exception as e:
            raise e

    #################################################
    # Top level logic for processing requests to the discovery server
    #################################################
    def handle_request(self):

        try:
            self.logger.info("DiscoveryMW::Handle")

            # Receive the data 
            bytesRcvd = self.rep.recv()

            # Deserialize the incoming bytes as a DiscoveryReq
            # That is what the pubs and subs are building 
            disc_req = discovery_pb2.DiscoveryReq()
            disc_req.ParseFromString(bytesRcvd)

            # Check the msg type in order to determine how to handle it
            if (disc_req.msg_type == discovery_pb2.TYPE_REGISTER):
                # Handle a register request
                timeout = self.upcall_obj.register_request(disc_req.register_req)
            elif (disc_req.msg_type == discovery_pb2.TYPE_ISREADY):
                # Handle a request made by a publisher asking if the system is ready
                timeout = self.upcall_obj.isready_request(disc_req.isready_req)
            elif (disc_req.msg_type == discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC):
                # Handle a request made by a subscriber to look up all publishers by topic
                timeout = self.upcall_obj.lookup_pub_by_topiclist_request(disc_req.lookup_req)
            else: # anything else is unrecognizable by this object
                # raise an exception here
                raise ValueError ("Unrecognized response message")

            return timeout
        except Exception as e:
            raise e 

    #####################################################
    # Send a response to an entity attempting to register with the discovery server
    #####################################################
    def send_register_response(self, status, reason):
        ''' Send a response back to a registrant that has attempted to register '''

        try:
            self.logger.info("DiscoveryMW::send_register_response")

            self.logger.debug("DiscoveryMW::send_register_response - populate the nested register resp")
            # Build the register response object
            register_response = discovery_pb2.RegisterResp()

            # Add in the status passed in
            register_response.status = status

            # If status is not null add in the reason
            if reason != None:
                register_response.reason = reason
            self.logger.debug("DiscoveryMW::register - done populating nested RegisterResp")

            self.logger.debug("DiscoveryMW::send_register_response - build the outer DiscoveryResp message")
            # Build the outer discovery response object
            discovery_response = discovery_pb2.DiscoveryResp()

            # Set the msg type
            discovery_response.msg_type = discovery_response.TYPE_REGISTER
            # Copy over the built nested register_response
            discovery_response.register_resp.CopyFrom(register_response)
            self.logger.debug("DiscoveryMW::send_register_response - Done building the outer DiscoveryResp message")

            # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
            # a real string
            buf2send = discovery_response.SerializeToString ()
            self.logger.debug("Stringified serialized buf = {}".format (buf2send))

            # Send a response back to the registrant that attempted to register
            self.logger.debug ("DiscoveryMW::send_register_response - send stringified buffer to Discovery service")
            self.rep.send(buf2send)  # we use the "send" method of ZMQ that sends the bytes

            self.logger.info("DiscoveryMW::send_register_response Register finished")
        
        except Exception as e:
            raise e
    
    ############################################
    # Send a response to an is ready response
    ############################################
    def send_isready_response(self, isready):
        ''' Send a response back to a registrant that has made an isready_request '''

        try:
            self.logger.info("DiscoveryMW::send_isready_response")

            self.logger.debug("DiscoveryMW::send_isready_response - populate the nested isready resp")
            # Build the register response object
            isready_response = discovery_pb2.IsReadyResp()
            # Load the isready response with the passed in status
            isready_response.status = isready
            
            self.logger.debug("DiscoveryMW::send_isready_response - done populating the nested isready resp")
            
            self.logger.debug ("DiscoveryMW::send_isready_response - build the outer DiscoveryResp message")
            # Build the outer discovery response object
            discovery_response = discovery_pb2.DiscoveryResp()
            discovery_response.msg_type = discovery_pb2.TYPE_ISREADY
            discovery_response.isready_resp.CopyFrom(isready_response)
            self.logger.debug("DiscoveryMW::send_isready_response - Done building the outer DiscoveryResp message")

            # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
            # a real string
            buf2send = discovery_response.SerializeToString ()
            self.logger.debug("Stringified serialized buf = {}".format (buf2send))

            # Send a response back to the registrant that sent isready request
            self.logger.debug ("DiscoveryMW::send_isready_response - send stringified buffer to Discovery service")
            self.rep.send(buf2send)  # we use the "send" method of ZMQ that sends the bytes

            self.logger.info("DiscoveryMW::send_isready_response sending isready response finished")

        except Exception as e:
            raise e

    ############################################
    # Send a response to a lookup pub by topiclist request
    ############################################
    def send_lookup_pub_by_topiclist_response(self, status, publisher_list):
        ''' Send a response back fore a request made to load list of pubishers by topic list '''
        
        try:
            self.logger.info("DiscoveryMW::send_lookup_pub_by_topiclist_response")

            self.logger.debug("DiscoveryMW::send_lookup_pub_by_topiclist_response building nested look_resp object")
            # Build the inner LookupPubByTopicReq  object
            lookup_resp = discovery_pb2.DiscoveryResp()
            lookup_resp.status = status
            lookup_resp.publisher_list = publisher_list
            self.logger.debug("DiscoveryMW::send_lookup_pub_by_topiclist_response done building nested look_resp object")

            self.logger.debug ("DiscoveryMW::send_lookup_pub_by_topiclist_response - build the outer DiscoveryResp message")
            # Build the outer discovery response object
            discovery_response = discovery_pb2.DiscoveryResp()
            discovery_response.msg_type = discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC
            discovery_response.lookup_resp.CopyFrom(lookup_resp)
            self.logger.debug("DiscoveryMW::send_lookup_pub_by_topiclist_response - Done building the outer DiscoveryResp message")

            # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
            # a real string
            buf2send = discovery_response.SerializeToString ()
            self.logger.debug("Stringified serialized buf = {}".format (buf2send))

            # Send a response back to the registrant that attempted to look up publishers
            self.logger.debug ("DiscoveryMW::send_lookup_pub_by_topiclist_response - send stringified buffer to Discovery service")
            self.rep.send(buf2send)  # we use the "send" method of ZMQ that sends the bytes

            self.logger.info("DiscoveryMW::send_lookup_pub_by_topiclist_response sending lookup response finished")
        
            # Return timeout of zero
            return 0
        except Exception as e:
            raise e
    

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
