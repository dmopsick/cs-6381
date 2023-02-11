###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the broker middleware code
#
# Created: Spring 2023
#
###############################################

# Designing the logic is left as an exercise for the student. Please see the
# PublisherMW.py file as to how the middleware side of things are constructed
# and accordingly design things for the broker side of things.
#
# As mentioned earlier, a broker serves as a proxy and hence has both
# publisher and subscriber roles. So in addition to the REQ socket to talk to the
# Discovery service, it will have both PUB and SUB sockets as it must work on
# behalf of the real publishers and subscribers. So this will have the logic of
# both publisher and subscriber middleware.

# import the needed packages
import os     # for OS functions
import sys    # for syspath and system exception
import time   # for sleep
import logging # for logging. Use it in place of print statements.
import zmq  # ZMQ sockets
import datetime

# import serialization logic
from CS6381_MW import discovery_pb2

class BrokerMW():

    def __init__(self, logger):
        self.logger = logger
        self.req = None # ZMQ REQ for connecting to Discovery service
        self.sub = None # ZMQ SUB socket for receiving from the publishers
        self.pub = None # ZMQ PUB socket for publishing what is received from the publishers to the subscribers
        self.poller = None # Wait on incoming replies
        self.addr = None # Broker's IP Address
        self.port = None # Broker's port
        self.upcall_obj = None
        self.handle_events = True

    ####################################
    # Configure Broker MW
    #
    ####################################
    def configure(self, args):
        ''' Initialize the object '''
        try :
            self.logger.info("BrokerMW::configure")

            self.addr = args.addr
            self.port = args.port

            self.logger.debug("BrokerMW::configure - obtain ZMQ context")
            context = zmq.Context()

            # Get the ZMQ poller 
            self.logger.debug("BrokerMW::configure - obtain the poller")
            self.poller = zmq.Poller()

            # Load all three sockets
            self.logger.debug("BrokerMW::configure - obtain REQ and PUB sockets")
            self.req = context.socket(zmq.REQ)
            self.sub = context.socket(zmq.SUB)
            self.pub = context.socket(zmq.PUB)
     
            self.logger.debug("BrokerMW::configure - register the REQ socket for incoming replies")
            self.poller.register(self.req, zmq.POLLIN)

            self.logger.debug ("BrokerMW::configure - connect to Discovery service")
            
            connect_str = "tcp://" + args.discovery
            self.req.connect (connect_str)

            self.logger.debug ("BrokerMW::configure - bind to the pub socket")

            bind_string = "tcp://*:" + str(self.port)
            self.pub.bind (bind_string)
            
            self.logger.info ("BrokerMW::configure completed")

        except Exception as e:
            raise e

    ############################################
    # Run the event loop where we expect to receive a reply to a sent request
    #
    ###########################################
    def event_loop(self, timeout=None):
        ''' Event loop to receive and respond to replies to sent requests '''

        try:
            self.logger.info ("BrokerMW::event_loop - run the event loop")

            while self.handle_events:
                # Poll for incoming events
                events = dict(self.poller.poll(timeout=timeout))

                if not events:
                    # Timeout has occurred
                    # Now it is time for application logic to take control again
                    timeout = self.upcall_obj.invoke_operation()
                elif self.req in events:
                    # Handle the incoming reply from remote entity and return the result
                    timeout = self.handle_reply()
                else:
                    raise Exception("Unknown event after poll")
            
        except Exception as e:
            raise e

    ########################################
    # Register the broker with the discovery service
    #
    # Does broker need a topiclist? 
    ########################################
    def register(self, name, topiclist):
        ''' Register the broker with the discovery service '''

        try :
            self.logger.info("BrokerMW::register")

            # Build the registrant info message 
            self.logger.debug ("BrokerMW::register - populate the Registrant Info")
            reg_info = discovery_pb2.RegistrantInfo () # allocate
            reg_info.id = name  # our id
            reg_info.addr = self.addr  # our advertised IP addr where we are publishing
            reg_info.port = self.port # port on which we are publishing
            self.logger.debug ("BrokerMW::register - done populating the Registrant Info")
      
            # Build a RegisterReq message
            self.logger.debug ("BrokerMW::register - populate the nested register req")
            register_req = discovery_pb2.RegisterReq () 
            register_req.role = discovery_pb2.ROLE_BOTH  # The broker is both
            # It was observed that we cannot directly assign the nested field here.
            # A way around is to use the CopyFrom method as shown
            register_req.info.CopyFrom (reg_info)  # copy contents of inner structure
            register_req.topiclist[:] = topiclist   # this is how repeated entries are added (or use append() or extend ()
            self.logger.debug ("BrokerMW::register - done populating nested RegisterReq")

            # Build the outer layer DiscoveryReq message
            self.logger.debug ("BrokerMW::register - build the outer DiscoveryReq message")
            disc_req = discovery_pb2.DiscoveryReq ()  # allocate
            disc_req.msg_type = discovery_pb2.TYPE_REGISTER  # set message type
            # It was observed that we cannot directly assign the nested field here.
            # A way around is to use the CopyFrom method as shown
            disc_req.register_req.CopyFrom (register_req)
            self.logger.debug ("BrokerMW::register - done building the outer message")

            # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
            # a real string
            buf2send = disc_req.SerializeToString ()
            self.logger.debug ("Stringified serialized buf = {}".format (buf2send))

            # now send this to our discovery service
            self.logger.debug ("BrokerMW::register - send stringified buffer to Discovery service")
            self.req.send (buf2send)  # we use the "send" method of ZMQ that sends the bytes

            # now go to our event loop to receive a response to this request
            self.logger.info ("BrokerMW::register - sent register message and now now wait for reply")

        except Exception as e:
            raise e

    ##################################
    # Handle an incoming reply
    #
    ##################################
    def handle_reply(self):
        ''' Handle an incoming reply '''
        try:
            self.logger.info("BrokerMW::handle_reply")

            # Receive all the bytes
            bytesReceived = self.req.recv()

            # Deserialize the message 
            disc_resp = discovery_pb2.DiscoveryResp()
            disc_resp.ParseFromString(bytesReceived)

            # Check the type of reply
            if (disc_resp.msg_type == discovery_pb2.TYPE_REGISTER):
                timeout = self.upcall_obj.register_response(disc_resp.register_resp)
            elif (disc_resp.msg_type == discovery_pb2.TYPE_ISREADY):
                timeout = self.upcall_obj.isready_response (disc_resp.isready_resp)
            else:
                raise ValueError("Unrecognized response message")

            return timeout

        except Exception as e:
            raise e
