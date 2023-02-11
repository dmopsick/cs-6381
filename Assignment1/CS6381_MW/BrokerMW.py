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
# BrokerMW.py file as to how the middleware side of things are constructed
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
from CS6381_MW import topic_pb2

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

            self.logger.debug("BrokerMW::configure - connect to Discovery service")
            
            connect_str = "tcp://" + args.discovery
            self.req.connect (connect_str)

            self.logger.debug("BrokerMW::configure - bind to the pub socket")
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
            self.logger.debug("BrokerMW::register - populate the Registrant Info")
            reg_info = discovery_pb2.RegistrantInfo () # allocate
            reg_info.id = name  # our id
            reg_info.addr = self.addr  # our advertised IP addr where we are publishing
            reg_info.port = self.port # port on which we are publishing
            self.logger.debug("BrokerMW::register - done populating the Registrant Info")
      
            # Build a RegisterReq message
            self.logger.debug("BrokerMW::register - populate the nested register req")
            register_req = discovery_pb2.RegisterReq () 
            register_req.role = discovery_pb2.ROLE_BOTH  # The broker is both
            # It was observed that we cannot directly assign the nested field here.
            # A way around is to use the CopyFrom method as shown
            register_req.info.CopyFrom (reg_info)  # copy contents of inner structure
            register_req.topiclist[:] = topiclist   # this is how repeated entries are added (or use append() or extend ()
            self.logger.debug("BrokerMW::register - done populating nested RegisterReq")

            # Build the outer layer DiscoveryReq message
            self.logger.debug("BrokerMW::register - build the outer DiscoveryReq message")
            disc_req = discovery_pb2.DiscoveryReq ()  # allocate
            disc_req.msg_type = discovery_pb2.TYPE_REGISTER  # set message type
            # It was observed that we cannot directly assign the nested field here.
            # A way around is to use the CopyFrom method as shown
            disc_req.register_req.CopyFrom (register_req)
            self.logger.debug("BrokerMW::register - done building the outer message")

            # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
            # a real string
            buf2send = disc_req.SerializeToString()
            self.logger.debug("Stringified serialized buf = {}".format (buf2send))

            # now send this to our discovery service
            self.logger.debug("BrokerMW::register - send stringified buffer to Discovery service")
            self.req.send (buf2send)  # we use the "send" method of ZMQ that sends the bytes

            # now go to our event loop to receive a response to this request
            self.logger.info ("BrokerMW::register - sent register message and now now wait for reply")

        except Exception as e:
            raise e

    ##########################################
    # Check if the system is ready
    #
    ##########################################
    def is_ready(self):
        ''' Check with the discovery service to see if the system is ready '''

        try:
            self.logger.info ("BrokerMW::is_ready")

            # first build a IsReady message
            self.logger.debug("BrokerMW::is_ready - populate the nested IsReady msg")
            isready_req = discovery_pb2.IsReadyReq()  # allocate 
            # actually, there is nothing inside that msg declaration.
            self.logger.debug("BrokerMW::is_ready - done populating nested IsReady msg")

            # Build the outer layer Discovery Message
            self.logger.debug("BrokerMW::is_ready - build the outer DiscoveryReq message")
            disc_req = discovery_pb2.DiscoveryReq()
            disc_req.msg_type = discovery_pb2.TYPE_ISREADY
            # It was observed that we cannot directly assign the nested field here.
            # A way around is to use the CopyFrom method as shown
            disc_req.isready_req.CopyFrom(isready_req)
            self.logger.debug("BrokerMW::is_ready - done building the outer message")
            
            # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
            # a real string
            buf2send = disc_req.SerializeToString()
            self.logger.debug("Stringified serialized buf = {}".format (buf2send))

            # now send this to our discovery service
            self.logger.debug("BrokerMW::is_ready - send stringified buffer to Discovery service")
            self.req.send (buf2send)  # we use the "send" method of ZMQ that sends the bytes
            
            # now go to our event loop to receive a response to this request
            self.logger.info("BrokerMW::is_ready - request sent and now wait for reply")
      
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
            elif(disc_resp.msg_type == discovery_pb2.TYPE_ISREADY):
                timeout = self.upcall_obj.isready_response (disc_resp.isready_resp)
            elif(disc_resp.msg_type == discovery_pb2.TYPE_LOOKUP_ALL_PUBS):
                # Invoke the application logic to handle the reponse from discovery for looking up pub list by topic list
                timeout = self.upcall_obj.lookup_all_publisher_list_response(disc_resp.lookup_all_resp)
            else:
                raise ValueError("Unrecognized response message")

            return timeout

        except Exception as e:
            raise e

    #####################################
    # Perform the role of the publisher
    #
    # Disseminate data on the pub socket
    ####################################
    def disseminate (self, id, topic, data, timestamp):
        try:
            self.logger.debug ("BrokerMW::disseminate")

            # Need to include the unique identifier of the subscriber sending this data
            # In addition to the current time at which the data was sent
            # That way we can compare when the data is sent vs received

            self.logger.debug ("BrokerMW::disseminate - Build the Publication message to sent")

            # Build the Publication message 
            publication = topic_pb2.Publication()
            publication.topic = topic
            publication.content = data
            publication.pub_id = id
            publication.tstamp = timestamp # Use the time set at publisher level

            self.logger.debug ("BrokerMW::disseminate - Built the Publication message to sent")

            # self.logger.debug ("BrokerMW::disseminate - publication to send: " + str(publication))

            # Serialize the publication
            buf2send = publication.SerializeToString()
            
            # Now use the protobuf logic to encode the info and send it.  But for now
            # we are simply sending the string to make sure dissemination is working.
            self.logger.debug ("BrokerMW::disseminate - {}".format(buf2send))

            self.logger.debug("BrokerMW::disseminate - Publish the stringified buffer")
            # send the info as bytes. See how we are providing an encoding of utf-8
            # self.pub.send(bytes(send_str, "utf-8"))
            self.pub.send_multipart([bytes(topic, "utf-8"), buf2send])


            self.logger.debug ("BrokerMW::disseminate complete")
        except Exception as e:
            raise e

    #################################################
    # Look up a list of publishers by the topic list
    #
    ################################################
    def lookup_all_publishers(self):
        ''' Look up a list of publishers by topic list '''
        try:
            self.logger.debug("BrokerMW::lookup_all_publishers")

            # Build the inner LookupAllPubReq  message
            self.logger.debug("BrokerMW::lookup_all_publishers - populate the nested LookupAllPubReq msg")
            lookup_req = discovery_pb2.LookupAllPubReq()  
            self.logger.debug("BrokerMW::lookup_all_publishers - done populating nested LookupAllPubReq msg")

            # Build the outer layer Discovery message
            self.logger.debug("BrokerMW::lookup_all_publishers - build the outer DiscoveryReq message")
            disc_req = discovery_pb2.DiscoveryReq()
            disc_req.msg_type = discovery_pb2.TYPE_LOOKUP_ALL_PUBS

            disc_req.lookup_all_req.CopyFrom(lookup_req)
            self.logger.debug("BrokerMW::lookup_all_publishers - done building the outer message")
      
            # Stringify the buffer and print it
            # Actually a sequence not a string
            buf2send = disc_req.SerializeToString()
            self.logger.debug("Stringified serialized buf = {}".format(buf2send))

            # Send this to our discovery service
            self.logger.debug("BrokerMW::lookup_all_publishers - send stringified buffer to Discovery service")
            self.req.send(buf2send)  # we use the "send" method of ZMQ that sends the bytes
      
            # Now go to our event loop to receive a response to this request
            self.logger.debug("BrokerMW::lookup_all_publishers - now wait for reply")

        except Exception as e:
            raise e

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


    ##################################################
    # Connect to a publisher
    #
    # Need to tell only subscribe to the topics this subscriber is interested in
    ##################################################
    def connect_to_publisher(self, ip_address, port, topiclist):
        ''' Connect to a publisher for the list of topics we are interested in '''

        try :
            # Build connection string
            connect_str = "tcp://" + ip_address + ":" + str(port)

            self.logger.debug("BrokerMW::connect_to_publisher - connecting to {}".format(connect_str))
            self.sub.connect(connect_str)

            # Specify which topics we are subscribing to on this socket
            for topic in topiclist:
                self.sub.subscribe(topic)
                self.logger.debug("BrokerMW::connect_to_publisher - Connecting to {} for topic {}".format(connect_str, topic))

        except Exception as e:
            raise e

    ####################################################
    # Consume data from the publishers we have subscribed to
    #
    # Print out the messages we receive
    ####################################################
    def consume(self):
        ''' Consume messages sent from the publishers we subscribe to '''
        try:
            self.logger.debug("SubscriberMW::consume - Consume from our configured sub socket")
            
            # bytesReceived = self.sub.recv_string()
            bytesReceived = self.sub.recv_multipart()

            # The first element of the received array is the topic
            # The second is the publication object
            # Get the second element 
            publicationBytes = bytesReceived[1]

            # Decode the data 
            publication = topic_pb2.Publication()
            publication.ParseFromString(publicationBytes)
    
            # self.logger.debug("SubscriberMW::consume - Received " + publication.content)

            self.logger.debug("SubscriberMW::consume - Consumption complete")
            
            return publication

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