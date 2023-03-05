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

from DhtUtil import DhtUtil

ADDRESS_SPACE = 8

class DiscoveryMW():
    
    ########################################
    # constructor
    ########################################
    def __init__(self, logger):
        self.logger = logger # internal logger for print statements
        self.rep = None # ZMQ RES socket used to receive requests from pubs and subs AND other nodes now
        self.poller = None # used to wait on incoming replies
        self.addr = None # Advertised IP address
        self.port = None # The port num where we are going to publish our topic
        self.upcall_obj = None # handle to appln obj to handle appln-specific data
        self.handle_events = True # in general we keep going thru the event loop
        self.req_list = [] # The list of req sockets 
        self.dht_file_name = None
        self.dht = None
        self.finger_table = None
        self.dht_util = None

    ########################################
    # configure/initialize
    ########################################
    def configure(self, args):
        ''' Initialize the object '''

        try:
            # Here we initialize any internal variables
            self.logger.info ("DiscoveryMW::configure")

            # First retrieve our advertised IP addr and the publication port num
            self.port = args.port
            self.addr = args.addr
            self.dht_file_name = args.dht_name

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

            self.logger.debug("DiscoveryMW::configure - Load finger table from Appln to build list of req sockets")

            self.logger.debug("DiscoveryAppln::configure - Loading DHT Util, building DHT, then creating Finger Table")

            # Create a DHT Util class for use in the discovery logic
            self.dht_util = DhtUtil()
            # Build a DHT for this node to use
            self.dht = self.dht_util.build_dht(self.dht_file_name)
            # Create a finger table for this node, from the DHT we built
            self.finger_table = self.dht_util.create_finger_table(args.name, self.dht, ADDRESS_SPACE)

            self.logger.debug("DiscoveryAppln::configure - created Finger table: ")
            self.logger.debug(self.finger_table)

            self.logger.debug("DiscoveryMW::configure - Create a REQ socket for each distinct node")

            # Create a REQ socket for each of the distinct nodes in the finger table
            for node in self.finger_table:
                # Create one socket per node
                node_req = context.socket(zmq.REQ)

                # Build the connection string
                connect_str = "tcp://" + node["IP"] + ":" + str(node["port"])

                # Register the socket for POLLIN events
                self.poller.register(node_req, zmq.POLLIN)

                # Add settings from our class Slack to try to prevent deadlock
                node_req.setsockopt(zmq.RCVTIMEO, 2000)
                node_req.setsockopt(zmq.LINGER, 0)
                node_req.setsockopt(zmq.REQ_RELAXED,1)

                # Connect the socket
                node_req.connect(connect_str)

                # Add to the list of req sockets
                self.req_list.append(node_req)

            self.logger.debug("DiscoveryMW::configure - Done configuring a REQ socket for each distinct node")
            self.logger.debug(self.req_list)

            # note that we publish on any interface hence the * followed by port number.
            # We always use TCP as the transport mechanism (at least for these assignments)
            # Since port is an integer, we convert it to string to make it part of the URL
            bind_string = "tcp://*:" + str(self.port)
            self.logger.debug("DiscoveryMW::configure - attempting to bind to " + bind_string)
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

                # Where do I put the check for messages from other nodes?

                # if not events:
                #     timeout = self.upcall_obj.invoke_operation()
                if self.rep in events:
                    timeout = self.handle_request(self.rep)
               
                # Iterate through each of the req_list and check for events
                for req in self.req_list:
                    # Check if there is an incoming request on the specified port
                    if req in events:
                        self.logger.info("DiscoveryMW::event_loop - Received a message on a req socket")

                        # Handle the incoming request from another DHT node
                        timeout = self.handle_request(req)

            self.logger.info ("DiscoveryMW::event_loop - out of the event loop")
        except Exception as e:
            raise e

    #################################################
    # Top level logic for processing requests to the discovery server
    #################################################
    def handle_request(self, socket):
        ''' Handle a received request '''

        try:
            self.logger.info("DiscoveryMW::Handle received request")

            # Receive the data from the specified socket
            bytesRcvd = socket.recv()

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
            elif (disc_req.msg_type == discovery_pb2.TYPE_LOOKUP_ALL_PUBS):
                timeout = self.upcall_obj.lookup_all_publishers(disc_req.lookup_all_req)
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

            self.logger.info("Sending response of status: {}, reason: {}")

            self.logger.debug("DiscoveryMW::send_register_response - build the outer DiscoveryResp message")
            # Build the outer discovery response object
            discovery_response = discovery_pb2.DiscoveryResp()

            # Set the msg type
            discovery_response.msg_type = discovery_pb2.TYPE_REGISTER
            # Copy over the built nested register_response
            discovery_response.register_resp.CopyFrom(register_response)
            self.logger.debug("DiscoveryMW::send_register_response - Done building the outer DiscoveryResp message")

            # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
            # a real string
            buf2send = discovery_response.SerializeToString ()
            self.logger.debug("Stringified serialized buf = {}".format (buf2send))

            self.logger.debug("DiscoveryMW::send_register_response -- Here is the rep socket at this time")
            self.logger.debug(self.dump_object_properties(self.rep))

            # Send a response back to the registrant that attempted to register
            self.logger.debug ("DiscoveryMW::send_register_response - send stringified buffer response to the entity registering")
            self.rep.send(buf2send)  # we use the "send" method of ZMQ that sends the bytes

            self.logger.info("DiscoveryMW::send_register_response Register finished")
        
            # Return a timeout of 0
            return 0

        except Exception as e:
            raise e

    def dump_object_properties(self, obj):
        props = [prop for prop in dir(obj) if not prop.startswith('__')]
        prop_str = "\n".join([f"{prop}: {getattr(obj, prop)}" for prop in props])
        return prop_str
    
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

            # Return a timeout of 0
            return 0

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
        
            # Build the inner LookupPubByTopicReq object
            lookup_resp = discovery_pb2.LookupPubByTopicResp()
            lookup_resp.status = status

            # Only build out the list of publishers if there any to send
            if (len(publisher_list) > 0):
            
                self.logger.debug("DiscoveryMW::send_lookup_pub_by_topiclist_response Converting each publisher into a Registrant Info record")

                # Build a list of Registrant info
                for publisher in publisher_list:
                    # Add a new Registrant info to the list of publishers
                    registrant_info = lookup_resp.publisher_list.add()
                    # Update the new registrant's data
                    registrant_info.id = publisher.name
                    registrant_info.addr = publisher.ip_address
                    registrant_info.port = publisher.port
                    # self.logger.debug("DiscoveryMW::send_lookup_pub_by_topiclist_response - FLAG 1: Adding " + registrant_info.id + " " + registrant_info.addr  + " " +  str(registrant_info.port))

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
    
    def send_lookup_all_publisher_response(self, status, all_publisher_list):
        ''' Send a response to a request for all publishers '''

        try:
            self.logger.debug("DiscoveryMW::send_lookup_all_publisher_response")

            # Build the inner LookupPubByTopicReq  object
            lookup_resp = discovery_pb2.LookupAllPubResp()
            lookup_resp.status = status

            # Only build out the list of publishers if there any to send
            if (len(all_publisher_list) > 0):
            
                self.logger.debug("DiscoveryMW::send_lookup_all_publisher_response Converting each publisher into a Registrant Info record")

                # Build a list of Registrant info
                for publisher in all_publisher_list:
                    # Add a new Registrant info to the list of publishers
                    registrant_info = lookup_resp.publisher_list.add()
                    # Update the new registrant's data
                    registrant_info.id = publisher.name
                    registrant_info.addr = publisher.ip_address
                    registrant_info.port = publisher.port

            self.logger.debug("DiscoveryMW::send_lookup_all_publisher_response done building nested look_resp object")

            self.logger.debug ("DiscoveryMW::send_lookup_all_publisher_response - build the outer DiscoveryResp message")
            # Build the outer discovery response object
            discovery_response = discovery_pb2.DiscoveryResp()
            discovery_response.msg_type = discovery_pb2.TYPE_LOOKUP_ALL_PUBS
            discovery_response.lookup_all_resp.CopyFrom(lookup_resp)
            self.logger.debug("DiscoveryMW::send_lookup_all_publisher_response - Done building the outer DiscoveryResp message")

            # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
            # a real string
            buf2send = discovery_response.SerializeToString ()
            self.logger.debug("Stringified serialized buf = {}".format (buf2send))

            # Send a response back to the registrant that attempted to look up publishers
            self.logger.debug ("DiscoveryMW::send_lookup_all_publisher_response - send stringified buffer to Discovery service")
            self.rep.send(buf2send)  # we use the "send" method of ZMQ that sends the bytes

            self.logger.info("DiscoveryMW::send_lookup_all_publisher_response sending lookup response finished")
        
            # Return timeout of zero
            return 0

        except Exception as e:
            raise e

    ####################################################
    # Forward a register request to another node for completion
    #
    # This DHT node received that it is not able to complete
    # Because it is not the successor of the hashed topic of the node
    # Pass it on to another node to register/pass on again
    ####################################################
    def forward_reg_req_to_node(self, reg_req, node_to_forward_to):

        try:
            self.logger.debug("DiscoveryMW::forward_reg_req_to_node - Forwarding a register request to {}".format(node_to_forward_to["id"]))

            # Declare a variable for the index of the node to forward to
            node_index = -1

            self.logger.debug("DiscoveryMW::forward_reg_req_to_node - Find the index of the REQ socket to foward to")
            # Find the specified req to send to based on the node we want to forward to
            # Find the index of the node we are forwarding to in the finger table
            for index, node in enumerate(self.finger_table):
                # Check if the entry in the finger table pertains to the node we will forward to
                if node["id"] == node_to_forward_to["id"]:
                    node_index = index
                    break
    
            # The req socket we want to send to has the same index in the req table
            # As the chosen node does in the finger table
            specified_req = self.req_list[node_index]

            # Build the outer layer DiscoveryReq message 
            disc_req = discovery_pb2.DiscoveryReq()
            disc_req.msg_type = discovery_pb2.TYPE_REGISTER  # set message type
            # It was observed that we cannot directly assign the nested field here.
            # A way around is to use the CopyFrom method as shown
            disc_req.register_req.CopyFrom (reg_req)
            self.logger.debug("DiscoveryMW::forward_reg_req_to_node - done building the outer message")
            
            # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
            # a real string
            buf2send = disc_req.SerializeToString()
            self.logger.debug("Stringified serialized buf = {}".format(buf2send))

            # now send this to our discovery service
            self.logger.debug("DiscoveryMW::forward_reg_req_to_node - send stringified buffer to Discovery service")
            specified_req.send(buf2send)

            # now go to our event loop to receive a response to this request
            self.logger.info("DiscoveryMW::forward_reg_req_to_node - The register request has been succesfully forwarded")
    
            # Return the timeout of 0
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
