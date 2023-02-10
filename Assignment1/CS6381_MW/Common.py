###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the publisher middleware code
#
# Created: Spring 2023
#
###############################################

# This file contains any declarations that are common to all middleware entities

# For now we do not have anything here but you can add enumerated constants for
# the role we are playing and any other common things that we need across
# all our middleware objects. Make sure then to import this file in those files once
# some content is added here that is needed by others. 

# Data model to hold information on a publisher
class Publisher:
    
    def __init__(self):
        self.name = None
        self.ip_address = None
        self.port = None
        self.topic_list = None

# Data model to hold information on a subscriber
class Subscriber:
     # This is not required to store for subscribers in our requirements
     # But it seems like a good idea to me so I am
    def __init__(self):
        self.name = None
        self.ip_address = None
        self.port = None
        self.topic_list = None

class Constants:

    DISSEMINATION_STRATEGY_DIRECT = "Direct"
    DISSEMINATION_STRATEGY_BROKER = "Broker"

    def __init__(self):
        pass