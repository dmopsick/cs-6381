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

# Combine the data models into one entity with a role variale
class Entity:

    def __init__(self):
        self.role = None
        self.name = None
        self.ip_address = None
        self.port = None
        self.topic_list = None

# Use constants instead of magic strings
class Constants:

    DISSEMINATION_STRATEGY_DIRECT = "Direct"
    DISSEMINATION_STRATEGY_BROKER = "Broker"

    def __init__(self):
        pass