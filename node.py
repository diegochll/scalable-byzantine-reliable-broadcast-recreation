from scipy.stats import poisson

from numpy import random
import threading

def get_random_sample(expected_sample_size,num_nodes,node_id):
    sample_size = random.poisson(expected_sample_size)
    while(sample_size<=0 or sample_size>=(num_nodes-1)): #can pick at most num_nodes-2 neighbors
        sample_size = random.poisson(expected_sample_size)
    sample = []
    for x in range(sample_size):
        randIndex = random.randint(num_nodes)
        while(randIndex == node_id or randIndex in sample):
            randIndex = random.randint(num_nodes)
        sample.append(randIndex)
    return sample


class Message:
    def __init__(self,originator,message_type,content,signature = ""):

        self.originator = originator
        self.type = message_type
        self.content = content
        self.signature = signature

    def __str__(self):
        return "Message(from: '{}'; type: '{}'; content: '{}'; signature: '{}')".format(self.originator,self.type, self.content,self.signature)

class Node:
    def __init__(self,node_id,expected_sample_size,num_nodes,message_queues):
        self.node_id = node_id
        self.G = set(get_random_sample(expected_sample_size,num_nodes, self.node_id))
        self.num_messages_sent = 0
        self.is_originator = False
        self.delivered = Message(-1, "DEFAULT", "")
        self.event = threading.Event()
        for g in self.G:
            m = Message(self.node_id,"GOSSIP_SUBSCRIBE","default")
            self.send(g,m,message_queues)

    def broadcast(self,type,message,node_message_lists):
        # only used by originator
        if self.is_originator:
            m = Message(self.node_id,type,message)
            print("\toriginator in broadcast function; broadcasting message {}. calling dispatch...".format(str(m)))
            self.dispatch(m,node_message_lists)

    def send(self,node,message,node_message_lists):
        self.num_messages_sent += 1
        print("node {} appending message {} to node {}'s message list, {}".format(self.node_id,str(message),node,str([str(message) for message in node_message_lists[node]])))
        node_message_lists[node].append(message)
        print("\t node {}'s message list after sending the message: {}".format(node,str([str(message) for message in node_message_lists[node]])))

    def dispatch(self,message,node_message_lists):
        print("this node's 'delivered' message is {}; attempting to dispatch message {}...".format(str(self.delivered),str(message)))
        if self.delivered.type == "DEFAULT": # no message has been delivered yet
            print("\tnode {}'s delivered type is default, and outgoing message's type is not default...".format(self.node_id))
            self.delivered = message
            print("\tsending message to nodes: {}".format(self.G))
            for node in self.G:
                self.send(node,message,node_message_lists)

    def receive(self,node_message_lists):
        if len(node_message_lists[self.node_id]) == 0:
            return False
        message = node_message_lists[self.node_id][0]
        node_message_lists[self.node_id].pop(0)
        print("node {} receiving message {}".format(self.node_id,str(message)))
        if message.type == "GOSSIP_SUBSCRIBE":
            print("\t node {} receiving a gossip subscription from node {}. adding to gossip set...".format(self.node_id,message.originator))
            if not self.delivered.type == "DEFAULT":
                # self already delivered a value, so send it along to the node requesting a gossip subscription
                m = Message(self.node_id,self.delivered.type,self.delivered.content,self.delivered.signature)
                self.send(message.originator,m,node_message_lists)
            self.G.add(message.originator)
            return True

        elif message.type == "GOSSIP":
            print("\tnode {} receiving gossip from node {}; dispatching...".format(self.node_id,message.originator))
            if self.verify(message):
                self.dispatch(message,node_message_lists)
            return True

        return False

    def verify(self,message):
        return True

    def deliver(self,message,delivered_message_list):
        self.delivered = message
        delivered_message_list.append(message)
        # this is the last action a node will take


