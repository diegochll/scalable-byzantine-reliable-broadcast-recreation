from scipy.stats import poisson

from numpy import random

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

class Node:
    def __init__(self,node_id,expected_sample_size,num_nodes,message_queues):
        self.node_id = node_id
        self.G = set(get_random_sample(expected_sample_size,num_nodes, self.node_id))
        for g in self.G:
            self.send(g,"GOSSIP_SUBSCRIBE","")
        self.is_originator = False
        self.delivered = Message(-1,"PLACE_HOLDER","")
        async self.message_lists = message_queues

    def broadcast(self,type,message,node_message_lists):
        # only used by originator
        if not self.is_originator:
            return

        for message_list in node_message_lists:
            message_list.append(Message(self.node_id,type,message))

    def send(self,node,type,content):
        self.message_lists[node].append(Message(self.node_id,type,content))

    def dispatch(self,message):
        if self.delivered.type == "PLACE_HOLDER":
            self.delivered = message

            for node in self.G:
                self.send(node,"GOSSIP",message.content)

    def receive(self,node_message_lists):
        if node_message_lists[self.node_id].empty():
            return False
        message = node_message_lists[self.node_id][0]
        node_message_lists[self.node_id].pop()
        if message.type == "GOSSIP_SUBSCRIBE":
            if not self.delivered.content == "":
                message_originator = message.originator
                node_message_lists[message_originator].append(Message(self.node_id,"GOSSIP",self.delivered))
            self.G.add(message.originator)
            return True

        elif message.type == "GOSSIP":
            if self.verify(message):
                self.dispatch(message)
            return True

        return False

    def verify(self,message):
        return True

    def deliver(self,message,delivered_message_list):
        self.delivered = message
        delivered_message_list.append(message)
        # this is the last action a node will take


