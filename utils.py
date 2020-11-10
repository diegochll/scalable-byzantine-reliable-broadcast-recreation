from numpy import random


class MessageTransport():
    def __init__(self, originator, message_type, message):
        self.originator = originator
        self.message = message
        self.message_type = message_type 

class Message:
    def __init__(self, content, signature = ""):
        self.content = content
        self.signature = signature

    def __str__(self):
        return "Message(from: '{}'; type: '{}'; content: '{}'; signature: '{}')".format(self.originator,self.type, self.content,self.signature)


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