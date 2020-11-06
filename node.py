from scipy.stats import poisson

def get_random_sample(expected_sample_size,num_nodes,node_id):
    #todo: implement Omega from paper
    sample_size = poisson(expected_sample_size)
    while(sample_size<=0 or sample_size>=(num_nodes-1)): #can pick at most num_nodes-2 neighbors
        sample_size = poisson(expected_sample_size)
    sample = []
    for x in range(sample_size):
        randIndex = randint(num_nodes)
        while(randIndex == node_id or randIndex in sample):
            randIndex = randint(num_nodes)
        sample.append(randIndex)
    return sample

class Node:

    def __init__(self,node_id,expected_sample_size,num_nodes,message_queues):
        self.node_id = node_id
        self.G = get_random_sample(expected_sample_size,num_nodes, self.node_id)
        for g in self.G:
            self.send(g,"GOSSIP_SUBSCRIBE",message_queues)
        self.is_originator = False

    def broadcast(self,message,node_message_lists):
        # only used by originator
        if not self.is_originator:
            return

        for message_list in node_message_lists:
            message_list.append(message)

        yield

    def send(self,node,message,node_message_lists):
        node_message_lists[node].append(message)

    def receive(self,node_message_lists):
        message = node_message_lists[self.node_id][0]
        node_message_lists[self.node_id].pop()
        if message == "GOSSIP_SUBSCRIBE":
            if not delivered

    def deliver(self,message,delivered_message_list):
        delivered_message_list.append(message)
        # this is the last action a node will take


