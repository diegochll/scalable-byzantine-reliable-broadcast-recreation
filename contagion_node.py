import threading
from config import DEBUG
from debug_utils import debug, stringify_queue, print_queue_status
from message_types import READY_SUBSCRIBE, READY, SEND

class ContagionNode:
    def __init__(self, node_id, ready_sample_size, delivery_sample_size, num_nodes, node_message_lists, delivery_threshold, contagion_threshold):
        self.node_id = node_id
        self.ready_group = get_random_sample(ready_sample_size, num_nodes, node_id)
        self.delivery_group = get_random_sample(delivery_sample_size, node_id)
        self.ready_messages = []
        self.delivered = False
        self.ready_subscribed = [] # R with squiggly on top
        for recipient_node_id in self.ready_group:
            message = Message(self.node_id, READY_SUBSCRIBE, "default")
            self.send(recipient_node_id, message, node_message_lists)
        for recipient_node_id in self.delivery_group:
            message = Message(self.node_id, READY_SUBSCRIBE, "default")
            self.send(recipient_node_id, message, node_message_lists)        
        self.delivery_threshold = delivery_threshold
        self.contagion_threshold = contagion_threshold

    def receive(self, node_message_lists):
        if node_message_lists[self.node_id].qsize() == 0: 
            return False
        message = node_message_lists[self.node_id].get()
        debug("node {} receiving message {}".format(self.node_id,str(message)))
        
        if message.type == READY_SUBSCRIBE:
            for ready_message in self.ready_messages:
                self.send(ready_message.originator, READY, node_message_lists)
            ready_subscribed.append(ready_message)
    
    def prb_broadcast(message):
        if message.type !=  SEND:
            return False
        



    
    def send(self,recipient_node_id, message, node_message_lists):
        self.num_messages_sent += 1
        print_queue_status(self.node_id, message, recipient_node_id, node_message_lists[recipient_node_id], sending=True)
        node_message_lists[recipient_node_id].put(message)
        print_queue_status(self.node_id, message, recipient_node_id, node_message_lists[recipient_node_id], sending=False)