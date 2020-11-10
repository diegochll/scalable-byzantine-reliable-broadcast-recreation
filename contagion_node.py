import threading
from config import DEBUG
from debug_utils import debug, stringify_queue, print_queue_status
from message_types import READY_SUBSCRIBE, READY, SEND
from collections import defaultdict
from utils import Message, MessageTransport

class Replies():
    def __init__():
        self.ready = defaultdict(set)
        self.delivery = defaultdict(set)


def verify(message):
    return True # have oracle decide in the future

    

# make signatures UNIQUE to a message
class ContagionNode:
    def __init__(self, node_id, ready_sample_size, delivery_sample_size, num_nodes, node_message_lists, delivery_threshold, contagion_threshold):
        self.node_id = node_id
        self.ready_group = self.sample(ready_sample_size, num_nodes, node_id, READY_SUBSCRIBE, node_message_lists) # R with no squiggly
        self.delivery_group = self.sample(delivery_sample_size, num_nodes, node_id, READY_SUBSCRIBE, node_message_lists)
        self.ready_messages = set() # AKA READY in paper
        self.ready_subscribed = [] # R with squiggly on top  
        self.delivery_threshold = delivery_threshold # D carot
        self.contagion_threshold = contagion_threshold  # R carot
        self.replies = Replies()        
        self.delivered = False
        self.delivered_message = ""
    
    def sample(sample_size, num_nodes, node_id, message_type, node_message_lists):
        node_sample = get_random_sample(sample_size, num_nodes, node_id)
        for recipient_node_id in node_sample:
            message = MessageTransport(self.node_id, READY_SUBSCRIBE, Message(READY_SUBSCRIBE, signature=READY_SUBSCRIBE))
            self.send(recipient_node_id, message, node_message_lists)
        return node_sample


    def receive(self, node_message_lists):
        if node_message_lists[self.node_id].qsize() == 0: 
            return False

        message_transport = node_message_lists[self.node_id].get()
        message = message_transport.message
        message_originator = message_transport.originator

        if message_transport.type == READY_SUBSCRIBE:
            for ready_message in self.ready_messages:
                message_to_send = MessageTransport(message_originator, READY, ready_message) 
                self.send(message_originator, message_to_send, node_message_lists)
            ready_subscribed.append(message_originator)

        if message_transport.type == READY:
            if verify(message):
                reply = message
                if message_originator in self.ready_group:
                    self.replies.ready[message_originator].add(message)
                if message_originator in self.delivery_group:
                    self.replies.delivery[message_originator].add(message)
        
        check_message_ready(node_message_lists)
        check_message_delivered()
                
    def prb_broadcast(message):
        if not self.delivered:
            self.delivered = True
            for node_id in self.delivery_group: # delivery group??
    
    def check_message_delivered():
        messages_delivered = defaultdict(0)
        for node_id, message in self.replies.delivery.items():
            messages_delivered[message] = messages_delivered[message] + 1
        
        potential_delivered_message = list(filter(lambda message_and_count : message_and_count[1] >= self.delivery_threshold, messages_delivered.items()))
        
        if len(potential_delivered_message) == 1:
            self.delivered = True
            self.delivered_message = potential_delivered_message[0]



    def check_message_ready(node_message_lists):
        messages_received_ready = defaultdict(0)
        for node_id, message in replies.ready.items():
            messages_received_ready[message] = messages_received_ready[message] + 1

        potential_messages_to_send = filter(lambda message_and_count : message_and_count[1] >= self.contagion_threshold,  messages_received_ready.items())
        message_not_sent = list(filter(lambda message : message not in self.ready_messages, potential_messages_to_send))
        
        if len(message_not_sent) == 1:
            ready_message_to_send = message_not_sent[0]
            self.ready_messages.add(ready_message_to_send)
            _send_ready_messages(ready_message_to_send)
        

    def _send_ready_messages(message, node_message_lists):
        message_ready_transport = MessageTransport(self.node_id, READY, message)
        for recipient_node_id in self.ready_subscribed:
            self.send(recipient_node_id, message_ready_transport, node_message_lists)
    
    def pcb_deliver(self, message_transport, node_message_lists):
        message = message_transport.message
        if self.verify(message):
            self.ready_messages.append(message)
            message_ready_transport = MessageTransport(self.node_id, READY, message)
            for recipient_node in self.ready_subscribed:
                self.send(recipient_node, message_ready_transport, node_message_lists)
    
    def send(self,recipient_node_id, message_transport, node_message_lists):
        self.num_messages_sent += 1
        print_queue_status(self.node_id, message, recipient_node_id, node_message_lists[recipient_node_id], sending=True)
        node_message_lists[recipient_node_id].put(message_transport)
        print_queue_status(self.node_id, message, recipient_node_id, node_message_lists[recipient_node_id], sending=False)

