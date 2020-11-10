import threading
from config import DEBUG
from debug_utils import debug, stringify_queue, print_queue_status
from message_types import READY_SUBSCRIBE, READY, SEND
from collections import defaultdict
from utils import Message

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
        self.ready_group = get_random_sample(ready_sample_size, num_nodes, node_id) # R with no squiggly
        self.delivery_group = get_random_sample(delivery_sample_size, node_id)
        self.ready_messages = set() # AKA READY in paper
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
        self.replies = Replies()

    def receive(self, node_message_lists):
        if node_message_lists[self.node_id].qsize() == 0: 
            return False
        message = node_message_lists[self.node_id].get()
        debug("node {} receiving message {}".format(self.node_id,str(message)))
        
        if message.type == READY_SUBSCRIBE:
            for ready_message in self.ready_messages:
                message_to_send = Message(mesage.originator, ready_message.message_type, ready_message.content, signature=ready_message.signature) 
                self.send(message.originator, message_to_send, node_message_lists)
            ready_subscribed.append(message.originator)
        
        if message.type == READY:
            if verify(message):
                reply = message
                if message.originator in self.ready_group:
                    self.replies.ready[message.originator].add(message)
                if message.originator in self.delivery_group:
                    self.replies.delivery[message.originator].add(message)
        
        check_message_ready(node_message_lists)
        check_message_delivered()
                
    def prb_broadcast(message):
        if not self.delivered:
            self.delivered = True
            for node_id in self.delivery_group: # delivery group??
    
    def check_message_delivered():
        messages_delivered = defaultdict(0)
        for node_id, message in self.replies.delivery.items():
            messages_delivered[message.signature] = messages_delivered[message.signature]
        
        potential_delivered_message = list(filter(lambda message_and_count : message_and_count[1] >= self.delivery_threshold, messages_delivered.items()))
        if len(potential_delivered_message) == 1:
            # prb deliver?



    def check_message_ready(node_message_lists):
        messages_received_ready = defaultdict(0)
        for node_id, message in replies.ready.items():
            messages_received_ready[message.signature] = messages_received_ready[message.signature] + 1

        potential_messages_to_send = filter(lambda message_and_count : message_and_count[1] >= self.contagion_threshold,  messages_received_ready.items())
        message_signatures = list(map(lambda message : message.signature, self.ready_messages))
        message_not_sent = list(filter(lambda message : message.signature not in message_signatures, potential_messages_to_send))
        
        if len(message_not_sent) == 1:
            ready_message_to_send = message_not_sent[0]
            self.ready_messages.add(ready_message_to_send)
            _send_ready_messages(ready_message_to_send)
        

    def _send_ready_messages(message, node_message_lists):
        for recipient_node_id in self.ready_subscribed:
            message_ready = Message(self.node_id, message.content, message.signature)
            self.send(recipient_node_id, message, node_message_lists)
    
    def pcb_deliver(self,message,node_message_lists):
        if self.verify(message):
            self.ready_messages.append(message)
            self.ready_msg = Message(self.node_id, READY, message.content,message.signature)
            for recipient_node in self.ready_subscribed:
                self.send(recipient_node, ready_msg, node_message_lists)
    
    def send(self,recipient_node_id, message, node_message_lists):
        self.num_messages_sent += 1
        print_queue_status(self.node_id, message, recipient_node_id, node_message_lists[recipient_node_id], sending=True)
        node_message_lists[recipient_node_id].put(message)
        print_queue_status(self.node_id, message, recipient_node_id, node_message_lists[recipient_node_id], sending=False)

