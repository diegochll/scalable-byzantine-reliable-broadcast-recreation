from scipy.stats import poisson
import threading
from config import DEBUG
from debug_utils import debug_print, stringify_queue, print_queue_status
from utils import Message, MessageTransport, get_random_sample
from message_types import READY_SUBSCRIBE, READY, SEND, GOSSIP_SUBSCRIBE, ECHO_SUBSCRIBE, GOSSIP, ECHO
import random

def pcb_sample(size,num_nodes,node_id):
    to_ret = set()
    for i in range(size):
        to_ret.add(get_random_sample(1,num_nodes,node_id)[0])
    return to_ret


class Replies:
    def __init__(self):
        self.ready = {} # map from node to set of messages
        self.delivery = {}


class Node:
    def __init__(self, node_id, expected_sample_size, num_nodes, node_message_lists, echo_sample_size, ready_sample_size, delivery_sample_size, delivery_threshold, contagion_threshold, byzantine_ratio):
        self.node_id = node_id
        self.num_messages_sent = 0
        self.is_originator = False
        self.event = threading.Event()
        self.byzantine = random.uniform(0,1) < byzantine_ratio
        print(self.byzantine)
    #********* pb_init(self,expected_sample_size,num_nodes,message_queues):
        self.G = set(get_random_sample(expected_sample_size, num_nodes, self.node_id))
        for g in self.G:
            m = MessageTransport(self.node_id, GOSSIP_SUBSCRIBE, Message(GOSSIP_SUBSCRIBE, signature=""))
            self.send(g, m, node_message_lists)
        self.pb_delivered = None

    #********** pcb_init(self,echo_sample_size,delivery_threshold,num_nodes,message_queues):
        self.echo = None
        self.echo_sample = pcb_sample(echo_sample_size, num_nodes, self.node_id)
        self.delivery_threshold = delivery_threshold
        self.pcb_delivered = None
        self.pcb_replies = {}
        for e in self.echo_sample:
            self.send(e, MessageTransport(self.node_id, ECHO_SUBSCRIBE, Message(ECHO_SUBSCRIBE, "")), node_message_lists)
        self.echo_subscription_set = set()

    #*********** prb_init(self, node_id, ready_sample_size, delivery_sample_size, num_nodes, node_message_lists, delivery_threshold, contagion_threshold)
        self.ready_group = self.sample(ready_sample_size, num_nodes, node_id, READY_SUBSCRIBE, node_message_lists) # R with no squiggly
        self.delivery_group = self.sample(delivery_sample_size, num_nodes, node_id, READY_SUBSCRIBE, node_message_lists)
        self.ready_messages = set() # AKA READY in paper
        self.ready_subscribed = set() # R with squiggly on top
        self.delivery_threshold = delivery_threshold # D carot
        self.contagion_threshold = contagion_threshold  # R carot
        self.replies = Replies()        
        self.delivered = False
        self.prb_delivered_message = None
        self.ready_messages_already_sent = {} #dict from message to set of nodes message has already been sent to

    def sample(self,sample_size, num_nodes, node_id, message_type, node_message_lists):
        node_sample = pcb_sample(sample_size, num_nodes, node_id)
        for recipient_node_id in node_sample:
            message = MessageTransport(self.node_id, message_type, Message("", signature=""))
            self.send(recipient_node_id, message, node_message_lists)
        return node_sample

    def prb_broadcast(self, message_content, node_message_lists):
        self.pcb_broadcast(message_content, node_message_lists)

    def pcb_broadcast(self, message_content, node_message_lists):
        message = Message(content=message_content, signature="from {}".format(self.node_id))
        #only originator calls broadcast - so we can set delivered immediately
        self.pcb_delivered = message #last input should maybe be self.sign(message)
        self.pb_broadcast(message,node_message_lists)

    def pb_broadcast(self,message,node_message_lists):
        # only used by originator
        if self.is_originator:
            debug_print("\toriginator in broadcast function; creating and broadcasting message with content {}. calling dispatch...".format(str(message)))
            self.dispatch(message,node_message_lists)

    def dispatch(self, message, node_message_lists):
        #only originator calls dispatch
        debug_print("this node's 'pb_delivered' message is {}; attempting to dispatch message {}...".format(str(self.pb_delivered),str(message)))
        if self.pb_delivered is None: # no message has been delivered yet
            debug_print("\tnode {}'s delivered type is default, and outgoing message's type is not default...".format(self.node_id))
            self.pb_delivered = message
            debug_print("\tsending message to nodes: {}".format(self.G))
            for node in self.G:
                self.send(node, MessageTransport(self.node_id, GOSSIP, message), node_message_lists)
            self.pb_deliver(message, node_message_lists)

    def send(self, recipient_node_id, message_transport, node_message_lists):
        if type(message_transport) is not MessageTransport:
            exit(-1)
        if recipient_node_id == self.node_id:
            return
        self.num_messages_sent += 1
        if DEBUG:
            print_queue_status(self.node_id, message_transport, recipient_node_id, node_message_lists[recipient_node_id], sending=True)
        node_message_lists[recipient_node_id].put(message_transport)
        if DEBUG:
            print_queue_status(self.node_id, message_transport, recipient_node_id, node_message_lists[recipient_node_id], sending=False)

    def receive(self,node_message_lists):
        if node_message_lists[self.node_id].empty():
            return False
        message_transport = node_message_lists[self.node_id].get()
        message = message_transport.message
        message_originator = message_transport.originator
        message_type = message_transport.message_type

        debug_print("node {} receiving message {}".format(self.node_id, str(message_transport)))
        if not self.byzantine:
            if message_type == GOSSIP_SUBSCRIBE:
                debug_print("\t node {} receiving a gossip subscription from node {}. adding to gossip set...".format(self.node_id,message_originator))
                if self.pb_delivered != None:
                    # self already delivered a value, so send it along to the node requesting a gossip subscription
                    message_transport = MessageTransport(self.node_id, GOSSIP, self.pb_delivered)
                    self.send(message_originator, message_transport, node_message_lists)
                self.G.add(message_originator)
                return True

            elif message_type == GOSSIP:
                debug_print("\tnode {} receiving gossip from node {}; dispatching...".format(self.node_id,message_originator))
                if self.verify(message):
                    self.dispatch(message,node_message_lists)
                return True

            elif message_type == ECHO:
                if message_originator in self.echo_sample and message_originator not in self.pcb_replies.keys() and self.verify(message):
                    self.pcb_replies[message_originator] = message
                    if len(self.pcb_replies.keys()) >= self.delivery_threshold:
                        self.pcb_delivered = message
                        #trigger pcb.delivered
                        self.pcb_deliver(message, node_message_lists)
                return True

            elif message_type == ECHO_SUBSCRIBE:
                if self.echo != None:
                    m = self.echo
                    self.send(message_originator,MessageTransport(self.node_id,ECHO,m),node_message_lists) # update here
                self.echo_subscription_set.add(message_originator)
                return True

            elif message_type == READY_SUBSCRIBE:
                for ready_message in self.ready_messages:
                    message_to_send = MessageTransport(message_originator, READY, ready_message) 
                    self.send(message_originator, message_to_send, node_message_lists)
                self.ready_subscribed.add(message_originator)
                return True

            elif message_type == READY:
                if self.verify(message):
                    #reply = message
                    if message_originator in self.ready_group:
                        if message_originator not in self.replies.ready:
                            self.replies.ready[message_originator] = set()
                        self.replies.ready[message_originator].add(message)

                    if message_originator in self.delivery_group:
                        if message_originator not in self.replies.delivery:
                            self.replies.delivery[message_originator] = set()
                        self.replies.delivery[message_originator].add(message)

                self.check_message_ready(node_message_lists)
                self.check_message_delivered()
                return True

        return False

    def check_message_delivered(self):
        if self.delivered:
            return
        messages_delivered = {}

        for node_id, message_set in self.replies.delivery.items():
            for message in message_set:
                if message not in messages_delivered:
                    messages_delivered[message] = set()
                messages_delivered[message].add(node_id)

        for message, node_set in messages_delivered.items():
            if len(node_set) >= self.delivery_threshold and not self.delivered:
                self.delivered = True
                # trigger, prb_deliver
                self.prb_delivered_message = message
                break

        #
        # potential_delivered_message = list(filter(lambda message_and_count : message_and_count[1] >= self.delivery_threshold, messages_delivered.items()))
        #
        # if len(potential_delivered_message) == 1:
        #     self.delivered = True
        #     self.delivered_message = potential_delivered_message[0]
    def check_message_ready(self,node_message_lists):
        #convert dictionary from nodes to list of ready reply messages to dictionary from message to set of nodes which have sent it
        messages_received_ready = {}
        for node_id, message_set in self.replies.ready.items():
            for message in message_set:
                if message not in messages_received_ready:
                    messages_received_ready[message] = set()
                messages_received_ready[message].add(node_id)

        for message, node_set in messages_received_ready.items():
            if len(node_set) >= self.contagion_threshold:
                if message not in self.ready_messages_already_sent.keys():
                    self.ready_messages_already_sent[message] = set()

                self.ready_messages.add(message)
                for node in self.ready_subscribed:
                    if node not in self.ready_messages_already_sent[message]:
                        #only send each ready message to each node once
                        self.send(node, MessageTransport(self.node_id, READY, message), node_message_lists)
                    self.ready_messages_already_sent[message].add(node)

                break
        #
        # potential_messages_to_send = filter(lambda message_and_count : message_and_count[1] >= self.contagion_threshold,messages_received_ready.items())
        # message_not_sent = list(filter(lambda message : message not in self.ready_messages, potential_messages_to_send))
        #
        # if len(message_not_sent) == 1:
        #     ready_message_to_send = message_not_sent[0]
        #     self.ready_messages.add(ready_message_to_send)
        #     self._send_ready_messages(ready_message_to_send,node_message_lists)

    def _send_ready_messages(self,message, node_message_lists):
        message_ready_transport = MessageTransport(self.node_id, READY, message)
        for recipient_node_id in self.ready_subscribed:
            self.send(recipient_node_id, message_ready_transport, node_message_lists)
    
    def pcb_deliver(self, message, node_message_lists):
        debug_print("node {} calling pcb deliver on message {}".format(str(self.node_id),str(message)))
        if self.verify(message):
            self.ready_messages.add(message)
            message_ready_transport = MessageTransport(self.node_id, READY, message)
            for recipient_node in self.ready_subscribed:
                self.send(recipient_node, message_ready_transport, node_message_lists)
    
    def verify(self,message_transport):
        return True

    def pb_deliver(self, message, node_message_lists):
        debug_print("node {} calling pb_deliver...".format(self.node_id))
        if self.verify(message):
            self.echo = message
            debug_print("node {} sending echo message {}\n\tto echo subscription set {}".format(self.node_id,self.echo,str(self.echo_subscription_set)))
            for e in self.echo_subscription_set:
                self.send(e, MessageTransport(self.node_id, ECHO, message), node_message_lists)
