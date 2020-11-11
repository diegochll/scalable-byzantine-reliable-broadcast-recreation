from scipy.stats import poisson
import threading
from config import DEBUG
from debug_utils import debug, stringify_queue, print_queue_status
from utils import Message, MessageTransport, get_random_sample
from message_types import READY_SUBSCRIBE, READY, SEND, GOSSIP_SUBSCRIBE, ECHO_SUBSCRIBE, GOSSIP, ECHO
 


def pcb_sample(size,num_nodes,node_id):
    to_ret = set()
    for i in range(size):
        to_ret.add(get_random_sample(1,num_nodes,node_id)[0])
    return to_ret


class Node:
    def __init__(self,node_id,expected_sample_size,num_nodes,node_message_lists,echo_sample_size, ready_sample_size, delivery_threshold, contagion_threshold):
        self.node_id = node_id
        self.num_messages_sent = 0
        self.is_originator = False
        self.event = threading.Event()

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
            self.send(e, MessageTransport(self.node_id, ECHO_SUBSCRIBE, Message(ECHO_SUBSCRIBE, ""), node_message_lists)
        self.echo_subscription_set = set()

    #*********** prb_init(self, node_id, ready_sample_size, delivery_sample_size, num_nodes, node_message_lists, delivery_threshold, contagion_threshold)
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


    def pcb_broadcast(self, type, message_content, node_message_lists):
        message =  Message(content=message_content, signature="")
        self.pcb_delivered = message #last input should maybe be self.sign(message)
        self.pb_broadcast(type, message,node_message_lists)
    
    def prb_broadcast(self, type, message, node_message_lists):
        self.pcb_broadcast(self, type, message, node_message_lists)

    def pb_broadcast(self,type,message,node_message_lists):
        # only used by originator
        if self.is_originator:
            debug("\toriginator in broadcast function; broadcasting message {}. calling dispatch...".format(str(m)))
            self.dispatch(m,node_message_lists)

    def send(self, recipient_node_id, message_transport, node_message_lists):
        self.num_messages_sent += 1
        print_queue_status(self.node_id, message_transport, recipient_node_id, node_message_lists[recipient_node_id], sending=True)
        node_message_lists[recipient_node_id].put(message_transport)
        print_queue_status(self.node_id, message_transport, recipient_node_id, node_message_lists[recipient_node_id], sending=False)

    def dispatch(self, message, node_message_lists):
        debug("this node's 'pb_delivered' message is {}; attempting to dispatch message {}...".format(str(self.pb_delivered),str(message)))
        if self.pb_delivered is None: # no message has been delivered yet
            debug("\tnode {}'s delivered type is default, and outgoing message's type is not default...".format(self.node_id))
            self.pb_delivered = message
            debug("\tsending message to nodes: {}".format(self.G))
            for node in self.G:
                self.send(node, MessageTransport(self.node_id, GOSSIP, message), node_message_lists)
            self.pb_deliver(message, node_message_lists)

    def receive(self,node_message_lists):
        
        if node_message_lists[self.node_id].qsize() == 0: # why would this happen?
            return False
        
        debug("node {} receiving message {}".format(self.node_id,str(message)))
        
        message_transport = node_message_lists[self.node_id].get()
        message = message_transport.message
        message_originator = message_transport.originator
        message_type = message_transport.type

        if message_type == GOSSIP_SUBSCRIBE:
            debug("\t node {} receiving a gossip subscription from node {}. adding to gossip set...".format(self.node_id,message.originator))
            if self.pb_delivered != None:
                # self already delivered a value, so send it along to the node requesting a gossip subscription
                message_transport = MessageTransport(self.node_id, GOSSIP, self.pb_delivered)
                self.send(message_originator, message_transport, node_message_lists)
            self.G.add(message_originator)
            return True

        elif message_type == GOSSIP:
            debug("\tnode {} receiving gossip from node {}; dispatching...".format(self.node_id,message.originator))
            if self.verify(message):
                self.dispatch(message,node_message_lists)
            return True

        elif message_type == ECHO:
            if message_originator in self.echo_sample and message_originator not in self.pcb_replies.keys() and self.verify(message):
                self.pcb_replies[message.originator] = message
                if len(self.pcb_replies.keys()) >= self.delivery_threshold:
                    self.pcb_delivered = message
                    #trigger pcb.delivered
                    self.pcb_deliver(message, node_message_lists)

        elif message_type == ECHO_SUBSCRIBE:
            if self.echo != None:
                m = self.echo
                self.send(message.originator,m,node_message_lists)
            self.echo_subscription_set.add(message.originator)

        elif message_type == READY_SUBSCRIBE:
            for ready_message in self.ready_messages:
                message_to_send = MessageTransport(message_originator, READY, ready_message) 
                self.send(message_originator, message_to_send, node_message_lists)
            ready_subscribed.append(message_originator)

        elif message_type == READY:
            if verify(message):
                reply = message
                if message_originator in self.ready_group:
                    self.replies.ready[message_originator].add(message)
                if message_originator in self.delivery_group:
                    self.replies.delivery[message_originator].add(message)

        check_message_ready(node_message_lists)
        check_message_delivered()
        return False

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
    
    def pcb_deliver(self, message, node_message_lists):
        if self.verify(message):
            self.ready_messages.append(message)
            message_ready_transport = MessageTransport(self.node_id, READY, message)
            for recipient_node in self.ready_subscribed:
                self.send(recipient_node, message_ready_transport, node_message_lists)
    
    def verify(self,message):
        return True

    def pb_deliver(self, message, node_message_lists):
        debug("node {} calling pb_deliver...".format(self.node_id))
        if self.verify(message):
            self.echo = message
            debug("node {} sending echo message {}\n\tto echo subscription set {}".format(self.node_id,self.echo,str(self.echo_subscription_set)))
            for e in self.echo_subscription_set:
                self.send(e, MessageTransport(self.node_id, ECHO, message), node_message_lists)


