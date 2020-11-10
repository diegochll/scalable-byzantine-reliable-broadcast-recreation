from scipy.stats import poisson
import threading
from config import DEBUG
from debug_utils import debug, stringify_queue, print_queue_status
from utils import Message, get_random_sample

MODE = "MURMUR"


def pcb_sample(size,num_nodes,node_id):
    to_ret = set()
    for i in range(size):
        to_ret.add(get_random_sample(1,num_nodes,node_id)[0])
    return to_ret


class Node:
    def __init__(self,node_id,expected_sample_size,num_nodes,node_message_lists,echo_sample_size,delivery_threshold):
        self.node_id = node_id
        self.num_messages_sent = 0
        self.is_originator = False
        self.event = threading.Event()

    #********* pb_init(self,expected_sample_size,num_nodes,message_queues):
        self.G = set(get_random_sample(expected_sample_size, num_nodes, self.node_id))
        for g in self.G:
            m = Message(self.node_id,"GOSSIP_SUBSCRIBE","default")
            self.send(g,m,node_message_lists)
        self.pb_delivered = Message(-1, "DEFAULT", "")

    #********** pcb_init(self,echo_sample_size,delivery_threshold,num_nodes,message_queues):
        self.echo = Message(-1,"DEFAULT","")
        self.echo_sample = pcb_sample(echo_sample_size, num_nodes,self.node_id)
        self.delivery_threshold = delivery_threshold

        self.pcb_delivered = Message(-1,"DEFAULT","")
        self.replies = {}
        for e in self.echo_sample:
            self.send(e,Message(self.node_id,"ECHO_SUBSCRIBE","default",""), node_message_lists)
        self.echo_subscription_set = set()

    def pcb_broadcast(self,type,message,node_message_lists):
        self.pcb_delivered = Message(self.node_id,type,message,"")#last input should maybe be self.sign(message)
        self.pb_broadcast(type,message,node_message_lists)
    
    def prb_broadcast(self, type, message, node_message_lists):
        self.pcb_broadcast(self, type, message, node_message_lists)

    

    def pb_broadcast(self,type,message,node_message_lists):
        # only used by originator
        if self.is_originator:
            m = Message(self.node_id,type,message)
            debug("\toriginator in broadcast function; broadcasting message {}. calling dispatch...".format(str(m)))
            self.dispatch(m,node_message_lists)

    def send(self,recipient_node_id, message, node_message_lists):
        self.num_messages_sent += 1
        print_queue_status(self.node_id, message, recipient_node_id, node_message_lists[recipient_node_id], sending=True)
        node_message_lists[recipient_node_id].put(message)
        print_queue_status(self.node_id, message, recipient_node_id, node_message_lists[recipient_node_id], sending=False)

    def dispatch(self,message,node_message_lists):
        debug("this node's 'pb_delivered' message is {}; attempting to dispatch message {}...".format(str(self.pb_delivered),str(message)))
        if self.pb_delivered.type == "DEFAULT": # no message has been delivered yet
            debug("\tnode {}'s delivered type is default, and outgoing message's type is not default...".format(self.node_id))
            self.pb_delivered = message
            debug("\tsending message to nodes: {}".format(self.G))
            for node in self.G:
                self.send(node,Message(self.node_id,message.type,message.content,message.signature),node_message_lists)
            self.pb_deliver(message,node_message_lists)

    def receive(self,node_message_lists):
        if node_message_lists[self.node_id].qsize() == 0: # why would this happen?
            return False
        message = node_message_lists[self.node_id].get()
        debug("node {} receiving message {}".format(self.node_id,str(message)))
        if message.type == "GOSSIP_SUBSCRIBE":
            debug("\t node {} receiving a gossip subscription from node {}. adding to gossip set...".format(self.node_id,message.originator))
            if not self.pb_delivered.type == "DEFAULT":
                # self already delivered a value, so send it along to the node requesting a gossip subscription
                m = Message(self.node_id,self.pb_delivered.type,self.pb_delivered.content,self.pb_delivered.signature)
                self.send(message.originator,m,node_message_lists)
            self.G.add(message.originator)
            return True

        elif message.type == "GOSSIP":
            debug("\tnode {} receiving gossip from node {}; dispatching...".format(self.node_id,message.originator))
            if self.verify(message):
                self.dispatch(message,node_message_lists)
            return True

        elif message.type == "ECHO":
            if message.originator in self.echo_sample and message.originator not in self.replies.keys() and self.verify(message):
                self.replies[message.originator] = message
                if len(self.replies.keys()) >= self.delivery_threshold:
                    self.pcb_delivered = message
                    #trigger pcb.delivered

        elif message.type == "ECHO_SUBSCRIBE":
            if self.echo.type != "DEFAULT":
                m = self.echo
                self.send(message.originator,m,node_message_lists)
            self.echo_subscription_set.add(message.originator)

        return False

    def verify(self,message):
        return True

    def pb_deliver(self,message,node_message_lists):
        debug("node {} calling pb_deliver...".format(self.node_id))
        if self.verify(message):
            self.echo = Message(self.node_id,message.type,message.content,message.signature)
            debug("node {} sending echo message {}\n\tto echo subscription set {}".format(self.node_id,self.echo,str(self.echo_subscription_set)))
            for e in self.echo_subscription_set:
                self.send(e,Message(self.node_id,"ECHO",message.content,message.signature),node_message_lists)


