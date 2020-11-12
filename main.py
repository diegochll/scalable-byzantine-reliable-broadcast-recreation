from config import NODE_AMOUNT, EXPECTED_SAMPLE_SIZE, CORRECT_MESSAGE, ECHO_SAMPLE_SIZE, DELIVERY_THRESHOLD, DEBUG, DELIVERY_SAMPLE_SIZE,CONTAGION_THRESHOLD, READY_SAMPLE_SIZE
from node import Node
from utils import Message, MessageTransport
import threading
import queue
import time
from debug_utils import print_queue_status, debug_print,stringify_queue
import math
import matplotlib.pyplot as plt
from numpy import random
import timer
from collections import deque

class Experiment:
    def __init__(self,node_amount, expected_sample_size, correct_message, echo_sample_size, delivery_threshold, delivery_sample_size, contagion_threshold, ready_sample_size):
        self.node_message_lists = [queue.Queue() for i in range(node_amount)]
        self.node_message_lists_lock = threading.Lock()
        self.nodes = []
        self.id_map = {}
        for i in range(node_amount):
            node = Node(i, expected_sample_size, node_amount, self.node_message_lists, echo_sample_size, ready_sample_size, delivery_sample_size, delivery_threshold, contagion_threshold)
            self.nodes.append(node)
            self.id_map[i] = node
        self.messages_delivered = []
        self.num_messages_sent = 0
        self.old_num_messages_in_queues = 0
        self.correct_message = correct_message
        self.num_nodes = node_amount
        self.num_correct_delivery = 0
        self.num_messages_delivered = 0
        self.time_taken = 0
        self.timed_out = False
        self.timed_out_timer = threading.Timer(120,self.time_out)#2 minute time out

    #todo: implement malicious node logic
    #todo: implement diameter calculations
    #todo: implement latency calculations
    #todo: implement message signatures + verification logic

    def acquire_node_message_lock(self):
        if DEBUG:
           self.node_message_lists_lock.acquire(True)

    def release_node_message_lock(self):
        if DEBUG:
            self.node_message_lists_lock.release()

    def check_connected(self):
        q = deque()
        visited = set()
        q.appendleft(self.nodes[0].node_id)
        while len(q) != 0:
            node_id = q.pop()
            visited.add(node_id)
            node = self.id_map[node_id]
            for neighbor_node in node.G:
                if neighbor_node not in visited:
                    q.appendleft(neighbor_node)
        return len(visited)

    """
    if a node's message queue is nonempty, sets that node's event bool to true - this is how nodes will be reawoken after blocking
    :return false iff there are no more messages in any queue
    """
    def wake_up_nodes(self):
        debug_print("waking up nodes...")
        to_ret = True
        for i,queue in enumerate(self.node_message_lists):
            if not queue.qsize() == 0:
                debug_print( "\twaking up node {}".format(i))
                self.nodes[i].event.set()
                to_ret = False
        debug_print("done waking up nodes. returning {}".format(to_ret))
        self.wake_all_nodes()
        return to_ret

    def wake_all_nodes(self):
        for node in self.nodes:
            node.event.set()

    """
    while there are messages left in any queue, there is some possibility that a node will have a message delivered to it
    if a node is unable to process some message or has no more messages, it will block until its event is set
    """
    def handle_messages(self,node_number, node):
        #print("I am node: " + str(node_number) + "; this is my message queue: " + str([str(message) for message in node_message_lists[node_number]]))
        debug_print("\tthis is my gossip set: {}".format(node.G))
        # the first node is the originator and needs to send out the message to decide upon
        if node_number == 0:
            debug_print("I am node 0 and the originator; broadcasting gossip with correct value...")
            node.is_originator = True
            self.acquire_node_message_lock()
            node.prb_broadcast(self.correct_message,self.node_message_lists)
            self.release_node_message_lock()

        self.acquire_node_message_lock()
        while not self.wake_up_nodes() and not self.timed_out:
            debug_print("there are messages left in the message queues; node {} is processing incoming messages...".format(node_number))
            debug_print("\tnode {}'s message list: {}".format(node_number,stringify_queue(self.node_message_lists[node_number])))
            if not node.receive(self.node_message_lists):
                debug_print("node {} could not receive a message. blocking until it is woken...".format(node_number))
                # there are no more messages for this node (or some sort of failure on receiving a message); we want to yield to the next node
                self.release_node_message_lock()
                node.event.clear()
                node.event.wait()
                self.acquire_node_message_lock()
        debug_print("message list is empty.")
        self.release_node_message_lock()
        return

    def time_out(self):
        self.timed_out = True

    def run(self):
        debug_print("starting simulation.")

        node_workers = []
        for i in range(self.num_nodes):
            node_worker = threading.Thread(target=self.handle_messages, args=(i, self.nodes[i]))
            node_worker.start()
            node_workers.append(node_worker)
        self.timed_out_timer.start()
        start = float(time.time())
        for worker in node_workers:
            worker.join()
        end = float(time.time())
        self.timed_out_timer.cancel()
        self.time_taken = float(end-start)
        debug_print("finished simulation.")
        debug_print("counting number of nodes which delivered the correct message...")

        for node in self.nodes:
            self.num_messages_sent += node.num_messages_sent
            debug_print("\tnode {} prb delivered message {}".format(node.node_id,node.prb_delivered_message))
            if node.prb_delivered_message is not None and node.prb_delivered_message.content == self.correct_message:
                self.num_correct_delivery += 1
        debug_print("number of nodes which delivered the correct message: {}".format(self.num_correct_delivery))
        debug_print("total messages sent: {}".format(self.num_messages_sent))
        debug_print("total time taken for simulation: {}".format(self.time_taken))


if __name__ == "__main__":
    random.seed(int(time.time()))
    node_amounts = [10,20,50,100,150,200,250,300,350,400,450,500,600,700,800,900,1000]
    times = []
    messages = []
    num_nodes_correct = []
    for i,node_amount in enumerate(node_amounts):
        e = Experiment(node_amount=node_amount,expected_sample_size=math.ceil(math.log2(node_amount)),correct_message="CORRECT_MESSAGE",echo_sample_size=math.ceil(math.log2(node_amount)),delivery_threshold=math.ceil(math.log2(math.log2(node_amount))),delivery_sample_size=math.ceil(math.log2(node_amount)),ready_sample_size=math.ceil(math.log2(node_amount)),contagion_threshold=math.ceil(math.log2(math.log2(node_amount))))
        e.run()
        times.append(e.time_taken)
        messages.append(e.num_messages_sent)
        num_nodes_correct.append(e.num_correct_delivery)
        if e.timed_out:
            print("TIMED OUT. num nodes: {}; sample sizes: {}; thresholds: {} connected nodes: {}".format(node_amount,math.ceil(math.log2(node_amount)),math.ceil(math.log2(math.log2(node_amount)), e.check_connected())))
        else:
            print("ran experiment. num messages sent: {}, num nodes correct: {}, time taken to come to consensus: {}, connect nodes: {}".format(e.num_messages_sent,e.num_correct_delivery,e.time_taken, e.check_connected()))

    fig,ax = plt.figure()
    plt.title("times")
    ax.plot(times,node_amounts)