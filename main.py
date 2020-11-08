from debug_utils import debug, stringify_queue
from config import NODE_AMOUNT, EXPECTED_SAMPLE_SIZE, CORRECT_MESSAGE, ECHO_SAMPLE_SIZE, DELIVERY_THRESHOLD, DEBUG
from node import Node
import threading
import queue

#todo: implement malicious node logic
#todo: implement diameter calculations
#todo: implement latency calculations
#todo: implement message signatures + verification logic

node_message_lists = [queue.Queue() for i in range(NODE_AMOUNT)]
node_message_lists_lock = threading.Lock()
nodes = [Node(i,EXPECTED_SAMPLE_SIZE,NODE_AMOUNT,node_message_lists, ECHO_SAMPLE_SIZE,DELIVERY_THRESHOLD) for i in range(NODE_AMOUNT)]
messages_delivered = []
num_messages_delivered = 0
old_num_messages_in_queues = 0



def acquire_node_message_lock():
    if DEBUG:
       node_message_lists_lock.acquire(True)

def release_node_message_lock():
    if DEBUG:
        node_message_lists_lock.release()

"""
if a node's message queue is nonempty, sets that node's event bool to true - this is how nodes will be reawoken after blocking
:return false iff there are no more messages in any queue
"""
def wake_up_nodes():
    global debug
    global node_message_lists
    global nodes
    debug("waking up nodes...")
    to_ret = True
    for i,queue in enumerate(node_message_lists):
        if not queue.qsize() == 0:
            debug( "\twaking up node {}".format(i))
            nodes[i].event.set()
            to_ret = False
    debug("done waking up nodes. returning {}".format(to_ret))
    wake_all_nodes()
    return to_ret


def wake_all_nodes():
    global nodes
    for node in nodes:
        node.event.set()


"""
while there are messages left in any queue, there is some possibility that a node will have a message delivered to it
if a node is unable to process some message or has no more messages, it will block until its event is set
"""
def handle_messages(node_number, node):
    global node_message_lists
    #print("I am node: " + str(node_number) + "; this is my message queue: " + str([str(message) for message in node_message_lists[node_number]]))
    debug("\tthis is my gossip set: {}".format(node.G))
    # the first node is the originator and needs to send out the message to decide upon
    if node_number == 0:
        debug("I am node 0 and the originator; broadcasting gossip with correct value...")
        node.is_originator = True
        acquire_node_message_lock()
        node.pcb_broadcast("GOSSIP",CORRECT_MESSAGE,node_message_lists)
        release_node_message_lock()

    acquire_node_message_lock()
    while not wake_up_nodes():
        debug("there are messages left in the message queues; node {} is processing incoming messages...".format(node_number))
        debug("\tnode {}'s message list: {}".format(node_number,stringify_queue(node_message_lists[node_number])))
        if not node.receive(node_message_lists):
            debug("node {} could not receive a message. blocking until it is woken...".format(node_number))
            # there are no more messages for this node (or some sort of failure on receiving a message); we want to yield to the next node
            release_node_message_lock()
            node.event.clear()
            node.event.wait()
            acquire_node_message_lock()
    debug("message list is empty.")
    release_node_message_lock()
    return



def main():
    debug("starting simulation.")
    global node_message_lists
    global nodes

    node_workers = []
    for i in range(NODE_AMOUNT):
        node_worker = threading.Thread(target=handle_messages, args=(i, nodes[i]))
        node_worker.start()
        node_workers.append(node_worker)

    for worker in node_workers:
        worker.join()

    debug("finished simulation.")
    debug("counting number of nodes which delivered the correct message...")
    num_correct_delivery = 0
    num_messages_delivered = 0
    for node in nodes:
        if node.pcb_delivered.type != "DEFAULT" and node.pcb_delivered.content == CORRECT_MESSAGE:
            num_correct_delivery += 1
            num_messages_delivered += node.num_messages_sent
    print("number of nodes which delivered the correct message: {}".format(num_correct_delivery))
    print("total messages sent: {}".format(num_messages_delivered))


if __name__ == "__main__":
    main()