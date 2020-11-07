from config import NODE_AMOUNT, EXPECTED_SAMPLE_SIZE,CORRECT_MESSAGE
from node import Node
import threading

#todo: implement malicious node logic
#todo: implement diameter calculations
#todo: implement latency calculations
#todo: implement message signatures + verification logic

message_queues = [[] for i in range(NODE_AMOUNT)]
message_queue_lock = threading.Lock()
nodes = [Node(i,EXPECTED_SAMPLE_SIZE,NODE_AMOUNT,message_queues) for i in range(NODE_AMOUNT)]

messages_delivered = []

num_messages_delivered = 0

old_num_messages_in_queues = 0

debug = True

"""
if a node's message queue is nonempty, sets that node's event bool to true - this is how nodes will be reawoken after blocking
:return false iff there are no more messages in any queue
"""
def wake_up_nodes():
    global debug
    global message_queues
    global nodes
    if debug:
        print("waking up nodes...")
    to_ret = True
    for i,queue in enumerate(message_queues):
        if not len(queue) == 0:
            if debug:
                print("\twaking up node {}".format(i))
            nodes[i].event.set()
            to_ret = False
    if debug:
        print("done waking up nodes. returning {}".format(to_ret))
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
    global message_queues
    global message_queue_lock
    print("I am node: " + str(node_number) + "; this is my message queue: " + str([str(message) for message in message_queues[node_number]]))
    print("\tthis is my gossip set: {}".format(node.G))
    global debug
    # the first node is the originator and needs to send out the message to decide upon
    if node_number == 0:
        print("I am node 0 and the originator; broadcasting gossip with correct value...")
        node.is_originator = True
        message_queue_lock.acquire(True)
        node.broadcast("GOSSIP",CORRECT_MESSAGE,message_queues)
        message_queue_lock.release()

    message_queue_lock.acquire(True)
    while not wake_up_nodes():
        if debug:
            print("there are messages left in the message queues; node {} is processing incoming messages...".format(node_number))
            print("\tnode {}'s message list: {}".format(node_number,str([str(message) for message in message_queues[node_number]])))
        if not node.receive(message_queues):
            if debug:
                print("node {} could not receive a message. blocking until it is woken...".format(node_number))
            # there are no more messages for this node (or some sort of failure on receiving a message); we want to yield to the next node
            message_queue_lock.release()
            node.event.clear()
            node.event.wait()
            message_queue_lock.acquire(True)
    print("message list is empty.")
    message_queue_lock.release()
    return



def main():
    print("starting simulation.")
    global message_queues
    global nodes

    node_workers = []
    for i in range(NODE_AMOUNT):
        node_worker = threading.Thread(target=handle_messages, args=(i, nodes[i]))
        node_worker.start()
        node_workers.append(node_worker)

    for worker in node_workers:
        worker.join()

    print("finished simulation.")
    print("counting number of nodes which delivered the correct message...")
    num_correct_delivery = 0
    num_messages_delivered = 0
    for node in nodes:
        if node.delivered.type != "DEFAULT" and node.delivered.content == CORRECT_MESSAGE:
            num_correct_delivery += 1
            num_messages_delivered += node.num_messages_sent
    print("number of nodes which delivered the correct message: {}".format(num_correct_delivery))
    print("total messages sent: {}".format(num_messages_delivered))


if __name__ == "__main__":
    main()