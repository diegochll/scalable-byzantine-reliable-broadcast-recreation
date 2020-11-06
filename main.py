from config import NODE_AMOUNT
from node import Node
import threading

message_queues = [[] for i in range(NODE_AMOUNT)]
nodes = [Node() for i in range(NODE_AMOUNT)]




def handle_messages(node_number, node, message_queues):
    print("I am : " + str(node_number) + " this is my node object :" + str(node) + "and this is my message queue : " + str(message_queues[node_number]) )


for i in range(NODE_AMOUNT):
    node_worker = threading.Thread(target=handle_messages, args=(i, nodes[i], message_queues))
    node_worker.start()


