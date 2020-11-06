from config import NODE_AMOUNT
from node import Node
import threading

EXPECTED_SAMPLE_SIZE = 5 # todo: figure out how to tune G according to NODE_AMOUNT

message_queues = [[] for i in range(NODE_AMOUNT)]
nodes = [Node(i,EXPECTED_SAMPLE_SIZE,NODE_AMOUNT,message_queues) for i in range(NODE_AMOUNT)]

messages_delivered = []

nodes[0].is_originator = True


def handle_messages(node_number, node, message_queues):
    print("I am : " + str(node_number) + " this is my node object :" + str(node) + "and this is my message queue : " + str(message_queues[node_number]) )


def main():
    for i in range(NODE_AMOUNT):
        node_worker = threading.Thread(target=handle_messages, args=(i, nodes[i], message_queues))
        node_worker.start()


