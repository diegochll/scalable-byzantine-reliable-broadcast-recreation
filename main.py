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
import numpy as np
from collections import deque

glob_timeout = False


class Experiment:
    def __init__(self,node_amount, byzantine_ratio, expected_sample_size, correct_message, echo_sample_size,echo_threshold, delivery_threshold, delivery_sample_size, contagion_threshold, ready_sample_size):
        self.node_message_lists = [queue.Queue() for i in range(node_amount)]
        self.node_message_lists_lock = threading.Lock()
        self.nodes = []
        self.id_map = {}
        for i in range(node_amount):
            node = Node(i, expected_sample_size= expected_sample_size, num_nodes = node_amount, node_message_lists= self.node_message_lists,echo_sample_size= echo_sample_size, echo_threshold=echo_threshold, ready_sample_size= ready_sample_size,delivery_sample_size= delivery_sample_size,delivery_threshold= delivery_threshold,contagion_threshold= contagion_threshold, byzantine_ratio= byzantine_ratio)
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
        self.time_of_first_delivery = 0
        self.latencies = []
        self.avg_latency = 0
        self.timed_out = False
        global glob_timeout
        glob_timeout = False
        self.timed_out_timer = threading.Timer(node_amount**2,self.time_out)#2 minute time out
        self.num_failed = 0
        for n in self.nodes:
            if n.crashed:
                self.num_failed += 1

    #todo: implement malicious node logic
    #todo: implement diameter calculations
    #todo: implement latency calculations
    #todo: implement message signatures + verification logic

    def check_delivered(self):
        flag = True
        while flag:
            for node in self.nodes:
                if node.prb_delivered_message is not None and node.node_id != 0:
                    self.time_of_first_delivery = time.time()
                    flag = False
                    break

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
        # self.wake_all_nodes()
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
        global glob_timeout
        # the first node is the originator and needs to send out the message to decide upon
        if node_number == 0:
            debug_print("I am node 0 and the originator; broadcasting gossip with correct value...")
            node.is_originator = True
            self.acquire_node_message_lock()
            node.prb_broadcast(self.correct_message,self.node_message_lists)
            self.release_node_message_lock()

        self.acquire_node_message_lock()
        while not self.wake_up_nodes() and not self.timed_out and not glob_timeout:
            debug_print("there are messages left in the message queues; node {} is processing incoming messages...".format(node_number))
            debug_print("\tnode {}'s message list: {}".format(node_number,stringify_queue(self.node_message_lists[node_number])))
            if not node.receive(self.node_message_lists):
                debug_print("node {} could not receive a message. blocking until it is woken...".format(node_number))
                # there are no more messages for this node (or some sort of failure on receiving a message); we want to yield to the next node
                self.release_node_message_lock()
                node.event.clear()
                node.event.wait()
                self.acquire_node_message_lock()
        self.wake_all_nodes()
        debug_print("message list is empty or timeout occurred.")
        self.release_node_message_lock()
        return

    def time_out(self):
        global glob_timeout
        self.timed_out = True
        glob_timeout = True

    def run(self):
        global glob_timeout
        debug_print("starting simulation.")
        node_workers = []
        for i in range(self.num_nodes):
            node_worker = threading.Thread(target=self.handle_messages, args=(i, self.nodes[i]))
            node_worker.setDaemon(True)
            node_worker.start()
            node_workers.append(node_worker)
        self.timed_out_timer.start()
        latency_checker = threading.Thread(target = self.check_delivered)
        latency_checker.setDaemon(True)
        latency_checker.start()
        start = time.perf_counter()
        for worker in node_workers:
            worker.join()
        end = time.perf_counter()
        for node in self.nodes:
            if not node.crashed:
                self.latencies.append(node.latency)
        self.timed_out_timer.cancel()
        self.time_taken = end-start
        self.avg_latency = np.average(self.latencies)
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


def ceil_sqrt(puts):
    return int(math.ceil(math.sqrt(puts)))


def ceil_min_log(puts):
    return int(np.ceil(min(np.divide(puts,2),np.multiply(5,math.log2(puts)))))


def ceil_log(puts):
    return int(np.ceil(math.log2(puts)))


def ceil_div2(puts):
    return int(math.ceil(puts/2))


def save_data(xs,ys,filename,labels = []):
    file = open(filename, "a+")
    if not len(labels) == 0:
        file.write(",".join(labels))
        file.write("\n")
    file.write(",".join([str(x) for x in xs]))
    file.write("\n")
    file.write(",".join([str(d) for d in ys]))
    file.write("\n")
    file.close()


def run_scale_n():
    random.seed(int(time.time()))
    node_amounts = [10, 20, 50, 75, 100, 150, 200, 250]
    fs = [0,.05,.1,.25,.33]
    pltlabels = ['k','g','b','r','c']
    times = []
    messages = []
    num_nodes_correct = []
    latencies = []
    num_connected = []
    for f in fs:
        times.append([])
        messages.append([])
        latencies.append([])
        num_nodes_correct.append([])
        num_connected.append([])
    # sample_size_func = ceil_sqrt

    # threshold functions are applied on the result of applying the sample size function on the number of nodes
    expected_sample_size_func = ceil_min_log
    echo_sample_size_func = ceil_min_log
    echo_threshold_func = ceil_div2
    deliver_thres_func = ceil_sqrt
    deliver_sample_size_func = ceil_min_log
    ready_thres_func = ceil_div2
    ready_sample_size_func = ceil_min_log

    for i, node_amount in enumerate(node_amounts):

        for j,f in enumerate(fs):
        # e = Experiment(node_amount=node_amount,expected_sample_size=sample_size_func(node_amount),correct_message="CORRECT_MESSAGE",echo_sample_size=sample_size_func(node_amount),delivery_threshold=math.ceil(math.log2(math.log2(node_amount))),delivery_sample_size=sample_size_func(node_amount),ready_sample_size=sample_size_func(node_amount),contagion_threshold=math.ceil(math.log2(math.log2(node_amount))))
            e = Experiment(byzantine_ratio=f, node_amount=node_amount,
                       expected_sample_size=expected_sample_size_func(node_amount), correct_message="CORRECT_MESSAGE",
                       echo_sample_size=echo_sample_size_func(node_amount),
                       echo_threshold=echo_threshold_func(echo_sample_size_func(node_amount)),
                       delivery_threshold=deliver_thres_func(deliver_sample_size_func(node_amount)),
                       delivery_sample_size=deliver_sample_size_func(node_amount),
                       ready_sample_size=ready_sample_size_func(node_amount),
                       contagion_threshold=ready_thres_func(ready_sample_size_func(node_amount)))

            print(
            "\nrunning experiment with num nodes: {}; num failed: {}; num connected: {} \n\tsample sizes: \texpected gossip: {}; \techo: {}; \tdelivery: {}; \tready: {}\n\tthresholds: \t\techo: {}; \tdelivery: {}; \tready: {}".format(
                node_amount, e.num_failed, e.check_connected(), expected_sample_size_func(node_amount),
                echo_sample_size_func(node_amount),
                deliver_sample_size_func(node_amount), ready_sample_size_func(node_amount),
                echo_threshold_func(echo_sample_size_func(node_amount)),
                deliver_thres_func(deliver_sample_size_func(node_amount)),
                ready_thres_func(ready_sample_size_func(node_amount))), flush=True)
            e.run()


            if e.timed_out:
                times.append(-1)
                messages.append(-1)
                num_nodes_correct.append(-1)
                latencies.append(-1)
                num_connected.append(-1)
                print(
                    "{}: TIMED OUT. num nodes: {}; \n\tsample sizes: \texpected gossip: {}; \techo: {}; \tdelivery: {}; \tready: {}\n\tthresholds: \t\techo: {}; \tdelivery: {}; \tready: {}".format(
                        time.time(), node_amount, expected_sample_size_func(node_amount),
                        echo_sample_size_func(node_amount), deliver_sample_size_func(node_amount),
                        ready_sample_size_func(node_amount), echo_threshold_func(echo_sample_size_func(node_amount)),
                        deliver_thres_func(deliver_sample_size_func(node_amount)),
                        ready_thres_func(ready_sample_size_func(node_amount))), flush=True)

            else:
                times[j].append(e.time_taken)
                messages[j].append(e.num_messages_sent)
                num_nodes_correct[j].append(e.num_correct_delivery)
                latencies[j].append(e.avg_latency)
                num_connected[j].append(e.check_connected())

                fn = "output/times_{}-nodes_f-0".format(node_amount)
                labels = ["number of nodes", "times"]
                save_data(xs = node_amounts, ys = times[-1], labels = labels, filename = fn)
                fn = "output/num-messages_{}-nodes_f-0".format(node_amount)
                labels = ["number of nodes", "number of messages sent"]
                save_data(xs = node_amounts, ys = messages[-1], labels = labels, filename = fn)

                fn = "output/latencies_{}-nodes_f-0".format(node_amount)
                labels = ["number of nodes", "latencies"]
                save_data(xs = node_amounts, ys = latencies[-1], labels = labels, filename = fn)

                print(
                    "Ran experiment. \nStats: \n\ttime: {}; messages sent: {}; num nodes correct: {}; latency: {}; number of nodes connected: {}".format(
                         times[j][-1], messages[j][-1], num_nodes_correct[j][-1], latencies[j][-1], num_connected[j][-1]))

        for i, arr in enumerate(times):
            plt.plot(node_amounts[:len(arr)], arr, pltlabels[i],label="f={}".format(fs[i]))
        # plt.plot(node_amounts, times_byz,'b')
        plt.title("time for consensus")
        plt.legend(["f=0", "f=.05", "f=.1", "f=.25", "f=.33"])
        plt.ylabel("time (s)")
        plt.xlabel("number of nodes")
        plt.savefig("times_against_nodes_n{}.png".format(node_amount))
        plt.show()

        for i, arr in enumerate(messages):
            plt.plot(node_amounts[:len(arr)], arr, pltlabels[i],label="f={}".format(fs[i]))
        # plt.plot(node_amounts, np.divide(messages_byz, 10 ** 5),'b')
        plt.title("total number of messages sent")
        plt.legend(["f=0", "f=.05", "f=.1", "f=.25", "f=.33"])
        plt.xlabel("number of nodes")
        plt.ylabel("messages sent")
        plt.savefig("messages_sent_against_nodes_n{}.png".format(node_amount))
        plt.show()

        for i, arr in enumerate(latencies):
            plt.plot(node_amounts[:len(arr)], arr, pltlabels[i],label="f={}".format(fs[i]))
        # plt.plot(node_amounts, latencies_byz,'b')
        plt.title("average latency")
        plt.legend(["f=0", "f=.05", "f=.1", "f=.25", "f=.33"])
        plt.xlabel("number of nodes")
        plt.ylabel("latencies")
        plt.savefig("latencies_against_nodes_n{}.png".format(node_amount))
        plt.show()

    for i,arr in enumerate(times):
        plt.plot(node_amounts, arr, pltlabels[i])
    # plt.plot(node_amounts, times_byz,'b')
    plt.title("time for consensus")
    plt.legend(["f=0","f=.05","f=.1","f=.25","f=.33"])
    plt.ylabel("time (s)")
    plt.xlabel("number of nodes")
    plt.savefig("times_against_nodes.png")
    plt.show()

    for i,arr in enumerate(messages):
        plt.plot(node_amounts, arr, pltlabels[i])
    # plt.plot(node_amounts, np.divide(messages_byz, 10 ** 5),'b')
    plt.title("total number of messages sent")
    plt.legend(["f=0","f=.05","f=.1","f=.25","f=.33"])
    plt.xlabel("number of nodes")
    plt.ylabel("messages sent")
    plt.savefig("messages_sent_against_nodes.png")
    plt.show()

    for i, arr in enumerate(latencies):
        plt.plot(node_amounts, arr, pltlabels[i])
    # plt.plot(node_amounts, latencies_byz,'b')
    plt.title("average latency")
    plt.legend(["f=0","f=.05","f=.1","f=.25","f=.33"])
    plt.xlabel("number of nodes")
    plt.ylabel("latencies")
    plt.savefig("latencies_against_nodes.png")
    plt.show()


def run_scale_sample_sizes():
    fig, (ax_times, ax_lat, ax_mess) = plt.subplots(3, 1)
    fig.tight_layout(pad=3.0)
    ax_times.set_title("times against sample size (n = 50)")
    ax_times.set_xlabel("sample size")
    ax_times.set_ylabel("time spent")
    ax_lat.set_title("latencies against sample size (n = 50)")
    ax_lat.set_xlabel("sample size")
    ax_lat.set_ylabel("latency")
    ax_mess.set_title("number of messages sent against sample size (n = 50)")
    ax_mess.set_xlabel("sample size")
    ax_mess.set_ylabel("messages sent")
    node_amount = 50
    sample_sizes = [2, ceil_log(node_amount), ceil_sqrt(node_amount), ceil_div2(node_amount)]
    num_runs = 10
    failure_rates = [0, .05, .125, .25, .33]
    formats = ['k*', 'b*', 'r*', 'g*', 'c*']
    times = np.zeros((num_runs, len(sample_sizes)))
    num_messages = np.zeros((num_runs, len(sample_sizes)))
    latencies = np.zeros((num_runs, len(sample_sizes)))
    for j, failure_rate in enumerate(failure_rates):
        for i in range(num_runs):
            for indx, sample_size in enumerate(sample_sizes):
                print("starting run {} of experiment with sample size: {}, failure rate: {}".format(i, sample_size,
                                                                                                    failure_rate))

                e = Experiment(node_amount=node_amount, byzantine_ratio=failure_rate, expected_sample_size=sample_size,
                               correct_message="CORRECT",
                               echo_sample_size=sample_size, echo_threshold=ceil_div2(sample_size),
                               delivery_sample_size=sample_size,
                               delivery_threshold=ceil_log(sample_size), contagion_threshold=ceil_div2(sample_size),
                               ready_sample_size=sample_size)
                e.run()
                if e.timed_out:
                    print("experiment timed out")
                else:
                    times[i, indx] = e.time_taken
                    if e.avg_latency >= 0:
                        latencies[i, indx] = e.avg_latency
                    else:
                        print("negative latency...")
                        latencies[i, indx] = 0
                    num_messages[i, indx] = e.num_messages_sent

        # for failure_rate in failure_rates:
        save_data(xs=sample_sizes, ys=[np.average(times[:, col]) for col in range(times.shape[1])], labels=["times"],
                  filename=
                  "output/f{}_times_varying_sample_size".format(int(failure_rate * 100)))
        save_data(xs=sample_sizes, ys=[np.average(latencies[:, col]) for col in range(latencies.shape[1])],
                  filename="output/f{}_latencies_varying_sample_size".format(int(failure_rate * 100)),
                  labels=["latencies"])
        save_data(xs=sample_sizes, ys=[np.average(num_messages[:, col]) for col in range(num_messages.shape[1])],
                  labels=["num_messages"],
                  filename="output/f{}_num-messages_varying_sample_size".format(int(failure_rate * 100)))

        ax_times.plot(sample_sizes, [np.average(times[:, col]) for col in range(times.shape[1])], formats[j])
        ax_lat.plot(sample_sizes, [np.average(latencies[:, col]) for col in range(latencies.shape[1])], formats[j])
        ax_mess.plot(sample_sizes, [np.average(num_messages[:, col]) for col in range(num_messages.shape[1])],
                     formats[j])

        plt.savefig("n50_varying_sample_size_f{}".format(str(int(failure_rate * 100))))

    ax_times.legend(["f=0", "f=.05", "f=.125", "f=.25", "f=.33"], loc="upper left", fontsize="x-small")
    # ax_lat.legend(["f=0", "f = .01", "f = .05", "f = .1", "f = .15", "f = .25", "f = .33", "f =.5"], loc = "upper left",fontsize = "x-small")
    # ax_mess.legend(["f=0", "f = .01", "f = .05", "f = .1", "f = .15", "f = .25", "f = .33", "f =.5"], loc = "upper left",fontsize = "x-small")
    plt.savefig("n50_varying_sample_size_all-fs")
    plt.show()


if __name__ == "__main__":
    run_scale_n()
    #run_scale_sample_sizes()

