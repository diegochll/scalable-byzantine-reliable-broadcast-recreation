NODE_AMOUNT = 10
EPSILON = 0
EXPECTED_SAMPLE_SIZE = 10 # todo: figure out how to tune G according to NODE_AMOUNT (I think grows logarithmically with N??)
CORRECT_MESSAGE = "CORRECT_MESSAGE"
DEBUG = False
ECHO_SAMPLE_SIZE = 5
DELIVERY_THRESHOLD = 3
DELIVERY_SAMPLE_SIZE = 5
READY_SAMPLE_SIZE = 5
CONTAGION_THRESHOLD = 3

#C = number of correct nodes
#G = average gossip set size
#p = 1 - (1-G/N)^2
#murmur satisfies "epsilon_t totality" where epsilon_t \leq sum_{k=1}^{C/2} ({C \choose k}(1-p)^{k(C-k)})
#diameter of the correct gossip network limits to lim_{C \rightarrow \infty} D(C,G) = \frac{\log(C)}{\log(2 + 2f) + \log(G)}
# if we fix epsilon, then G must scale logarithmically with N I think??