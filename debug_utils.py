from config import DEBUG

def debug(msg):
    if DEBUG:
        print(msg)

def stringify_queue(queue):
    #Synchronized queue can't be iterated through, making it a list works and is atomic. Since it is in debug though it is locked
    if DEBUG:
        stringified_queue = str([str(message) for message in list(queue.queue)]) 
        return stringified_queue
    return ""