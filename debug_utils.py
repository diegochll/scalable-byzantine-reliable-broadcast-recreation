from config import DEBUG


def debug_print(msg):
    if DEBUG:
        print(msg)


def stringify_queue(queue):
    # Synchronized queue can't be iterated through, making it a list works and is atomic. Since it is in debug though it is locked
    if DEBUG:
        stringified_queue = str([str(message) for message in list(queue.queue)])
        return stringified_queue
    return ""


def print_queue_status(sender_id, sent_message, recipient_id, recipient_queue, sending=True):
    stringified_messages = stringify_queue(recipient_queue)
    action = "appending" if sending else "appended"
    debug_print(
        "node {} {} {} to node {}'s message queue, {}".format(sender_id, str(sent_message), action, recipient_id,
                                                              stringified_messages))
