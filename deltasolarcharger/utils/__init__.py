import logging.handlers
import logging


def log_worker_configurer(log_queue):
    h = logging.handlers.QueueHandler(log_queue)  # Just the one handler needed
    root = logging.getLogger()
    root.addHandler(h)
    # send all messages, for demo; no other level or filter logic applied.
    root.setLevel(logging.DEBUG)


def log(*args, level=logging.INFO, print_out=False):
    logger = logging.getLogger()
    final_string = ""
    for item in args:
        final_string += str(item) + ' '

    if print_out:
        print(args)

    logger.log(level, final_string)
