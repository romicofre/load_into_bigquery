import os

import psutil


def memory_usage():
    """
    :return: memory usage in MB
    """
    mem = psutil.Process(os.getpid()).memory_info().rss
    return mem / 1024 ** 2
