from queue import Queue
# from queue import deque
from collections import deque 

class Stream:
    """
    Represents a generic stream of objects.
    """
    def __init__(self):
        self._stream = deque()

    def __next__(self):
        if not self._stream:
            raise StopIteration()
        next_item = self._stream.popleft()  # Blocking get
        if next_item is None:
            raise StopIteration()
        return next_item

    def __iter__(self):
        return self

    def add_item(self, item: object):
        self._stream.append(item)

    def close(self):
        self._stream.append(None)

    def duplicate(self):
        ret = Stream()
        # ret._stream.queue = self._stream.queue.copy()
        ret._stream = self._stream.copy()
        return ret

    def get_item(self):
        return self.__next__()

    def count(self):
        return self._stream.qsize()

    def first(self):
        # return self._stream.queue[0]
        return self._stream[0]

    def last(self):
        # x = self._stream.queue[-1]
        # if x is None:  # if stream is closed last is None. We need the one before None.
        #     x = self._stream.queue[-2]
        # return x
        x = self._stream[-1]
        if x is None:  # if stream is closed last is None. We need the one before None.
            x = self._stream[-2]
        return x

class InputStream(Stream):
    """
    A stream receiving its items from some external source.
    """
    def add_item(self, item: object):
        raise Exception("Unsupported operation")


class OutputStream(Stream):
    """
    A stream sending its items to some external sink.
    """
    def get_item(self):
        raise Exception("Unsupported operation")

    def first(self):
        raise Exception("Unsupported operation")

    def last(self):
        raise Exception("Unsupported operation")
