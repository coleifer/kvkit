import contextlib
import struct


def clean_key_slice(key):
    start = key.start
    stop = key.stop
    reverse = key.step
    first = start is None
    last = stop is None
    one_empty = (first and not last) or (last and not first)
    none_empty = not first and not last
    if reverse:
        if one_empty:
            start, stop = stop, start
        if none_empty and (start < stop):
            start, stop = stop, start
    if none_empty and start > stop:
        reverse = True
    return start, stop, reverse


class KVHelper(object):
    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def incr(self, key, amount=1):
        try:
            value = self[key]
        except KeyError:
            value = amount
        else:
            value = struct.unpack('>q', value)[0] + amount
        self[key] = struct.pack('>q', value)
        return value

    def decr(self, key, amount=1):
        return self.incr(key, amount * -1)

    @contextlib.contextmanager
    def transaction(self):
        yield
