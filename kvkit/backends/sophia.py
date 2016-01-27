import struct

from sophy import SimpleDatabase


class Sophia(SimpleDatabase):
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
