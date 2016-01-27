import struct

from forestdb import ForestDB as _ForestDB


class ForestDB(_ForestDB):
    def __init__(self, filename):
        super(ForestDB, self).__init__(filename)
        self._kv = self.kv('default')

    def __getitem__(self, key):
        return self._kv[key]

    def __setitem__(self, key, value):
        self._kv[key] = value

    def __delitem__(self, key):
        del self._kv[key]

    def __contains__(self, key):
        return key in self._kv

    def update(self, _data_dict=None, **data):
        return self._kv.update(_data_dict, **data)

    def __len__(self):
        return len(self._kv)

    def cursor(self, *args, **kwargs):
        return self._kv.cursor(*args, **kwargs)

    def keys(self):
        return self._kv.keys()

    def values(self):
        return self._kv.values()

    def __iter__(self):
        return iter(self._kv)

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
