# Requries plyvel.
from contextlib import contextmanager
import struct

import plyvel

from kvkit.backends.helpers import clean_key_slice
from kvkit.backends.helpers import KVHelper


class LevelDB(KVHelper):
    def __init__(self, filename, *args, **kwargs):
        self.filename = filename
        kwargs.setdefault('create_if_missing', True)
        self.db = plyvel.DB(filename, *args, **kwargs)
        self._closed = False

    def __getitem__(self, key):
        if isinstance(key, slice):
            start, stop, reverse = clean_key_slice(key)
            if reverse:
                # LevelDB uses slightly different meaning for start/stop when
                # reverse than kyotocabinet.
                start, stop = stop, start
            return self.db.iterator(
                start=start,
                stop=stop,
                include_start=True,
                include_stop=True,
                reverse=reverse)
        elif isinstance(key, (list, tuple)):
            pass
        else:
            res = self.db.get(key)
            if res is None:
                raise KeyError(key)
            return res

    def open(self):
        pass

    def close(self):
        if self._closed:
            return False

        self.db.close()
        self._closed = True
        return True

    def __setitem__(self, key, value):
        self.db.put(key, value)

    def __delitem__(self, key):
        self.db.delete(key)

    def update(self, _data=None, **kwargs):
        batch = self.db.write_batch()
        if _data:
            kwargs.update(_data)
        for key, value in kwargs.iteritems():
            batch.put(key, value)
        batch.write()

    def keys(self):
        return (key for key in self.db.iterator(include_value=False))

    def values(self):
        return (value for value in self.db.iterator(include_key=False))

    def items(self):
        return (item for item in self.db)
