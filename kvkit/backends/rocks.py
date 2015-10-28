# Requires pyrocksdb.
from contextlib import contextmanager
import struct

# See http://pyrocksdb.readthedocs.org/en/latest/tutorial/index.html
import rocksdb

from kvkit.backends.helpers import KVHelper


class RocksDB(KVHelper):
    def __init__(self, filename, *args, **kwargs):
        self.filename = filename
        kwargs.setdefault('create_if_missing', True)
        options = rocksdb.Options(**kwargs)
        self.db = rocksdb.DB(filename, options)
        self._closed = False

    def __getitem__(self, key):
        if isinstance(key, slice):
            if key.start > key.stop or key.step:
                return self.get_slice_rev(key.start, key.stop)
            else:
                return self.get_slice(key.start, key.stop)
        elif isinstance(key, (list, tuple)):
            return self.db.multi_get(key)
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
        batch = rocksdb.WriteBatch()
        if _data:
            kwargs.update(_data)
        for key, value in kwargs.iteritems():
            batch.put(key, value)
        self.db.write(batch)

    def keys(self):
        iterator = self.db.iterkeys()
        iterator.seek_to_first()
        for key in iterator:
            yield key

    def values(self):
        iterator = self.db.itervalues()
        iterator.seek_to_first()
        for value in iterator:
            yield value

    def items(self):
        iterator = self.db.iteritems()
        iterator.seek_to_first()
        for item in iterator:
            yield item

    def get_slice(self, start, end):
        iterator = self.db.iteritems()
        if start is None:
            iterator.seek_to_first()
        else:
            iterator.seek(start)

        while True:
            key, value = iterator.next()
            if key > end:
                raise StopIteration
            yield key, value

    def get_slice_rev(self, start, end):
        iterator = reversed(self.db.iteritems())
        if start is None:
            iterator.seek_to_last()
        else:
            iterator.seek(start)

        try:
            key, value = iterator.next()
        except StopIteration:
            iterator.seek_to_last()
            key, value = iterator.next()

        if key <= start or start is None:
            yield key, value

        while True:
            key, value = iterator.next()
            if key < end:
                raise StopIteration
            yield key, value
