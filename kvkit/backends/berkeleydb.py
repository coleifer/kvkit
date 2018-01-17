# Requires bsddb3 package.
import bsddb3
from bsddb3.db import DBNotFoundError

from kvkit.backends.helpers import KVHelper


class BerkeleyDB(KVHelper, bsddb3._DBWithCursor):
    def __init__(self, filename, flag='c', mode=0o666, btflags=0,
                 cache_size=None, maxkeypage=None, minkeypage=None,
                 page_size=None, lorder=None):

        self.filename = filename
        flags = bsddb3._checkflag(flag, filename)
        env = bsddb3._openDBEnv(cache_size)
        db = bsddb3.db.DB(env)
        if page_size is not None:
            db.set_pagesize(page_size)
        if lorder is not None:
            db.set_lorder(lorder)
        db.set_flags(btflags)
        if minkeypage is not None:
            db.set_bt_minkey(minkeypage)
        if maxkeypage is not None:
            db.set_bt_maxkey(maxkeypage)
        db.open(self.filename, bsddb3.db.DB_BTREE, flags, mode)
        super(BerkeleyDB, self).__init__(db)

    def __getitem__(self, key):
        if isinstance(key, slice):
            if key.start > key.stop or key.step:
                return self.get_slice_rev(key.start, key.stop)
            else:
                return self.get_slice(key.start, key.stop)
        else:
            return super(BerkeleyDB, self).__getitem__(key)

    def get_slice(self, start, end):
        try:
            key, value = self.set_location(start)
        except DBNotFoundError:
            raise StopIteration
        else:
            if key > end:
                raise StopIteration
            yield key, value

        while True:
            try:
                key, value = self.next()
            except DBNotFoundError:
                raise StopIteration
            else:
                if key > end:
                    raise StopIteration
            yield key, value

    def get_slice_rev(self, start, end):
        if start is None or end is None:
            start, end = end, start

        if start is None:
            key, value = self.last()
        else:
            try:
                key, value = self.set_location(start)
            except DBNotFoundError:
                key, value = self.last()

        if start is None or key <= start:
            yield key, value

        while True:
            try:
                key, value = self.previous()
            except DBNotFoundError:
                raise StopIteration
            else:
                if key < end:
                    raise StopIteration
            yield key, value
