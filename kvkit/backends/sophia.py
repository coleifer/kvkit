import os

from sophy import Sophia as _Sophia

from kvkit.backends.helpers import KVHelper


class Sophia(KVHelper):
    def __init__(self, filename, index_type='string', **kwargs):
        self.filename, self.db_name = os.path.split(filename)
        self.index_type = index_type
        db_defs = [(self.db_name, self.index_type)]
        self._env = _Sophia(self.filename, databases=db_defs, auto_open=False,
                            **kwargs)
        self.open()

    def open(self):
        ret = self._env.open()
        self._db = self._env[self.db_name]
        return self

    def close(self):
        return self._env.close()

    def __getitem__(self, key):
        return self._db[key]

    def __setitem__(self, key, value):
        self._db[key] = value

    def __delitem__(self, key):
        del self._db[key]

    def __contains__(self, key):
        return key in self._db

    def __len__(self):
        return len(self._db)

    def update(self, _data=None, **k):
        self._db.update(_data=_data, **k)

    def transaction(self):
        return self._db.transaction()

    def view(self, name):
        return self._db.view(name)

    def keys(self):
        return self._db.keys()

    def values(self):
        return self._db.values()

    def items(self):
        return self._db.items()

    def __iter__(self):
        return iter(self._db)
