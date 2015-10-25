# kvkit

High-level Python toolkit for ordered key/value stores.

Supports:

* [BerkeleyDB](http://www.oracle.com/technetwork/database/database-technologies/berkeleydb/downloads/index.html) via [bsddb3](https://www.jcea.es/programacion/pybsddb_doc/).
* [KyotoCabinet](http://fallabs.com/kyotocabinet/) via [Python 2.x bindings](http://fallabs.com/kyotocabinet/pythonlegacydoc/).
* [LevelDB](http://leveldb.org/) via [plyvel](https://plyvel.readthedocs.org/en/latest/).
* [RocksDB](http://rocksdb.org/) via [pyrocksdb](http://pyrocksdb.readthedocs.org/en/v0.4/)
* [Sqlite4 LSM DB](https://www.sqlite.org/src4/doc/trunk/www/lsmusr.wiki) via [python-lsm-db](http://lsm-db.readthedocs.org/en/latest/)

Right now KyotoCabinet is the most well-supported database, but the SQLite4 LSM is also pretty robust. The other databases implement the minimal slicing APIs to enable the Model/Secondary Indexing APIs to work.

This project should be considered **experimental**.

### Examples

`kvkit` provides indexing and slicing operations. Slices are inclusive of both endpoints.

```pycon

>>> from kvkit import CacheTreeDB  # KyotoCabinet in-memory B-tree
>>> db = CacheTreeDB()
>>> # Populate some data.
>>> for key in ['aa', 'aa1', 'aa2', 'bb', 'cc', 'dd', 'ee']:
...     db[key] = key
...
>>> list(db['aa':'cc'])
[('aa', 'aa'), ('aa1', 'aa1'), ('aa2', 'aa2'), ('bb', 'bb'), ('cc', 'cc')]

>>> list(db['aa0':'cc2'])  # Example where start & end do not exist.
[('aa1', 'aa1'), ('aa2', 'aa2'), ('bb', 'bb'), ('cc', 'cc')]
```

`kvkit` also provides a lightweight structured data abstraction, with secondary indexing.

```python

from datetime import date
from kvkit import *

db = PrototypeTreeDB()  # KyotoCabinet in-memory red/black tree.

class User(Model):
    username = Field(index=True)
    dob = DateField(index=True)

    class Meta:
        database = db

User.create(username='huey', dob=date(2008, 5, 1))
User.create(username='mickey', dob=date(2005, 8, 1))
User.create(username='zaizee', dob=date(2009, 1, 1))

for user in User.query(User.dob < date(2008, 12, 31)):
    print user.username

# Prints "huey" and "mickey"

user = User.query(User.username == 'mickey')[0]
print user.username

# Prints "mickey".
```
