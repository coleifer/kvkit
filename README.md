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

### Features

* Store structured data models.
* Secondary indexes and arbitrarily complex querying.
* Graph database (Hexastore) with search pipeline.
* High-level slicing APIs.

### Models

`kvkit` provides a lightweight structured data model API. Individual fields on the model can be optionally typed, and also support secondary indexes.

Field types:

* `Field()`: simplest field type, treated as raw bytes.
* `DateTimeField()`: store Python `datetime` objects.
* `DateField()`: store Python `date` objects.
* `LongField()`: store Python `int` and `long`. Values are encoded as an 8 byte `long long`, big-endian.
* `FloatField()`: store Python `float`. Values are encoded as an 8 byte double-precision float, big-endian.

A `Model` is composed of one or more fields, in addition to a required `id` field which stores an automatically-generated integer ID.

`Model` classes are defined declaratively, a-la many popular Python ORMs:

```python

# KyotoCabinet on-disk B-tree.
db = TreeDB('address_book.kct')

# Create a base model-class pointing at our db.
class BaseModel(Model):
    class Meta:
        database = db

class Contact(BaseModel):
    last_name = Field(index=True)
    first_name = Field(index=True)


class PhoneNumber(BaseModel):
    contact_id = LongField(index=True)
    phone_number = Field()

    def get_contact(self):
        return Contact.load(self.contact_id)


class Address(BaseModel):
    contact_id = LongField(index=True)
    street = Field()
    city = Field(index=True)
    state = Field(index=True)
    postal_code = Field()

    def get_contact(self):
        return Contact.load(self.contact_id)
```

To create a new contact and add a phone number for them, we might write:

```python

huey = Contact.create(
    last_name='Leifer',
    first_name='Huey',
    dob=datetime.date(2011, 5, 1))

phone = PhoneNumber.create(
    contact_id=huey.id,
    phone_number='555-1234')
```

Let's say we need to look up Huey's phone number(s). We might write:

```python
huey = Contact.get(Contact.first_name == 'Huey')
phones = PhoneNumber.query(PhoneNumber.contact_id == huey.id)
for phone in phones:
    print phone.phone_number
```

If there were more than one person named "Huey" in our database, we could be more specific by specifying additional query clauses:

```python

huey_leifer = Contact.get(
    (Contact.first_name == 'Huey') &
    (Contact.last_name == 'Leifer'))
```

To query all contacts whose last name begins with "Le" we could write:

```python

Contact.query(Contact.last_name.startswith('Le'))
```

If we wanted to express a range, such as "Le" -> "Mo", we could write:

```python

Contact.query(
    (Contact.last_name >= 'Le') &
    (Contact.last_name <= 'Mo'))
```

Fields can be queried using the following operations:

* `==` for equality
* `<` and `<=`
* `>` and `>=`
* `!=` for inequality
* `.startswith()` for prefix search

Multiple clauses can be combined using set operations:

* `&` for AND (intersection)
* `|` for OR (union)

### Graph database (Hexastore)

The graph database is based on an idea described in the Redis [secondary indexing documentation](http://redis.io/topics/indexes#representing-and-querying-graphs-using-an-hexastore). The idea is that the database will store triples of `subject`, `predicate` and `object`. These can be any application-specific values. For example, I might want to store my friends and some information about them:

```python

db = CacheTreeDB()  # KyotoCabinet in-memory B-tree
graph = Hexastore(db)

data = (
    ('charlie', 'friends', 'huey'),
    ('charlie', 'friends', 'mickey'),
    ('charlie', 'friends', 'zaizee'),
    ('huey', 'friends', 'charlie'),
    ('huey', 'friends', 'zaizee'),
    ('zaizee', 'friends', 'huey'),
    ('charlie', 'lives', 'KS'),
    ('huey', 'lives', 'KS'),
    ('mickey', 'lives', 'KS'),
    ('zaizee', 'lives', 'MO'),
)
graph.store_many(data)
```

To do a simple query asking who my friends are, I can write:

```python

for result in graph.query(s='charlie', p='friends'):
    print result['o']

# prints huey, mickey, zaizee
```

I can also ask for other things, like all the people who live in Kansas:

```python

for result in graph.query(p='lives', o='KS'):
    print result['s']

# prints charlie, huey, mickey
```

Things get especially interesting when you construct a pipeline using variables. Let's get all of my friends who live in Kansas:

```python

X = graph.v.X  # Create a variable reference.
results = graph.search(
    ('charlie', 'friends', X),
    (X, 'lives', 'KS'))
print results['X']

# prints set(['huey', 'mickey'])
```

In this query, we will use two variables, and answer the question "Who has friends who live in Missouri?"

```python

X = graph.v.X
Y = graph.v.Y
results = graph.search(
    (X, 'lives', 'MO'),
    (Y, 'friends', X))
print results['Y']

# prints set(['charlie', 'huey'])
# charlie and huey are friends with zaizee, who lives in MO.
```

### Unified Slicing API

`kvkit` provides unified indexing and slicing APIs. Slices obey the following rules:

* Inclusive of both endpoints.
* If the start key does not exist, the next-highest key will be used, if one exists.
* If the end key does not exist, the next-lowest key will be used, if one exists.
* Supports efficient iteration forwards or backwards.

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

In addition to slicing, all databases implement the following dictionary-like methods:

* `update()`
* `keys()`
* `values()`
* `items()`
* `__setitem__` and `__delitem__`
* `__iter__`

All databases also implement:

* `incr()`
* `decr()`
* `open()`
* `close()`
