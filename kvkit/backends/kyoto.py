# Requires kyotocabinet Python legacy bindings.
import operator
import os
import struct

import kyotocabinet as kc

from kvkit.exceptions import DatabaseError
from kvkit.backends.helpers import clean_key_slice


# Generic modes.
EXCEPTIONAL = kc.DB.GEXCEPTIONAL
CONCURRENT = kc.DB.GCONCURRENT

# Open options.
READER = kc.DB.OREADER
WRITER = kc.DB.OWRITER
CREATE = kc.DB.OCREATE
TRUNCATE = kc.DB.OTRUNCATE
AUTOCOMMIT = kc.DB.OAUTOTRAN
AUTOSYNC = kc.DB.OAUTOSYNC
NOLOCK = kc.DB.ONOLOCK  # Open without locking.
TRYLOCK = kc.DB.OTRYLOCK  # Open and lock without blocking.
NOREPAIR = kc.DB.ONOREPAIR

# Merge options.
MERGE_OVERWRITE = kc.DB.MSET  # Overwrite existing values.
MERGE_PRESERVE = kc.DB.MADD  # Keep existing values.
MERGE_REPLACE = kc.DB.MREPLACE  # Modify existing records only.
MERGE_APPEND = kc.DB.MAPPEND  # Append new values.

# Special filenames
DB_PROTOTYPE_HASH = '-'
DB_PROTOTYPE_TREE = '+'
DB_STASH = ':'
DB_CACHE_HASH = '*'
DB_CACHE_TREE = '%'

# Special extensions
DB_FILE_HASH = '.kch'
DB_FILE_TREE = '.kct'
DB_DIRECTORY_HASH = '.kcd'
DB_DIRECTORY_TREE = '.kcf'
DB_TEXT = '.kcx'

NOP = kc.Visitor.NOP

# Tuning parameters

# Log parameters are supported by all databases.
P_LOG = 'log'  # Path to log file.
P_LOGKINDS = 'logkinds'  # debug, info, warn or error.
P_LOGPX = 'logpx'  # prefix for each log message.

P_BUCKETS = 'bnum'  # Buckets in hash table.

P_CAPCNT = 'capcnt'  # Sets the capacity by record number.
P_CAPSIZ = 'capsiz'  # Sets the capacity by memory usage.

P_SIZE = 'psiz'  # page size.
P_PCCAP = 'pccap'  # tune page cache.

# Reduce memory at the expense of time. Linear means use linear linked-list
# for hash collisions, saves 6 bytes/record. Compression should only be used
# for values > 1KB.
P_OPTS = 'opts'  # s (small), l (linear), c (compress)

P_ZCOMP = 'zcomp'  # zlib, def (deflate), gz, lzo, lzma, arc
P_ZKEY = 'zkey'  # cipher key of compressor
P_RCOMP = 'rcomp'  # comp fn: lex, dec (decimal), lexdesc, decdesc
P_APOW = 'apow'  # alignment
P_FPOW = 'fpow'  # tune_fbp
P_MSIZ = 'msiz'  # tune map
P_DFUNIT = 'dfunit'  # tune defrag.

"""
All databases: log, logkinds, logpx
Stash: bnum
Cache hash: opts, bnum, zcomp, capcnt, capsiz, zkey
Cache tree: opts, bnum, zcomp, zkey, psiz, rcomp, pccap
File hash: apow, fpow, opts, bnum, msiz, dfunit, zcomp, zkey
File tree: apow, fpow, opts, bnum, msiz, dfunit, zcomp, zkey, psiz, rcomp,
           pccap
Dir hash: opts, zcomp, zkey.
Dir tree: opts, zcomp, zkey, psiz, rcomp, pccap.
Plain: n/a

StashDB
-------
bnum: default is ~1M. Should be 80% - 400% to total records. Collision
      chaining is linear linked list search.

CacheHashDB
-----------
bnum: default ~1M. Should be 50% - 400% of total records. Collision chaining
      is binary search.
opts: useful to reduce memory at expense of time effciency. Use compression
      if the key and value of each record is > 1KB.
cap_count and/or cap_size: used to keep memory usage constant by expiring
                           old records.

CacheTreeDB
-----------
Inherits all tuning options from the CacheHashDB, since each node of the btree
is serialized as a page-buffer and treated as a record in the cache hash db.

page size: default is 8192
page cache: default is 64MB
comparator: default is lexical ordering

HashDB
------

bnum: default ~1M. Suggested ratio is twice the total number of records, but
      can be anything from 100% - 400%.
apow: Power of the alignment of record size. Default=3, so the address of
      each record is aligned to a multiple of 8 (1<<3) bytes.
fpow: Power of the capacity of the free block pool. Default=10, rarely needs
      to be modified.
msiz: Size of internal memory-mapped region. Default is 64MB.
dfunit: Unit step number of auto-defragmentation. Auto-defrag is disabled by
        default.

apow, fpow, opts and bnum *must* be specified before a DB is opened and
cannot be changed after the fact.

TreeDB
------

Inherits tuning parameters from the HashDB.

page size: default is 8192
page cache: default is 64MB
comparator: default is lexical

The default alignment is 256 (1<<8) and the default bucket number is ~64K.
The bucket number should be calculated by the number of pages. Suggested
ratio of bucket number is 10% of the number of records.

page size must be specified before the DB is opened and cannot be changed.
"""


class Database(object):
    default_flags = kc.DB.OWRITER | kc.DB.OCREATE
    extension = None

    def __init__(self, filename, exceptional=False, concurrent=False,
                 open_database=True, **opts):
        config = 0
        if exceptional:
            config |= EXCEPTIONAL
        if concurrent:
            config |= CONCURRENT

        if opts:
            opt_str = '#'.join([
                '%s=%s' % (key, value) for key, value in opts.items()])
            filename = '#'.join((filename, opt_str))

        self.filename = filename
        self._config = config
        if self.extension and not self.filename.endswith(self.extension):
            self.filename = '%s%s' % (self.filename, self.extension)
        self.db = kc.DB(self._config)
        self._closed = True
        if open_database:
            self.open()

    def open(self, flags=None):
        if not self._closed:
            self.close()
        if flags is None:
            flags = self.default_flags
        if not self.db.open(self.filename, flags):
            raise DatabaseError(self.db.error())
        return True

    def close(self):
        if not self.db.close():
            raise DatabaseError(self.db.error())
        self._closed = True
        return True

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self.close()
        except DatabaseError:
            pass

    def __setitem__(self, key, value):
        self.db.set(key, value)

    def __getitem__(self, key):
        if isinstance(key, (list, tuple)):
            return self.db.get_bulk(key, True)
        elif isinstance(key, slice):
            start, stop, reverse = clean_key_slice(key)
            if reverse:
                return self.get_slice_rev(start, stop)
            else:
                return self.get_slice(start, stop)
        else:
            value = self.db.get(key)
            if value is None:
                raise KeyError(key)
            return value

    def __delitem__(self, key):
        if isinstance(key, (list, tuple)):
            self.db.remove_bulk(key, True)
        elif isinstance(key, slice):
            pass
        else:
            self.db.remove(key)

    def __contains__(self, key):
        return self.db.check(key) != -1

    def __len__(self):
        return self.db.count()

    def update(self, _data=None, **kwargs):
        """
        Update multiple records atomically. Returns the number of records
        updated, raising a `DatabaseError` on error.
        """
        if _data:
            if kwargs:
                _data.update(kwargs)
            ret = self.db.set_bulk(_data, True)
        else:
            ret = self.db.set_bulk(kwargs, True)
        if ret < 0:
            raise DatabaseError('Error updating records: %s' % self.db.error())
        return ret

    def pop(self, key=None):
        """
        Remove the first record, or the record specified by the given key,
        returning the value.
        """
        if key:
            ret = self.db.seize(key)
        else:
            ret = self.db.shift()

        if ret is None:
            raise KeyError(key)

        return ret

    def clear(self):
        """Remove all records, returning `True` on success."""
        return self.db.clear()

    def flush(self, hard=True):
        """Synchronize to disk."""
        return self.db.synchronize(hard)

    def add(self, key, value):
        """
        Add the key/value pair to the database. If the key already exists, then
        no change is made to the existing value.

        Returns boolean indicating whether value was added.
        """
        return self.db.add(key, value)

    def replace(self, key, value):
        """
        Replace the value at the given key. If the key does not exist, then
        no change is made.

        Returns boolean indicating whether value was replaced.
        """
        return self.db.replace(key, value)

    def append(self, key, value):
        """
        Append the value to a pre-existing value at the given key. If no
        value exists, this is equivalent to set.
        """
        return self.db.append(key, value)

    def cas(self, key, old, new):
        """
        Conditionally set the new value for the given key, but only if the
        pre-existing value at the key equals `old`.

        Returns boolean indicating if the value was swapped.
        """
        return self.db.cas(key, old, new)

    def copy_to_file(self, dest):
        """
        Create a copy of the database, returns boolean indicating success.
        """
        return self.db.copy(dest)

    def begin(self, hard=False):
        """
        Begin a transaction. If `hard=True`, then the operation will be
        physically synchronized with the device.

        Returns boolean indicating success.
        """
        return self.db.begin_transaction(hard)

    def commit(self):
        """
        Commit a transaction. Returns boolean indicating success.
        """
        return self.db.end_transaction(True)

    def rollback(self):
        """
        Rollback a transaction. Returns boolean indicating success.
        """
        return self.db.end_transaction(False)

    def transaction(self):
        return transaction(self)

    def match_prefix(self, prefix, max_records=-1):
        return self.db.match_prefix(prefix, max_records)

    def match_regex(self, regex, max_records=-1):
        return self.db.match_regex(regex, max_records)

    def match(self, query, acceptable_distance=1, utf8=False, max_records=-1):
        return self.db.match_similar(
            query,
            acceptable_distance,
            utf8,
            max_records)

    def lock(self, writable=False, processor=None):
        return self.db.occupy(writable, processor)

    def incr(self, key, n=1, initial=0):
        return self.db.increment(key, n, initial)

    def decr(self, key, n=1, initial=0):
        return self.db.increment(key, n * -1, initial)

    def cursor(self, reverse=False):
        return Cursor(self.db.cursor(), reverse)

    def atomic(self, hard=False):
        """
        Perform transaction via function `fn`.
        """
        def decorator(fn):
            def fn_wrapper():
                result = fn()
                if result is False:
                    return False
                return True

            def inner():
                return self.db.transaction(fn_wrapper)

            return inner
        return decorator

    def process(self, fn):
        """
        Process database using a function. The function should accept
        a key and value.
        """
        return self.db.iterate(fn)

    def cursor_process(self, fn):
        """
        Traverse records by cursor, using the given function.
        """
        return self.db.cursor_process(fn)

    def process_items(self, fn, store_result=False):
        if store_result:
            accum = []
        else:
            accum = None

        def process(cursor):
            cursor.jump()
            if store_result:
                def inner(key, value):
                    accum.append(fn(key, value))
                    return NOP
            else:
                def inner(key, value):
                    fn(key, value)
                    return NOP
            while cursor.accept(inner):
                cursor.step()

        self.db.cursor_process(process)
        return accum

    def __iter__(self):
        return iter(self.db)

    def keys(self):
        return iter(self.db)

    def itervalues(self):
        with self.cursor() as cursor:
            for _, value in cursor:
                yield value

    def values(self):
        processor = lambda k, v: v
        return self.process_items(processor, True)

    def iteritems(self):
        with self.cursor() as cursor:
            for item in cursor:
                yield item

    def items(self):
        processor = lambda k, v: (k, v)
        return self.process_items(processor, True)

    def merge(self, databases, mode=MERGE_OVERWRITE):
        return self.db.merge([database.db for database in databases], mode)

    def __or__(self, rhs):
        return self.merge(rhs)

    def _get_int(self, key):
        return struct.unpack('>q', self[key])[0]

    def _set_int(self, key, value):
        self[key] = struct.pack('>q', value)

    def _get_float(self, key):
        return struct.unpack('>d', self[key])[0]

    def _set_float(self, key, value):
        self[key] = struct.pack('>d', value)

    def get_slice(self, start, end):
        if start and start > end:
            raise ValueError('%s must be less than or equal to %s.' % (
                start, end))

        with self.cursor() as cursor:
            if start is None:
                cursor.first()
            else:
                cursor.seek(start)

            for i, (k, v) in enumerate(cursor.fetch_until(end)):
                yield (k, v)

    def get_slice_rev(self, start, end):
        if start and start < end:
            raise ValueError('%s must be greater than or equal to %s.' % (
                start, end))

        # Iterating backwards requires a bit more hackery...
        with self.cursor(reverse=True) as cursor:
            if start is None:
                cursor.last()
            else:
                if not cursor.seek(start):
                    cursor.last()

            # When seeking, kyotocabinet may go to the next highest matching
            # record. For backwards searches, we want the lowest without
            # going over as the start point. This bit corrects that.
            res = cursor.get()
            if res and start and res[0] > start:
                cursor._previous()

            for i, (k, v) in enumerate(cursor.fetch_until(end)):
                yield (k, v)


class Cursor(object):
    def __init__(self, cursor, reverse=False):
        self._cursor = cursor
        self._reverse = reverse
        self._consumed = False

    def __enter__(self):
        if self._reverse:
            self.last()
        else:
            self.first()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self.close()
        except:
            pass

    def close(self):
        self._cursor.disable()

    def first(self):
        self._consumed = False
        self._cursor.jump()

    def last(self):
        self._consumed = False
        self._cursor.jump_back()

    def seek(self, key):
        self._consumed = False
        return self._cursor.jump(key)

    def __iter__(self):
        self._consumed = False
        return self

    def next(self):
        if self._consumed:
            raise StopIteration

        key_value = self.get()
        if key_value is None:
            self._consumed = True
            raise StopIteration

        if self._reverse:
            self._consumed = not self._previous()
        else:
            self._consumed = not self._next()

        return key_value

    def set(self, value, step=False):
        return self._cursor.set_value(value, step)

    def get(self):
        return self._cursor.get()

    def remove(self):
        return self._cursor.remove()

    def pop(self):
        return self._cursor.seize()

    def _next(self):
        return self._cursor.step()

    def _previous(self):
        return self._cursor.step_back()

    def fetch_count(self, n):
        while n > 0:
            yield next(self)
            n -= 1

    def fetch_until(self, end_key):
        compare = operator.le if self._reverse else operator.ge
        for key, value in self:
            if compare(key, end_key):
                if key == end_key:
                    yield (key, value)
                raise StopIteration
            else:
                yield (key, value)


class _callable_context_manager(object):
    def __call__(self, fn):
        def inner(*args, **kwargs):
            with self:
                return fn(*args, **kwargs)
        return inner


class transaction(_callable_context_manager):
    def __init__(self, db):
        self._db = db

    def __enter__(self):
        self._db.begin()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self._db.rollback()
        else:
            try:
                self._db.commit()
            except:
                try:
                    self._db.rollback()
                except:
                    pass
                raise

    def commit(self):
        self._db.commit()
        self._db.begin()

    def rollback(self):
        self._db.rollback()
        self._db.begin()


class HashDB(Database):
    # Persisten O(1) hash table, unordered. Key-level locking (rwlock).
    extension = DB_FILE_HASH


class TreeDB(Database):
    # Persisten O(log N) B+ tree, ordered. Page-level locking (rwlock).
    extension = DB_FILE_TREE


class DirectoryHashDB(Database):
    # Persisten O(?), unordered. Key-level locking (rwlock).
    extension = DB_DIRECTORY_HASH


class DirectoryTreeDB(Database):
    # Persisten O(log N) B+ tree, ordered. Page-level locking (rwlock).
    extension = DB_DIRECTORY_TREE


class PlainTextDB(Database):
    # Persisten O(?) plain text, stored order. Key-level locking (rwlock).
    extension = DB_TEXT


class _FilenameDatabase(Database):
    filename = None

    def __init__(self, exceptional=False, concurrent=False, **opts):
        super(_FilenameDatabase, self).__init__(
            self.filename,
            exceptional,
            concurrent,
            **opts)


class PrototypeHashDB(_FilenameDatabase):
    # Volatile O(1) hash table, unordered. DB locking.
    filename = DB_PROTOTYPE_HASH


class PrototypeTreeDB(_FilenameDatabase):
    # Volatile O(log N) red-black tree, lexical order. DB locking.
    filename = DB_PROTOTYPE_TREE


class StashDB(_FilenameDatabase):
    # Volatile O(1) hash table, unordered. Key-level locking (rwlock).
    filename = DB_STASH


class CacheHashDB(_FilenameDatabase):
    # Volatile O(1) hash table, unordered. Key-level locking (mutex).
    filename = DB_CACHE_HASH


class CacheTreeDB(_FilenameDatabase):
    # Volatile O(log N) B+ tree, ordered. Page-level locking (rwlock).
    filename = DB_CACHE_TREE
