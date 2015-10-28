import datetime
import gc
import os
import shutil
import struct
import sys
import tempfile
import unittest

from kvkit.backends.kyoto import *
from kvkit.backends.kyoto import _FilenameDatabase
from kvkit.graph import *
from kvkit.query import *

try:
    from kvkit.backends.berkeleydb import BerkeleyDB
except ImportError:
    BerkeleyDB = None

try:
    from kvkit.backends.leveldb import LevelDB
except ImportError:
    LevelDB = None

try:
    from kvkit.backends.rocks import RocksDB
except ImportError:
    RocksDB = None

try:
    from kvkit.backends.sqlite4 import LSM
except ImportError:
    LSM = None


class BaseTestCase(unittest.TestCase):
    database_class = None

    def setUp(self):
        self.db = self.create_db()

    def tearDown(self):
        if self.db is not None:
            try:
                self.db.close()
            except Exception:
                pass
            finally:
                self.delete_db()

    def delete_db(self):
        if not isinstance(self.db, _FilenameDatabase):
            if os.path.isfile(self.db.filename):
                os.unlink(self.db.filename)
            elif os.path.isdir(self.db.filename):
                shutil.rmtree(self.db.filename)

    def create_db(self):
        if self.database_class is not None:
            db = self.database_class('test.db')
            return db

    def create_rows(self, n):
        for i in range(n):
            self.db['k%s' % i] = i


class KVKitTests(object):
    def test_storage(self):
        self.db['k1'] = 'v1'
        self.db['k2'] = 'v2'
        self.assertEqual(self.db['k1'], 'v1')
        self.assertEqual(self.db['k2'], 'v2')
        self.assertRaises(KeyError, lambda: self.db['k3'])

        self.assertEqual(self.db[['k1', 'k2', 'k3']], {
            'k1': 'v1',
            'k2': 'v2',
        })
        self.assertEqual(self.db[['kx', 'ky']], {})

        self.assertTrue('k1' in self.db)
        self.assertFalse('k3' in self.db)
        self.assertEqual(len(self.db), 2)

        self.assertEqual(self.db.pop('k1'), 'v1')
        self.assertRaises(KeyError, lambda: self.db.pop('k1'))

        self.assertEqual(self.db.pop(), ('k2', 'v2'))
        self.assertEqual(len(self.db), 0)

        data = dict(('k%s' % i, i) for i in range(0, 40, 5))
        self.db.update(**data)
        self.assertEqual(len(self.db), 8)

        self.assertEqual(sorted(self.db.match_prefix('k2')), ['k20', 'k25'])
        self.assertEqual(
            sorted(self.db.match_regex('k[2,3]5')),
            ['k25', 'k35'])
        self.assertEqual(
            sorted(self.db.match('k2')),
            ['k0', 'k20', 'k25', 'k5'])

        self.assertTrue(self.db.append('k0', 'xx'))
        self.assertTrue(self.db.append('kx', 'huey'))
        self.assertEqual(self.db['k0'], '0xx')
        self.assertEqual(self.db['kx'], 'huey')

        self.assertTrue(self.db.replace('kx', 'mickey'))
        self.assertEqual(self.db['kx'], 'mickey')
        self.assertFalse(self.db.replace('kz', 'foo'))
        self.assertRaises(KeyError, lambda: self.db['kz'])

        self.assertEqual(self.db.incr('ct'), 1)
        self.assertEqual(self.db.incr('ct', 2), 3)
        self.assertEqual(self.db.decr('ct', 4), -1)

    def test_store_types(self):
        self.db._set_int('i1', 1337)
        self.db._set_int('i2', 0)

        self.db._set_float('f1', 3.14159)
        self.db._set_float('f2', 0)

        self.assertEqual(self.db._get_int('i1'), 1337)
        self.assertEqual(self.db._get_int('i2'), 0)

        self.assertEqual(self.db._get_float('f1'), 3.14159)
        self.assertEqual(self.db._get_float('f2'), 0.)

    def test_atomic(self):
        @self.db.atomic()
        def atomic_succeed():
            self.db['k1'] = 'v1'

        @self.db.atomic()
        def atomic_fail():
            self.db['k2'] = 'v2'
            return False

        atomic_succeed()
        self.assertEqual(self.db['k1'], 'v1')

        atomic_fail()
        self.assertFalse('k2' in self.db)

    def test_transaction(self):
        with self.db.transaction():
            self.db['k1'] = 'v1'
            self.db['k2'] = 'v2'

        data = {'k1': 'v1', 'k2': 'v2'}
        self.assertEqual(self.db[['k1', 'k2']], data)

        def failed_transaction():
            with self.db.transaction():
                self.db['k3'] = 'v3'
                raise Exception()

        self.assertRaises(Exception, failed_transaction)
        self.assertFalse('k3' in self.db)

        @self.db.transaction()
        def f_succeed():
            self.db['kx'] = 'x'

        @self.db.transaction()
        def f_fail():
            self.db['kz'] = 'z'
            raise Exception()

        f_succeed()
        self.assertEqual(self.db['kx'], 'x')

        self.assertRaises(Exception, f_fail)
        self.assertRaises(KeyError, lambda: self.db['kz'])

    def test_cursors(self):
        self.create_rows(4)
        with self.db.cursor() as cursor:
            items = list(cursor)
            self.assertEqual(items, [
                ('k0', '0'),
                ('k1', '1'),
                ('k2', '2'),
                ('k3', '3'),
            ])

        cursor = self.db.cursor()
        cursor.seek('k2')
        results = list(cursor)
        self.assertEqual(results, [('k2', '2'), ('k3', '3')])
        cursor.close()

        with self.db.cursor() as cursor:
            cursor.seek('k1')
            results = cursor.fetch_until('k3')
            self.assertEqual(
                list(results), [('k1', '1'), ('k2', '2'), ('k3', '3')])

            cursor.seek('k0')
            results = cursor.fetch_count(2)
            self.assertEqual(list(results), [('k0', '0'), ('k1', '1')])

            cursor.seek('k1')
            results = cursor.fetch_until('k222')
            self.assertEqual(list(results), [('k1', '1'), ('k2', '2')])

    def test_process(self):
        self.db.update(k1='v1', k2='v2')
        def to_upper(key, value):
            return value.upper()

        self.assertTrue(self.db.process(to_upper))
        self.assertEqual(self.db['k1'], 'V1')
        self.assertEqual(self.db['k2'], 'V2')

    def test_dict_iteration(self):
        self.db.update(k1='v1', k2='v2', k3='v3')
        self.assertEqual(sorted(self.db.keys()), ['k1', 'k2', 'k3'])
        self.assertEqual(sorted(self.db.values()), ['v1', 'v2', 'v3'])
        self.assertEqual(sorted(self.db.itervalues()), ['v1', 'v2', 'v3'])
        self.assertEqual(sorted(self.db.items()), [
            ('k1', 'v1'), ('k2', 'v2'), ('k3', 'v3')])
        self.assertEqual(sorted(self.db.iteritems()), [
            ('k1', 'v1'), ('k2', 'v2'), ('k3', 'v3')])


class SliceTests(object):
    def assertSlice(self, s, expected):
        self.assertEqual(list(s), [(val, val) for val in expected])

    def create_slice_data(self):
        data = ('aa', 'aa1', 'aa2', 'bb', 'cc', 'dd', 'ee', 'ff')
        for value in data:
            self.db[value] = value

    def test_slices(self):
        self.create_slice_data()

        # Endpoints both exist.
        s = self.db['aa':'cc']
        self.assertSlice(s, ['aa', 'aa1', 'aa2', 'bb', 'cc'])

        # Missing start.
        s = self.db['aa0':'cc']
        self.assertSlice(s, ['aa1', 'aa2', 'bb', 'cc'])

        # Missing end.
        s = self.db['aa1':'cc2']
        self.assertSlice(s, ['aa1', 'aa2', 'bb', 'cc'])

        # Missing both.
        s = self.db['aa0':'cc2']
        self.assertSlice(s, ['aa1', 'aa2', 'bb', 'cc'])

        # Start precedes first key.
        s = self.db['\x01':'aa2']
        self.assertSlice(s, ['aa', 'aa1', 'aa2'])

        # End exceeds last key.
        s = self.db['dd':'zz']
        self.assertSlice(s, ['dd', 'ee', 'ff'])

    def test_slice_reverse(self):
        self.create_slice_data()

        # Endpoints both exist.
        s = self.db['ff':'cc']
        self.assertSlice(s, ['ff', 'ee', 'dd', 'cc'])

        # Missing end.
        s = self.db['cc':'aa0']
        self.assertSlice(s, ['cc', 'bb', 'aa2', 'aa1'])

        # Missing start.
        s = self.db['cc2':'aa1']
        self.assertSlice(s, ['cc', 'bb', 'aa2', 'aa1'])

        # Missing both.
        s = self.db['cc2':'aa0']
        self.assertSlice(s, ['cc', 'bb', 'aa2', 'aa1'])

        # Start exceeds last key.
        s = self.db['zz':'cc']
        self.assertSlice(s, ['ff', 'ee', 'dd', 'cc'])

        # End precedes first key.
        s = self.db['bb':'\x01']
        self.assertSlice(s, ['bb', 'aa2', 'aa1', 'aa'])

        # Start is almost to the last key.
        s = self.db['ef':'cc']
        self.assertSlice(s, ['ee', 'dd', 'cc'])

    def test_slice_start_end(self):
        self.create_slice_data()

        s = self.db[:'bb']
        self.assertSlice(s, ['aa', 'aa1', 'aa2', 'bb'])

        s = self.db[:'cc':True]
        self.assertSlice(s, ['ff', 'ee', 'dd', 'cc'])

        s = self.db['cc'::True]
        self.assertSlice(s, ['cc', 'bb', 'aa2', 'aa1', 'aa'])


class ModelTests(object):
    def setUp(self):
        super(ModelTests, self).setUp()

        class Person(Model):
            first = Field(index=True)
            last = Field(index=True)
            dob = DateField(index=True)

            class Meta:
                database = self.db
                serialize = False

        class Note(Model):
            content = Field()
            timestamp = DateTimeField(default=datetime.datetime.now)

            class Meta:
                database = self.db

        class Numeric(Model):
            x = LongField(index=True)
            y = FloatField(index=True)
            z = DateField(index=True)

            class Meta:
                database = self.db

        self.Person = Person
        self.Note = Note
        self.Numeric = Numeric

    def test_model_operations(self):
        huey = self.Person.create(
            first='huey',
            last='leifer',
            dob=datetime.date(2010, 1, 2))
        self.assertEqual(huey.first, 'huey')
        self.assertEqual(huey.last, 'leifer')
        self.assertEqual(huey.dob, datetime.date(2010, 1, 2))
        self.assertEqual(huey.id, 1)

        ziggy = self.Person.create(
            first='ziggy',
            dob=datetime.date(2011, 2, 3))
        self.assertEqual(ziggy.first, 'ziggy')
        self.assertEqual(ziggy.last, None)
        self.assertEqual(ziggy.id, 2)

        huey_db = self.Person.load(1)
        self.assertEqual(huey_db.first, 'huey')
        self.assertEqual(huey_db.last, 'leifer')
        self.assertEqual(huey_db.dob, datetime.date(2010, 1, 2))
        self.assertEqual(huey_db.id, 1)

        ziggy_db = self.Person.load(2)
        self.assertEqual(ziggy_db.first, 'ziggy')
        self.assertEqual(ziggy_db.last, '')
        self.assertEqual(ziggy_db.dob, datetime.date(2011, 2, 3))
        self.assertEqual(ziggy_db.id, 2)

        keys_1 = set(self.db.keys())
        huey_db.delete()
        keys_2 = set(self.db.keys())
        diff = keys_1 - keys_2
        one = struct.pack('>q', 1)
        two = struct.pack('>q', 2)
        self.assertEqual(diff, set([
            'person:1:first', 'person:1:last', 'person:1:dob', 'person:1:id',
            'idx:person:first\xffhuey\xff%s' % one,
            'idx:person:last\xffleifer\xff%s' % one,
            'idx:person:dob\xff2010-01-02\xff%s' % one]))
        self.assertEqual(keys_2, set([
            'person:2:first', 'person:2:last', 'person:2:dob', 'person:2:id',
            'idx:person:first\xffziggy\xff%s' % two,
            'idx:person:last\xff\xff%s' % two,
            'idx:person:dob\xff2011-02-03\xff%s' % two, 'id_seq:person',
            'idx:person:first\xff\xff\xff',
            'idx:person:last\xff\xff\xff',
            'idx:person:dob\xff\xff\xff',
        ]))

    def test_model_serialized(self):
        note = self.Note.create(content='note 1')
        self.assertTrue(note.timestamp is not None)
        self.assertEqual(note.id, 1)

        note_db = self.Note.load(note.id)
        self.assertEqual(note_db.content, 'note 1')
        self.assertEqual(note_db.timestamp, note.timestamp)
        self.assertEqual(note_db.id, 1)

        note2 = self.Note.create(content='note 2')
        keys_1 = set(self.db.keys())
        note.delete()
        keys_2 = set(self.db.keys())
        diff = keys_1 - keys_2
        self.assertEqual(diff, set(['note:1']))
        self.assertEqual(keys_1, set([
            'id_seq:note', 'note:1', 'note:2']))

    def _create_people(self):
        people = (
            ('huey', 'leifer'),
            ('mickey', 'leifer'),
            ('zaizee', 'owen'),
            ('beanie', 'owen'),
            ('scout', 'owen'),
        )
        for first, last in people:
            self.Person.create(first=first, last=last)

    def assertPeople(self, expr, first_names):
        results = self.Person.query(expr)
        self.assertEqual([person.first for person in results], first_names)

    def test_query(self):
        self._create_people()

        self.assertPeople(self.Person.last == 'leifer', ['huey', 'mickey'])
        self.assertPeople(
            self.Person.last == 'owen',
            ['zaizee', 'beanie', 'scout'])

    def test_get(self):
        self._create_people()
        huey = self.Person.get(self.Person.first == 'huey')
        self.assertEqual(huey.first, 'huey')
        self.assertEqual(huey.last, 'leifer')

        zaizee = self.Person.get(
            (self.Person.first == 'zaizee') &
            (self.Person.last == 'owen'))
        self.assertEqual(zaizee.first, 'zaizee')
        self.assertEqual(zaizee.last, 'owen')

        self.assertIsNone(self.Person.get(self.Person.first == 'not here'))

    def test_query_tree(self):
        self._create_people()

        expr = (self.Person.last == 'leifer') | (self.Person.first == 'scout')
        self.assertPeople(expr, ['huey', 'mickey', 'scout'])

        expr = (
            (self.Person.last == 'leifer') |
            (self.Person.first == 'scout') |
            (self.Person.first >= 'z'))
        self.assertPeople(expr, ['huey', 'mickey', 'zaizee', 'scout'])

    def test_less_than(self):
        self._create_people()

        # Less than an existing value.
        expr = (self.Person.first < 'mickey')
        self.assertPeople(expr, ['huey', 'beanie'])

        # Less than or equal to an existing value.
        expr = (self.Person.first <= 'mickey')
        self.assertPeople(expr, ['huey', 'mickey', 'beanie'])

        # Less than a non-existant value.
        expr = (self.Person.first < 'nuggie')
        self.assertPeople(expr, ['huey', 'mickey', 'beanie'])

        # Less than or equal to a non-existant value.
        expr = (self.Person.first <= 'nuggie')
        self.assertPeople(expr, ['huey', 'mickey', 'beanie'])

    def test_greater_than(self):
        self._create_people()

        # Greater than an existing value.
        expr = (self.Person.first > 'mickey')
        self.assertPeople(expr, ['zaizee', 'scout'])

        # Greater than or equal to an existing value.
        expr = (self.Person.first >= 'mickey')
        self.assertPeople(expr, ['mickey', 'zaizee', 'scout'])

        # Greater than a non-existant value.
        expr = (self.Person.first > 'nuggie')
        self.assertPeople(expr, ['zaizee', 'scout'])

        # Greater than or equal to a non-existant value.
        expr = (self.Person.first >= 'nuggie')
        self.assertPeople(expr, ['zaizee', 'scout'])

    def test_startswith(self):
        names = ('aaa', 'aab', 'abb', 'bbb', 'ba')
        for name in names:
            self.Person.create(first=name, last=name)

        self.assertPeople(
            self.Person.last.startswith('a'),
            ['aaa', 'aab', 'abb'])

        self.assertPeople(self.Person.last.startswith('aa'), ['aaa', 'aab'])
        self.assertPeople(self.Person.last.startswith('aaa'), ['aaa'])
        self.assertPeople(self.Person.last.startswith('aaaa'), [])
        self.assertPeople(self.Person.last.startswith('b'), ['bbb', 'ba'])
        self.assertPeople(self.Person.last.startswith('bb'), ['bbb'])
        self.assertPeople(self.Person.last.startswith('c'), [])

    def create_numeric(self):
        values = (
            (1, 2.0, datetime.date(2015, 1, 2)),
            (2, 3.0, datetime.date(2015, 1, 3)),
            (3, 4.0, datetime.date(2015, 1, 4)),
            (10, 10.0, datetime.date(2015, 1, 10)),
            (11, 11.0, datetime.date(2015, 1, 11)),
        )
        for x, y, z in values:
            self.Numeric.create(x=x, y=y, z=z)

    def assertNumeric(self, expr, xs):
        query = self.Numeric.query(expr)
        self.assertEqual([n.x for n in query], xs)

    def test_query_numeric(self):
        self.create_numeric()

        X = self.Numeric.x
        Y = self.Numeric.y

        self.assertNumeric(X == 3, [3])
        self.assertNumeric(X < 3, [1, 2])
        self.assertNumeric(X <= 3, [1, 2, 3])
        self.assertNumeric(X > 3, [10, 11])
        self.assertNumeric(X >= 3, [3, 10, 11])

        # Missing values.
        self.assertNumeric(X < 4, [1, 2, 3])
        self.assertNumeric(X <= 4, [1, 2, 3])
        self.assertNumeric(X > 4, [10, 11])
        self.assertNumeric(X >= 4, [10, 11])

        # Higher than largest.
        self.assertNumeric(X > 11, [])
        self.assertNumeric(X > 12, [])
        self.assertNumeric(X >= 12, [])

        # Lower than smallest.
        self.assertNumeric(X < 1, [])
        self.assertNumeric(self.Numeric.x < 0, [])  # XXX: ??
        self.assertNumeric(self.Numeric.x <= 0, [])

        # Floats.
        self.assertNumeric(Y == 4.0, [3])
        self.assertNumeric(Y < 4.0, [1, 2])
        self.assertNumeric(Y <= 4.0, [1, 2, 3])
        self.assertNumeric(Y > 4.0, [10, 11])
        self.assertNumeric(Y >= 4.0, [3, 10, 11])

        # Missing values.
        self.assertNumeric(Y < 5.0, [1, 2, 3])
        self.assertNumeric(Y <= 5.0, [1, 2, 3])
        self.assertNumeric(Y > 5.04, [10, 11])
        self.assertNumeric(Y >= 5.04, [10, 11])

        # Higher than largest.
        self.assertNumeric(Y > 11., [])
        self.assertNumeric(Y > 11.1, [])
        self.assertNumeric(Y >= 11.1, [])

        # Lower than smallest.
        self.assertNumeric(Y < 2.0, [])
        self.assertNumeric(self.Numeric.y < 0, [])  # XXX: ??
        self.assertNumeric(self.Numeric.y <= 0, [])

    def test_query_numeric_complex(self):
        # 1, 2, 3, 10, 11   ---   2., 3., 4., 10., 11.
        self.create_numeric()

        X = self.Numeric.x
        Y = self.Numeric.y

        expr = ((X <= 2) | (Y > 9))
        self.assertNumeric(expr, [1, 2, 10, 11])

        expr = (
            ((X < 1) | (Y > 10)) |
            ((X > 14) & (Y < 1)) |
            (X == 3))
        self.assertNumeric(expr, [3, 11])

        expr = ((X != 2) & (X != 3))
        self.assertNumeric(expr, [1, 10, 11])


class GraphTests(object):
    def setUp(self):
        super(GraphTests, self).setUp()
        self.H = Hexastore(self.db)

    def create_graph_data(self):
        data = (
            ('charlie', 'likes', 'huey'),
            ('charlie', 'likes', 'mickey'),
            ('charlie', 'likes', 'zaizee'),
            ('charlie', 'is', 'human'),
            ('connor', 'likes', 'huey'),
            ('connor', 'likes', 'mickey'),
            ('huey', 'eats', 'catfood'),
            ('huey', 'is', 'cat'),
            ('mickey', 'eats', 'anything'),
            ('mickey', 'is', 'dog'),
            ('zaizee', 'eats', 'catfood'),
            ('zaizee', 'is', 'cat'),
        )
        self.H.store_many(data)

    def test_search_extended(self):
        self.create_graph_data()
        X = self.H.v.x
        Y = self.H.v.y
        Z = self.H.v.z
        result = self.H.search(
            (X, 'likes', Y),
            (Y, 'is', 'cat'),
            (Z, 'likes', Y))
        self.assertEqual(result['x'], set(['charlie', 'connor']))
        self.assertEqual(result['y'], set(['huey', 'zaizee']))
        self.assertEqual(result['z'], set(['charlie', 'connor']))

        self.H.store_many((
            ('charlie', 'likes', 'connor'),
            ('connor', 'likes', 'charlie'),
            ('connor', 'is', 'baby'),
            ('connor', 'is', 'human'),
            ('nash', 'is', 'baby'),
            ('nash', 'is', 'human'),
            ('connor', 'lives', 'ks'),
            ('nash', 'lives', 'nv'),
            ('charlie', 'lives', 'ks')))

        result = self.H.search(
            ('charlie', 'likes', X),
            (X, 'is', 'baby'),
            (X, 'lives', 'ks'))
        self.assertEqual(result, {'x': set(['connor'])})

        result = self.H.search(
            (X, 'is', 'baby'),
            (X, 'likes', Y),
            (Y, 'lives', 'ks'))
        self.assertEqual(result, {
            'x': set(['connor']),
            'y': set(['charlie']),
        })

    def assertTriples(self, result, expected):
        result = list(result)
        self.assertEqual(len(result), len(expected))
        for i1, i2 in zip(result, expected):
            self.assertEqual(
                (i1['s'], i1['p'], i1['o']), i2)

    def test_query(self):
        self.create_graph_data()
        res = self.H.query('charlie', 'likes')
        self.assertTriples(res, (
            ('charlie', 'likes', 'huey'),
            ('charlie', 'likes', 'mickey'),
            ('charlie', 'likes', 'zaizee'),
        ))

        res = self.H.query(p='is', o='cat')
        self.assertTriples(res, (
            ('huey', 'is', 'cat'),
            ('zaizee', 'is', 'cat'),
        ))

        res = self.H.query(s='huey')
        self.assertTriples(res, (
            ('huey', 'eats', 'catfood'),
            ('huey', 'is', 'cat'),
        ))

        res = self.H.query(o='huey')
        self.assertTriples(res, (
            ('charlie', 'likes', 'huey'),
            ('connor', 'likes', 'huey'),
        ))

    def test_search(self):
        self.create_graph_data()
        X = self.H.v('x')
        result = self.H.search(
            {'s': 'charlie', 'p': 'likes', 'o': X},
            {'s': X, 'p': 'eats', 'o': 'catfood'},
            {'s': X, 'p': 'is', 'o': 'cat'})
        self.assertEqual(result, {'x': set(['huey', 'zaizee'])})

    def test_search_simple(self):
        self.create_friends()
        X = self.H.v('x')
        result = self.H.search({'s': X, 'p': 'friend', 'o': 'charlie'})
        self.assertEqual(result, {'x': set(['huey', 'zaizee'])})

    def test_search_2var(self):
        self.create_friends()
        X = self.H.v('x')
        Y = self.H.v('y')

        result = self.H.search(
            {'s': X, 'p': 'friend', 'o': 'charlie'},
            {'s': Y, 'p': 'friend', 'o': X})
        self.assertEqual(result, {
            'x': set(['huey']),
            'y': set(['charlie']),
        })

        result = self.H.search(
            ('charlie', 'friend', X),
            (X, 'friend', Y),
            (Y, 'friend', 'nuggie'))
        self.assertEqual(result, {
            'x': set(['huey']),
            'y': set(['mickey']),
        })

        result = self.H.search(
            ('huey', 'friend', X),
            (X, 'friend', Y))
        self.assertEqual(result['y'], set(['huey', 'nuggie']))

    def test_search_mutual(self):
        self.create_friends()
        X = self.H.v('x')
        Y = self.H.v('y')

        result = self.H.search(
            {'s': X, 'p': 'friend', 'o': Y},
            {'s': Y, 'p': 'friend', 'o': X})
        self.assertEqual(result['y'], set(['charlie', 'huey']))

    def create_friends(self):
        data = (
            ('charlie', 'friend', 'huey'),
            ('huey', 'friend', 'charlie'),
            ('huey', 'friend', 'mickey'),
            ('zaizee', 'friend', 'charlie'),
            ('zaizee', 'friend', 'mickey'),
            ('mickey', 'friend', 'nuggie'),
        )
        for item in data:
            self.H.store(*item)


class HashTests(KVKitTests, BaseTestCase):
    database_class = HashDB


class TreeTests(KVKitTests, GraphTests, ModelTests, SliceTests, BaseTestCase):
    database_class = TreeDB


class CacheHashTests(KVKitTests, BaseTestCase):
    database_class = CacheHashDB


class CacheTreeTests(KVKitTests, GraphTests, ModelTests, SliceTests,
                     BaseTestCase):
    database_class = CacheTreeDB


if BerkeleyDB:
    class BerkeleyDBTests(SliceTests, GraphTests, ModelTests, BaseTestCase):
        database_class = BerkeleyDB


if LevelDB:
    class LevelDBTests(SliceTests, GraphTests, ModelTests, BaseTestCase):
        database_class = LevelDB


if LSM:
    class LSMTests(SliceTests, GraphTests, ModelTests, BaseTestCase):
        database_class = LSM


if RocksDB:
    # RocksDB does not implement an actual `close()` method, so we cannot
    # reliably re-use the same database file due to locks hanging around.
    # For that reason, each test needs to either re-use the same DB or use
    # a new db file. I opted for the latter.
    class RocksDBTests(SliceTests, GraphTests, ModelTests, BaseTestCase):
        database_class = RocksDB

        def create_db(self):
            return self.database_class(tempfile.mktemp())


if __name__ == '__main__':
    unittest.main(argv=sys.argv)
