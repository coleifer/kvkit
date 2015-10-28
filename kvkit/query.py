import datetime
import pickle
import struct


class Node(object):
    # Node in a query tree.
    def __init__(self):
        self.negated = False

    def _e(op, inv=False):
        """
        Lightweight factory which returns a method that builds an Expression
        consisting of the left-hand and right-hand operands, using `op`.
        """
        def inner(self, rhs):
            if inv:
                return Expression(rhs, op, self)
            return Expression(self, op, rhs)
        return inner

    __and__ = _e('AND')
    __or__ = _e('OR')
    __rand__ = _e('AND', inv=True)
    __ror__ = _e('OR', inv=True)
    __eq__ = _e('=')
    __ne__ = _e('!=')
    __lt__ = _e('<')
    __le__ = _e('<=')
    __gt__ = _e('>')
    __ge__ = _e('>=')

    def startswith(self, prefix):
        return Expression(self, 'startswith', prefix)


class Expression(Node):
    def __init__(self, lhs, op, rhs):
        self.lhs = lhs
        self.op = op
        self.rhs = rhs

    def __repr__(self):
        return '<Expression: %s %s %s>' % (self.lhs, self.op, self.rhs)


class Field(Node):
    _counter = 0

    def __init__(self, index=False, default=None):
        self.index = index
        self.default = default
        self.model = None
        self.name = None
        self._order = Field._counter
        Field._counter += 1

    def __repr__(self):
        return '<%s: %s.%s>' % (
            type(self),
            self.model._meta.name,
            self.name)

    def bind(self, model, name):
        self.model = model
        self.name = name
        setattr(self.model, self.name, FieldDescriptor(self))

    def clone(self):
        field = type(self)(index=self.index, default=self.default)
        field.model = self.model
        field.name = self.name
        return field

    def db_value(self, value):
        return value

    def python_value(self, value):
        return value


class DateTimeField(Field):
    def db_value(self, value):
        if value:
            return value.strftime('%Y-%m-%d %H:%M:%S.%f')

    def python_value(self, value):
        if value:
            return datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S.%f')


class DateField(Field):
    def db_value(self, value):
        if value:
            return value.strftime('%Y-%m-%d')

    def python_value(self, value):
        if value:
            pieces = [int(piece) for piece in value.split('-')]
            return datetime.date(*pieces)


class LongField(Field):
    def db_value(self, value):
        return struct.pack('>q', value) if value is not None else ''

    def python_value(self, value):
        if value:
            return struct.unpack('>q', value)[0]


class FloatField(Field):
    def db_value(self, value):
        return struct.pack('>d', value) if value is not None else ''

    def python_value(self, value):
        if value:
            return struct.unpack('>d', value)[0]


class FieldDescriptor(object):
    def __init__(self, field):
        self.field = field
        self.name = self.field.name

    def __get__(self, instance, instance_type=None):
        if instance is not None:
            return instance._data.get(self.name)
        return self.field

    def __set__(self, instance, value):
        instance._data[self.name] = value


class DeclarativeMeta(type):
    def __new__(cls, name, bases, attrs):
        if bases == (object,):
            return super(DeclarativeMeta, cls).__new__(cls, name, bases, attrs)

        database = None
        fields = {}
        serialize = None

        # Inherit fields from parent classes.
        for base in bases:
            if not hasattr(base, '_meta'):
                continue

            for field in base._meta.sorted_fields:
                if field.name not in fields:
                    fields[field.name] = field.clone()

            if database is None and base._meta.database is not None:
                database = base._meta.database
            if serialize is None:
                serialize = base._meta.serialize

        # Introspect all declared fields.
        for key, value in attrs.items():
            if isinstance(value, Field):
                fields[key] = value

        # Read metadata configuration.
        declared_meta = attrs.pop('Meta', None)
        if declared_meta:
            if getattr(declared_meta, 'database', None) is not None:
                database = declared_meta.database
            if getattr(declared_meta, 'serialize', None) is not None:
                serialize = declared_meta.serialize

        # Always have an `id` field.
        if 'id' not in fields:
            fields['id'] = LongField()

        if serialize is None:
            serialize = True

        attrs['_meta'] = Metadata(name, database, fields, serialize)
        model = super(DeclarativeMeta, cls).__new__(cls, name, bases, attrs)

        # Bind fields to model.
        for name, field in fields.items():
            field.bind(model, name)

        # Process
        model._meta.prepared()

        return model


class Metadata(object):
    def __init__(self, model_name, database, fields, serialize):
        self.model_name = model_name
        self.database = database
        self.fields = fields
        self.serialize = serialize

        self.name = model_name.lower()
        self.sequence = 'id_seq:%s' % self.name

        self.defaults = {}
        self.defaults_callable = {}

    def prepared(self):
        self.sorted_fields = sorted(
            [field for field in self.fields.values()],
            key=lambda field: field._order)

        # Populate index attributes.
        self.indexed_fields = set()
        self.indexed_field_objects = []
        self.indexes = {}
        for field in self.sorted_fields:
            if field.index:
                self.indexed_fields.add(field.name)
                self.indexed_field_objects.append(field)
                self.indexes[field.name] = Index(self.database, field)

        for field in self.sorted_fields:
            if callable(field.default):
                self.defaults_callable[field.name] = field.default
            elif field.default:
                self.defaults[field.name] = field.default

    def next_id(self):
        return self.database.incr(self.sequence)

    def get_instance_key(self, instance_id):
        return '%s:%s' % (self.name, instance_id)


def with_metaclass(meta, base=object):
    return meta('newbase', (base,), {})


class Model(with_metaclass(DeclarativeMeta)):
    def __init__(self, **kwargs):
        self._data = self._meta.defaults.copy()
        for key, value in self._meta.defaults_callable.items():
            self._data[key] = value()
        self._data.update(kwargs)

    @classmethod
    def create(cls, **kwargs):
        instance = cls(**kwargs)
        instance.save()
        return instance

    @classmethod
    def load(cls, primary_key):
        return cls(**cls._read_model_data(primary_key))

    def save(self, atomic=True):
        if atomic:
            with self._meta.database.transaction():
                self._save()
        else:
            self._save()

    def _save(self):
        # If we are updating an existing object, load the original data
        # so we can correctly update any indexes.
        original_data = None
        if self.id and self._meta.indexes:
            original_data = type(self)._read_indexed_data(self.id)

        # Save the actual model data.
        self._save_model_data()

        # Update any secondary indexes.
        self._update_indexes(original_data)

    def _save_model_data(self):
        database = self._meta.database

        # Generate the next ID in sequence if no ID is set.
        if not self.id:
            self.id = self._meta.next_id()

        # Retrieve the primary key identifying this model instance.
        key = self._meta.get_instance_key(self.id)

        if self._meta.serialize:
            # Store all model data serialized in a single record.
            database[key] = pickle.dumps(self._data)
        else:
            # Store model data in discrete records, one per field.
            for field in self._meta.sorted_fields:
                field_key = '%s:%s' % (key, field.name)
                value = field.db_value(getattr(self, field.name))
                database[field_key] = value or ''

    def _update_indexes(self, original_data):
        database = self._meta.database
        primary_key = self.id

        for field, index in self._meta.indexes.items():
            # Retrieve the value of the indexed field.
            value = getattr(self, field)

            # If the value differs from what was previously stored, remove
            # the old value.
            if original_data is not None and original_data[field] != value:
                index.delete(value, primary_key)

            # Store the value in the index.
            index.store(value, primary_key)
            index.store_endpoint()

    @classmethod
    def _read_model_data(cls, primary_key, fields=None):
        key = cls._meta.get_instance_key(primary_key)
        if cls._meta.serialize:
            # For serialized models, simply grab all data.
            data = pickle.loads(cls._meta.database[key])
        else:
            # Load the model data from each field.
            data = {}
            fields = fields or cls._meta.sorted_fields
            for field in fields:
                field_key = '%s:%s' % (key, field.name)
                data[field.name] = field.python_value(
                    cls._meta.database[field_key])
        return data

    @classmethod
    def _read_indexed_data(cls, primary_key):
        return cls._read_model_data(
            primary_key,
            cls._meta.indexed_field_objects)

    def delete(self, atomic=True):
        if atomic:
            with self._meta.database.transaction():
                self._delete()
        else:
            self._delete()

    def _delete(self):
        database = self._meta.database

        key = self._meta.get_instance_key(self.id)
        if self._meta.serialize:
            del database[key]
        else:
            # Save model data to discrete fields.
            for field in self._meta.sorted_fields:
                field_key = '%s:%s' % (key, field.name)
                del database[field_key]

        for field, index in self._meta.indexes.items():
            index.delete(getattr(self, field), self.id)

    @classmethod
    def get(cls, expr):
        results = cls.query(expr)
        if results:
            return results[0]

    @classmethod
    def query(cls, expr):
        def dfs(expr):
            lhs = expr.lhs
            rhs = expr.rhs
            if isinstance(lhs, Expression):
                lhs = dfs(lhs)
            if isinstance(rhs, Expression):
                rhs = dfs(rhs)

            if isinstance(lhs, Field):
                index = cls._meta.indexes[lhs.name]
                return set(index.query(rhs, expr.op))
            elif expr.op == 'AND':
                return set(lhs) & set(rhs)
            elif expr.op == 'OR':
                return set(lhs) | set(rhs)
            else:
                raise ValueError('Unable to execute query, unexpected type.')

        id_list = dfs(expr)
        return [cls.load(primary_key) for primary_key in sorted(id_list)]


class Index(object):
    def __init__(self, database, field):
        self.database = database
        self.field = field
        self.name = 'idx:%s:%s' % (field.model._meta.name, field.name)
        self.stop_key = '%s\xff\xff\xff' % self.name
        self.convert_pk = field.model.id.db_value

    def get_key(self, value, primary_key):
        return '%s\xff%s\xff%s' % (
            self.name,
            self.field.db_value(value) or '',
            self.convert_pk(primary_key))

    def get_prefix(self, value=None, closed=False):
        if value is None:
            return '%s\xff' % self.name
        else:
            return '%s\xff%s%s' % (
                self.name,
                self.field.db_value(value),
                '\xff' if closed else '')

    def store(self, value, primary_key):
        self.database[self.get_key(value, primary_key)] = str(primary_key)

    def delete(self, value, primary_key):
        del self.database[self.get_key(value, primary_key)]

    def store_endpoint(self):
        self.database[self.stop_key] = ''

    def query(self, value, operation):
        if operation == '=':
            start_key = self.get_prefix(value, closed=True)
            end_key = start_key + '\xff'
            return [value for key, value in self.database[start_key:end_key]]
        elif operation in ('<', '<='):
            start_key = self.get_prefix()
            end_key = self.get_prefix(value) + '\xff'
            if operation == '<=':
                end_key += '\xff'
            results = self.database[start_key:end_key]
            return [value for key, value in results]
        elif operation in ('>', '>='):
            start_key = self.stop_key
            end_key = self.get_prefix(value)
            if operation == '>':
                end_key += '\xff\xff'
            results = self.database[start_key:end_key]
            return [value for i, (key, value) in enumerate(results) if i > 0]
        elif operation == '!=':
            match = self.get_prefix(value, closed=True)
            start_key = self.get_prefix()
            end_key = self.stop_key
            results = self.database[start_key:end_key]
            return [v for k, v in results if not k.startswith(match)][:-1]
        elif operation == 'startswith':
            start_key = self.get_prefix(value)
            end_key = start_key + '\xff\xff'
            return [value for key, value in self.database[start_key:end_key]]
