# Hexastore.
import itertools
import json


class _VariableGenerator(object):
    def __getattr__(self, name):
        return Variable(name)

    def __call__(self, name):
        return Variable(name)


class Hexastore(object):

    def __init__(self, database, prefix=''):
        self.database = database
        self.prefix = prefix
        self.v = _VariableGenerator()

    def _data_for_storage(self, s, p, o):
        serialized = json.dumps({
            's': s,
            'p': p,
            'o': o})

        data = {}
        for key in self.keys_for_values(s, p, o):
            data[key] = serialized

        return data

    def store(self, s, p, o):
        return self.database.update(self._data_for_storage(s, p, o))

    def store_many(self, items):
        data = {}
        for item in items:
            data.update(self._data_for_storage(*item))
        return self.database.update(data)

    def delete(self, s, p, o):
        for key in self.keys_for_values(s, p, o):
            del self.database[key]

    def keys_for_values(self, s, p, o):
        zipped = zip('spo', (s, p, o))
        for ((p1, v1), (p2, v2), (p3, v3)) in itertools.permutations(zipped):
            yield '::'.join((
                self.prefix,
                ''.join((p1, p2, p3)),
                v1,
                v2,
                v3))

    def keys_for_query(self, s=None, p=None, o=None):
        parts = [self.prefix]
        key = lambda parts: '::'.join(parts)

        if s and p and o:
            parts.extend(('spo', s, p, o))
            return key(parts), None
        elif s and p:
            parts.extend(('spo', s, p))
        elif s and o:
            parts.extend(('sop', s, o))
        elif p and o:
            parts.extend(('pos', p, o))
        elif s:
            parts.extend(('spo', s))
        elif p:
            parts.extend(('pso', p))
        elif o:
            parts.extend(('osp', o))
        return key(parts + ['']), key(parts + ['\xff'])

    def query(self, s=None, p=None, o=None):
        start, end = self.keys_for_query(s, p, o)
        if end is None:
            try:
                yield json.loads(self.database[start])
            except KeyError:
                raise StopIteration
        else:
            for key, value in self.database[start:end]:
                yield json.loads(value)

    def v(self, name):
        return Variable(name)

    def search(self, *conditions):
        results = {}

        for condition in conditions:
            if isinstance(condition, tuple):
                query = dict(zip('spo', condition))
            else:
                query = condition.copy()
            materialized = {}
            targets = []

            for part in ('s', 'p', 'o'):
                if isinstance(query[part], Variable):
                    variable = query.pop(part)
                    materialized[part] = set()
                    targets.append((variable, part))

            # Potentially rather than popping all the variables, we could use
            # the result values from a previous condition and do O(results)
            # loops looking for a single variable.
            for result in self.query(**query):
                ok = True
                for var, part in targets:
                    if var in results and result[part] not in results[var]:
                        ok = False
                        break

                if ok:
                    for var, part in targets:
                        materialized[part].add(result[part])

            for var, part in targets:
                if var in results:
                    results[var] &= materialized[part]
                else:
                    results[var] = materialized[part]

        return dict((var.name, vals) for (var, vals) in results.items())


class Variable(object):
    __slots__ = ['name']

    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return '<Variable: %s>' % (self.name)
