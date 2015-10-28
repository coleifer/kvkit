# Hexastore.
import itertools
import json


class Hexastore(object):
    def __init__(self, database, prefix=''):
        self.database = database
        self.prefix = prefix

    def store(self, s, p, o):
        serialized = json.dumps({
            's': s,
            'p': p,
            'o': o})

        data = {}
        for key in self.keys_for_values(s, p, o):
            data[key] = serialized

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

    def search(self, conditions):
        # I don't think this implementation is quite correct.
        results = {}

        for condition in conditions:
            if isinstance(condition, tuple):
                query = dict(zip('spo', condition))
            else:
                query = condition.copy()
            materialized = {}
            tmp_results = {}
            targets = []

            for part in ('s', 'p', 'o'):
                if isinstance(query[part], Variable):
                    variable = query.pop(part)
                    if variable in results:
                        materialized[part] = results[variable]
                    targets.append((variable, part))

            # Populate some result sets.
            for variable, target in targets:
                tmp_results[target] = set()
                if materialized:
                    for part, values in materialized.items():
                        for value in values:
                            query[part] = str(value)
                            for result in self.query(**query):
                                tmp_results[target].add(result[target])

                    if variable in results:
                        results[variable] &= tmp_results[target]
                    else:
                        results[variable] = tmp_results[target]
                else:
                    results.setdefault(variable, set())
                    for result in self.query(**query):
                        results[variable].add(result[target])

        return dict((var.name, vals) for (var, vals) in results.items())


class Variable(object):
    __slots__ = ['name']

    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return '<Variable: %s>' % (self.name)
