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
        """
        var x = graph.v('x')
        graph.search([
          { s: 'matteo', p: 'friend', oect: x },
          { s: x, p: 'likes', oect: 'beer' },
          { s: x, p: 'lives', oect: 'brescia' }
        ], function(err, solutions) {
          alert(JSON.stringify(solutions))
        })

        By combining different queries, I can ask fancy questions. For
        example: What are all my friends that, like beer, live in Barcelona,
        and matteocollina consider friends as well? To get this information I
        start with an spo query to find all the people I'm friend with. Than
        for each result I get I perform an spo query to check if they like
        beer, removing the ones for which I can't find this relation. I do it
        again to filter by city. Finally I perform an ops query to find, of
        the list I obtained, who is considered friend by matteocollina.
        """
        var_map = {}
        results = {}

        for condition in conditions:
            query = condition.copy()
            var_map = {}
            result_map = {}

            for part in ('s', 'p', 'o'):
                if isinstance(query[part], Variable):
                    variable = query.pop(part)
                    var_map[part] = variable

            # Populate some result sets.
            var_map_items = var_map.items()
            for part, var in var_map_items:
                result_map[part] = set()

                if var in results:
                    for val in results[var]:
                        query[part] = val
                        for result in self.query(**query):
                            result_map[part].add(result[part])

                    results[var] = results[var] & result_map[part]
                    del query[part]
                else:
                    results.setdefault(var, set())
                    for result in self.query(**query):
                        results[var].add(result[part])

        return dict((var.name, vals) for (var, vals) in results.items())


class Variable(object):
    __slots__ = ['name']

    def __init__(self, name):
        self.name = name
