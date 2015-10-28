#!/usr/bin/env python

import optparse
import sys
import unittest

from kvkit import tests

def runtests(cases=None):
    if cases:
        suite = unittest.TestLoader().loadTestsFromNames(cases)
    else:
        suite = unittest.TestLoader().loadTestsFromModule(tests)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    if result.failures:
        sys.exit(1)
    elif result.errors:
        sys.exit(2)
    sys.exit(0)

if __name__ == '__main__':
    parser = optparse.OptionParser()
    opt = parser.add_option
    opt('-b', '--berkeleydb', dest='berkeleydb', action='store_true')
    opt('-H', '--kyoto-hash', dest='kyoto_hash', action='store_true')
    opt('-k', '--kyoto', dest='kyoto', action='store_true')
    opt('-l', '--lsm', dest='lsm', action='store_true')
    opt('-m', '--minimal', dest='minimal', action='store_true')
    opt('-r', '--rocksdb', dest='rocksdb', action='store_true')
    opt('-T', '--kyoto-tree', dest='kyoto_tree', action='store_true')
    opt('-v', '--leveldb', dest='leveldb', action='store_true')

    options, args = parser.parse_args()
    cases = set()
    if options.minimal:
        cases = ['TreeTests']
    else:
        if options.berkeleydb:
            cases.add('BerkeleyDBTests')
        if options.kyoto:
            cases.update(('HashTests', 'TreeTests', 'CacheHashTests',
                          'CacheTreeTests'))
        if options.kyoto_hash:
            cases.update(('HashTests', 'CacheHashTests'))
        if options.kyoto_tree:
            cases.update(('TreeTests', 'CacheTreeTests'))
        if options.leveldb:
            cases.add('LevelDBTests')
        if options.lsm:
            cases.add('LSMTests')
        if options.rocksdb:
            cases.add('RocksDBTests')

    cases = ['kvkit.tests.%s' % case for case in sorted(cases)]
    runtests(cases)
