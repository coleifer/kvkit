from kvkit.exceptions import DatabaseError

try:
    from kvkit.backends.berkeleydb import BerkeleyDB
except ImportError:
    pass

try:
    from kvkit.backends.kyoto import CacheHashDB
    from kvkit.backends.kyoto import CacheTreeDB
    from kvkit.backends.kyoto import DirectoryHashDB
    from kvkit.backends.kyoto import DirectoryTreeDB
    from kvkit.backends.kyoto import HashDB
    from kvkit.backends.kyoto import PlainTextDB
    from kvkit.backends.kyoto import PrototypeHashDB
    from kvkit.backends.kyoto import PrototypeTreeDB
    from kvkit.backends.kyoto import StashDB
    from kvkit.backends.kyoto import TreeDB
except ImportError:
    pass

try:
    from kvkit.backends.leveldb import LevelDB
except ImportError:
    pass

try:
    from kvkit.backends.rocks import RocksDB
except ImportError:
    pass

try:
    from kvkit.backends.sqlite4 import LSM
except ImportError:
    pass

from kvkit.query import DateField
from kvkit.query import DateTimeField
from kvkit.query import Field
from kvkit.query import FloatField
from kvkit.query import LongField
from kvkit.query import Model


__version__ = '0.1.0'
