from crawlee._utils.try_import import install_import_hook as _install_import_hook
from crawlee._utils.try_import import try_import as _try_import

# These imports have only mandatory dependencies, so they are imported directly.
from ._base import StorageBackend
from ._file_system import FileSystemStorageBackend
from ._memory import MemoryStorageBackend

_install_import_hook(__name__)

# The following imports are wrapped in try_import to handle optional dependencies,
# ensuring the module can still function even if these dependencies are missing.
with _try_import(__name__, 'SqlStorageBackend', extra_name=['sql_sqlite', 'sql_postgres', 'sql_mysql']):
    from ._sql import SqlStorageBackend

with _try_import(__name__, 'RedisStorageBackend', extra_name='redis'):
    from ._redis import RedisStorageBackend

__all__ = [
    'FileSystemStorageBackend',
    'MemoryStorageBackend',
    'RedisStorageBackend',
    'SqlStorageBackend',
    'StorageBackend',
]
