from crawlee._utils.try_import import install_import_hook as _install_import_hook
from crawlee._utils.try_import import try_import as _try_import

# These imports have only mandatory dependencies, so they are imported directly.
from ._base import StorageClient
from ._file_system import FileSystemStorageClient
from ._memory import MemoryStorageClient

_install_import_hook(__name__)

# The following imports are wrapped in try_import to handle optional dependencies,
# ensuring the module can still function even if these dependencies are missing.
with _try_import(__name__, 'SqlStorageClient'):
    from ._sql import SqlStorageClient

with _try_import(__name__, 'RedisStorageClient'):
    from ._redis import RedisStorageClient

__all__ = [
    'FileSystemStorageClient',
    'MemoryStorageClient',
    'RedisStorageClient',
    'SqlStorageClient',
    'StorageClient',
]
