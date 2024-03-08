from __future__ import annotations

from typing import TYPE_CHECKING, Any, AsyncIterator, NamedTuple, TypedDict, TypeVar, overload

from apify_client.clients import KeyValueStoreClientAsync, KeyValueStoreCollectionClientAsync
from apify_shared.utils import ignore_docs

from apify._utils import wrap_internal
from apify.storages.base_storage import BaseStorage

if TYPE_CHECKING:
    from apify_client import ApifyClientAsync

    from apify._memory_storage import MemoryStorageClient
    from apify._memory_storage.resource_clients import KeyValueStoreClient, KeyValueStoreCollectionClient
    from apify.config import Configuration


T = TypeVar('T')


class IterateKeysInfo(TypedDict):
    """Contains information about a key-value store record."""

    size: int


class IterateKeysTuple(NamedTuple):
    """A tuple representing a key-value store record."""

    key: str
    info: IterateKeysInfo


class KeyValueStore(BaseStorage):
    """The `KeyValueStore` class represents a key-value store.

    You can imagine it as a simple data storage that is used
    for saving and reading data records or files. Each data record is
    represented by a unique key and associated with a MIME content type.

    Do not instantiate this class directly, use the `Actor.open_key_value_store()` function instead.

    Each crawler run is associated with a default key-value store, which is created exclusively
    for the run. By convention, the crawler input and output are stored into the
    default key-value store under the `INPUT` and `OUTPUT` key, respectively.
    Typically, input and output are JSON files, although it can be any other format.
    To access the default key-value store directly, you can use the
    `KeyValueStore.get_value` and `KeyValueStore.set_value` convenience functions.

    `KeyValueStore` stores its data either on local disk or in the Apify cloud,
    depending on whether the `APIFY_LOCAL_STORAGE_DIR` or `APIFY_TOKEN` environment variables are set.

    If the `APIFY_LOCAL_STORAGE_DIR` environment variable is set, the data is stored in
    the local directory in the following files:
    ```
    {APIFY_LOCAL_STORAGE_DIR}/key_value_stores/{STORE_ID}/{INDEX}.{EXT}
    ```
    Note that `{STORE_ID}` is the name or ID of the key-value store. The default key-value store has ID: `default`,
    unless you override it by setting the `APIFY_DEFAULT_KEY_VALUE_STORE_ID` environment variable.
    The `{KEY}` is the key of the record and `{EXT}` corresponds to the MIME content type of the data value.

    If the `APIFY_TOKEN` environment variable is set but `APIFY_LOCAL_STORAGE_DIR` is not, the data is stored in the
    [Apify Key-value store](https://docs.apify.com/storage/key-value-store) cloud storage.
    """

    _id: str
    _name: str | None
    _key_value_store_client: KeyValueStoreClientAsync | KeyValueStoreClient

    @ignore_docs
    def __init__(
        self: KeyValueStore,
        id: str,  # noqa: A002
        name: str | None,
        client: ApifyClientAsync | MemoryStorageClient,
        config: Configuration,
    ) -> None:
        """Create a `KeyValueStore` instance.

        Do not use the constructor directly, use the `Actor.open_key_value_store()` function instead.

        Args:
            id (str): ID of the key-value store.
            name (str, optional): Name of the key-value store.
            client (ApifyClientAsync or MemoryStorageClient): The storage client which should be used.
            config (Configuration): The configuration which should be used.
        """
        super().__init__(id=id, name=name, client=client, config=config)

        self.get_value = wrap_internal(self._get_value_internal, self.get_value)  # type: ignore
        self.set_value = wrap_internal(self._set_value_internal, self.set_value)  # type: ignore
        self.get_public_url = wrap_internal(self._get_public_url_internal, self.get_public_url)  # type: ignore
        self._id = id
        self._name = name
        self._key_value_store_client = client.key_value_store(self._id)

    @classmethod
    def _get_human_friendly_label(cls: type[KeyValueStore]) -> str:
        return 'Key-value store'

    @classmethod
    def _get_default_id(cls: type[KeyValueStore], config: Configuration) -> str:
        return config.default_key_value_store_id

    @classmethod
    def _get_single_storage_client(
        cls: type[KeyValueStore],
        id: str,  # noqa: A002
        client: ApifyClientAsync | MemoryStorageClient,
    ) -> KeyValueStoreClientAsync | KeyValueStoreClient:
        return client.key_value_store(id)

    @classmethod
    def _get_storage_collection_client(
        cls: type[KeyValueStore],
        client: ApifyClientAsync | MemoryStorageClient,
    ) -> KeyValueStoreCollectionClientAsync | KeyValueStoreCollectionClient:
        return client.key_value_stores()

    @overload
    @classmethod
    async def get_value(cls: type[KeyValueStore], key: str) -> Any:
        ...

    @overload
    @classmethod
    async def get_value(cls: type[KeyValueStore], key: str, default_value: T) -> T:
        ...

    @overload
    @classmethod
    async def get_value(cls: type[KeyValueStore], key: str, default_value: T | None = None) -> T | None:
        ...

    @classmethod
    async def get_value(cls: type[KeyValueStore], key: str, default_value: T | None = None) -> T | None:
        """Get a value from the key-value store.

        Args:
            key (str): Key of the record to retrieve.
            default_value (Any, optional): Default value returned in case the record does not exist.

        Returns:
            Any: The value associated with the given key. `default_value` is used in case the record does not exist.
        """
        store = await cls.open()
        return await store.get_value(key, default_value)

    async def _get_value_internal(self: KeyValueStore, key: str, default_value: T | None = None) -> T | None:
        record = await self._key_value_store_client.get_record(key)
        return record['value'] if record else default_value

    async def iterate_keys(
        self: KeyValueStore,
        exclusive_start_key: str | None = None,
    ) -> AsyncIterator[IterateKeysTuple]:
        """Iterate over the keys in the key-value store.

        Args:
            exclusive_start_key (str, optional): All keys up to this one (including) are skipped from the result.

        Yields:
            IterateKeysTuple: A tuple `(key, info)`,
                where `key` is the record key, and `info` is an object that contains a single property `size`
                indicating size of the record in bytes.
        """
        while True:
            list_keys = await self._key_value_store_client.list_keys(exclusive_start_key=exclusive_start_key)
            for item in list_keys['items']:
                yield IterateKeysTuple(item['key'], {'size': item['size']})

            if not list_keys['isTruncated']:
                break
            exclusive_start_key = list_keys['nextExclusiveStartKey']

    @classmethod
    async def set_value(
        cls: type[KeyValueStore],
        key: str,
        value: Any,
        content_type: str | None = None,
    ) -> None:
        """Set or delete a value in the key-value store.

        Args:
            key (str): The key under which the value should be saved.
            value (Any): The value to save. If the value is `None`, the corresponding key-value pair will be deleted.
            content_type (str, optional): The content type of the saved value.
        """
        store = await cls.open()
        return await store.set_value(key, value, content_type)

    async def _set_value_internal(
        self: KeyValueStore,
        key: str,
        value: Any,
        content_type: str | None = None,
    ) -> None:
        if value is None:
            return await self._key_value_store_client.delete_record(key)

        return await self._key_value_store_client.set_record(key, value, content_type)

    @classmethod
    async def get_public_url(cls: type[KeyValueStore], key: str) -> str:
        """Get a URL for the given key that may be used to publicly access the value in the remote key-value store.

        Args:
            key (str): The key for which the URL should be generated.
        """
        store = await cls.open()
        return await store.get_public_url(key)

    async def _get_public_url_internal(self: KeyValueStore, key: str) -> str:
        if not isinstance(self._key_value_store_client, KeyValueStoreClientAsync):
            raise RuntimeError('Cannot generate a public URL for this key-value store as it is not on the Apify Platform!')  # noqa: TRY004

        public_api_url = self._config.api_public_base_url

        return f'{public_api_url}/v2/key-value-stores/{self._id}/records/{key}'

    async def drop(self: KeyValueStore) -> None:
        """Remove the key-value store either from the Apify cloud storage or from the local directory."""
        await self._key_value_store_client.delete()
        self._remove_from_cache()

    @classmethod
    async def open(
        cls: type[KeyValueStore],
        *,
        id: str | None = None,  # noqa: A002
        name: str | None = None,
        force_cloud: bool = False,
        config: Configuration | None = None,
    ) -> KeyValueStore:
        """Open a key-value store.

        Key-value stores are used to store records or files, along with their MIME content type.
        The records are stored and retrieved using a unique key.
        The actual data is stored either on a local filesystem or in the Apify cloud.

        Args:
            id (str, optional): ID of the key-value store to be opened.
                If neither `id` nor `name` are provided, the method returns the default key-value store associated with the actor run.
                If the key-value store with the given ID does not exist, it raises an error.
            name (str, optional): Name of the key-value store to be opened.
                If neither `id` nor `name` are provided, the method returns the default key-value store associated with the actor run.
                If the key-value store with the given name does not exist, it is created.
            force_cloud (bool, optional): If set to True, it will open a key-value store on the Apify Platform even when running the actor locally.
                Defaults to False.
            config (Configuration, optional): A `Configuration` instance, uses global configuration if omitted.

        Returns:
            KeyValueStore: An instance of the `KeyValueStore` class for the given ID or name.
        """
        return await super().open(id=id, name=name, force_cloud=force_cloud, config=config)  # type: ignore
