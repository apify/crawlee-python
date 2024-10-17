# Dataset
Datasets are used to store structured data where each object stored has the same attributes, such as online store products or real estate offers. Dataset can be imagined as a table, where each object is a row and its attributes are columns. Dataset is an append-only storage - we can only add new records to it, but we cannot modify or remove existing records.

Each Crawlee project run is associated with a default dataset. Typically, it is used to store crawling results specific for the crawler run. Its usage is optional.

In Crawlee, the dataset is represented by the `Dataset` class. In order to simplify writes to the default dataset, Crawlee also provides the `Dataset.pushData()` function.

The data is stored in the directory specified by the `CRAWLEE_STORAGE_DIR` environment variable as follows:

`{CRAWLEE_STORAGE_DIR}/datasets/{DATASET_ID}/{INDEX}.json`

`{DATASET_ID}` is the name or the ID of the dataset. The default dataset has ID default, unless we override it by setting the `CRAWLEE_DEFAULT_DATASET_ID` environment variable. Each dataset item is stored as a separate JSON file, where `{INDEX}` is a zero-based index of the item in the dataset.

The following code demonstrates basic operations of the dataset:

``` javascript
import { Dataset } from 'crawlee';

// Write a single row to the default dataset
await Dataset.pushData({ col1: 123, col2: 'val2' });

// Open a named dataset
const dataset = await Dataset.open('some-name');

// Write a single row
await dataset.pushData({ foo: 'bar' });

// Write multiple rows
await dataset.pushData([{ foo: 'bar2', col2: 'val2' }, { col3: 123 }]);
```

# Key-value store

The key-value store is used for saving and reading data records or files. Each data record is represented by a unique key and associated with a MIME content type. Key-value stores are ideal for saving screenshots of web pages, PDFs or to persist the state of crawlers.

Each Crawlee project run is associated with a default key-value store. By convention, the project input and output are stored in the default key-value store under the `INPUT` and `OUTPUT` keys respectively. Typically, both input and output are JSON files, although they could be any other format.

In Crawlee, the key-value store is represented by the `KeyValueStore` class. In order to simplify access to the default key-value store, Crawlee also provides `KeyValueStore.getValue()` and `KeyValueStore.setValue()` functions.

The data is stored in the directory specified by the CRAWLEE_STORAGE_DIR environment variable as follows:
`{CRAWLEE_STORAGE_DIR}/key_value_stores/{STORE_ID}/{KEY}.{EXT}`

`{STORE_ID}` is the name or the ID of the key-value store. The default key-value store has ID default, unless we override it by setting the CRAWLEE_DEFAULT_KEY_VALUE_STORE_ID environment variable. The `{KEY}` is the key of the record and `{EXT}` corresponds to the MIME content type of the data value.

``` javascript
import { KeyValueStore } from 'crawlee';

// Get the INPUT from the default key-value store
const input = await KeyValueStore.getInput();

// Write the OUTPUT to the default key-value store
await KeyValueStore.setValue('OUTPUT', { myResult: 123 });

// Open a named key-value store
const store = await KeyValueStore.open('some-name');

// Write a record to the named key-value store.
// JavaScript object is automatically converted to JSON,
// strings and binary buffers are stored as they are
await store.setValue('some-key', { foo: 'bar' });

// Read a record from the named key-value store.
// Note that JSON is automatically parsed to a JavaScript object,
// text data is returned as a string, and other data is returned as binary buffer
const value = await store.getValue('some-key');

// Delete a record from the named key-value store
await store.setValue('some-key', null);
```

