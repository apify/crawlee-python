import asyncio

from crawlee.storages import Dataset


async def main() -> None:
    # Open the dataset, if it does not exist, it will be created.
    # Leave name empty to use the default dataset.
    dataset = await Dataset.open(name='my-dataset')

    # Push a single row of data.
    await dataset.push_data({'foo': 'bar'})

    # Push multiple rows of data (anything JSON-serializable can be pushed).
    await dataset.push_data([{'foo': 'bar2', 'col2': 'val2'}, {'col3': 123}])

    # Fetch all data from the dataset.
    data = await dataset.get_data()
    # Do something with it...

    # Remove the dataset.
    await dataset.drop()


if __name__ == '__main__':
    asyncio.run(main())
