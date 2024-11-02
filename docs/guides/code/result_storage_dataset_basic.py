import asyncio

# Import the Dataset class to manage data storage
from crawlee.storages import Dataset


async def main() -> None:
    # Open a default dataset using the asynchronous constructor.
    # This dataset is used to store data in the Crawlee storage.
    dataset = await Dataset.open()

    # Push a single row of data (a dictionary) to the named dataset.
    await dataset.push_data({'foo': 'bar'})

    # Push multiple rows of data (a list of dictionaries) to the named dataset.
    await dataset.push_data([{'foo': 'bar2', 'col2': 'val2'}, {'col3': 123}])


# Run the main function in an asynchronous event loop
if __name__ == '__main__':
    asyncio.run(main())
