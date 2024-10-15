import asyncio
from crawlee.storages import Dataset

async def main() -> None:
    dataset = await Dataset.open(name='my-dataset')

    # Add an item to the dataset
    await dataset.push_data({'name': 'John Doe', 'age': 30})

    # Add multiple items to the dataset
    await dataset.push_data([
        {'name': 'Jane Doe', 'age': 25},
        {'name': 'Alice', 'age': 27},
    ])

    # Retrieve all data from the dataset
    async for item in dataset.iter_data():
        print(item)

    # Remove the dataset
    await dataset.drop()

if __name__ == '__main__':
    asyncio.run(main())