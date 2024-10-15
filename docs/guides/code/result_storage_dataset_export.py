import asyncio
from crawlee.storages import Dataset

async def main() -> None:
    dataset = await Dataset.open(name='my-dataset')

    # Export data to a JSON file
    await dataset.export_to_json('./output/my-dataset.json')

    # Export data to a CSV file
    await dataset.export_to_csv('./output/my-dataset.csv')

if __name__ == '__main__':
    asyncio.run(main())
