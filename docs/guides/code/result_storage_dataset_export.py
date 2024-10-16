import asyncio
from crawlee.storages import Dataset

async def main() -> None:
    # Open a dataset named 'my-dataset'
    dataset = await Dataset.open(name='my-dataset')

    # Export the entire dataset to a JSON file
    await dataset.export_to({
        'target_key': 'my_dataset_json',  # Key under which to save in the key-value store
        'content_type': 'application/json',  # Specify the content type for JSON
        'filename': './output/my-dataset.json'  # Specify the filename for export
    })

    # You can also export to other formats by changing the content_type accordingly
    # For example, to export as CSV:
    await dataset.export_to({
        'target_key': 'my_dataset_csv',  # Key for CSV
        'content_type': 'text/csv',  # Specify content type for CSV
        'filename': './output/my-dataset.csv'  # Specify the filename for export
    })

if __name__ == '__main__':
    asyncio.run(main())
