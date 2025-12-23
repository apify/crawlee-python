"""Quick test script to verify metrics.jsonl generation."""

import json
import sys
from pathlib import Path

# Add tax_rag_project to path
sys.path.insert(0, str(Path(__file__).parent / 'tax_rag_project' / 'src'))

from tax_rag_scraper.utils.stats_tracker import CrawlStats


def test_metrics_generation():
    """Test that metrics can be generated and written."""
    print("Testing metrics generation...")

    # Create a mock stats object
    stats = CrawlStats()
    stats.urls_processed = 15
    stats.urls_failed = 2
    stats.documents_extracted = 13

    # Generate JSONL
    metrics_line = stats.to_jsonl(crawl_type='test')
    print(f"\nGenerated metrics JSONL:\n{metrics_line}\n")

    # Verify it's valid JSON
    try:
        metrics_data = json.loads(metrics_line)
        print("✓ Valid JSON format")
        print(f"✓ Contains fields: {list(metrics_data.keys())}")

        # Check required fields
        required_fields = ['timestamp', 'start_time', 'crawl_type', 'duration_seconds',
                          'urls_processed', 'urls_failed', 'success_rate', 'documents_extracted']
        missing_fields = [f for f in required_fields if f not in metrics_data]

        if missing_fields:
            print(f"✗ Missing fields: {missing_fields}")
            return False
        else:
            print(f"✓ All required fields present")

        # Verify values
        assert metrics_data['urls_processed'] == 15
        assert metrics_data['urls_failed'] == 2
        assert metrics_data['documents_extracted'] == 13
        assert metrics_data['crawl_type'] == 'test'
        assert abs(metrics_data['success_rate'] - 86.67) < 0.01
        print("✓ All values correct")

        print("\n✓ Metrics generation test PASSED!")
        return True

    except json.JSONDecodeError as e:
        print(f"✗ Invalid JSON: {e}")
        return False
    except AssertionError as e:
        print(f"✗ Value assertion failed: {e}")
        return False


def test_file_writing():
    """Test that metrics can be written to file."""
    print("\n" + "="*50)
    print("Testing file writing...")

    test_dir = Path(__file__).parent / 'tax_rag_project' / 'storage' / 'datasets' / 'default'
    test_dir.mkdir(parents=True, exist_ok=True)

    test_file = test_dir / 'test_metrics.jsonl'

    # Clean up any existing test file
    if test_file.exists():
        test_file.unlink()

    # Create stats and write
    stats = CrawlStats()
    stats.urls_processed = 10
    stats.urls_failed = 1
    stats.documents_extracted = 9

    metrics_line = stats.to_jsonl(crawl_type='test-write')

    with test_file.open('a') as f:
        f.write(metrics_line + '\n')

    print(f"✓ File written to: {test_file}")

    # Verify file exists and is readable
    if test_file.exists():
        with test_file.open('r') as f:
            content = f.read()
            print(f"✓ File content:\n{content}")

        # Clean up
        test_file.unlink()
        print("✓ Test file cleaned up")

        print("\n✓ File writing test PASSED!")
        return True
    else:
        print("✗ File was not created")
        return False


if __name__ == '__main__':
    success = test_metrics_generation() and test_file_writing()

    if success:
        print("\n" + "="*50)
        print("ALL TESTS PASSED ✓")
        print("="*50)
        sys.exit(0)
    else:
        print("\n" + "="*50)
        print("SOME TESTS FAILED ✗")
        print("="*50)
        sys.exit(1)
