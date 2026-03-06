#!/usr/bin/env python3
"""Test script for the full ETL pipeline."""

import asyncio
import sys
from pathlib import Path

# Add backend to path
backend_dir = str(Path(__file__).parent / "backend")
if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir)

from app.etl import fetch_items, fetch_logs, load_items, load_logs, sync


async def test_fetch_items():
    """Test fetch_items function."""
    print("\n=== Testing fetch_items() ===")
    try:
        items = await fetch_items()
        print(f"✓ Success! Fetched {len(items)} items")
        if items:
            print("\nFirst 3 items:")
            for item in items[:3]:
                print(f"  - type={item.get('type')}, lab={item.get('lab')}, title={item.get('title')}")
        return True
    except Exception as e:
        print(f"✗ Error: {type(e).__name__}: {e}")
        return False


async def test_fetch_logs():
    """Test fetch_logs function."""
    print("\n=== Testing fetch_logs() ===")
    try:
        logs = await fetch_logs(since=None)
        print(f"✓ Success! Fetched {len(logs)} logs")
        if logs:
            print("\nFirst log entry:")
            first = logs[0]
            print(f"  - id={first.get('id')}, lab={first.get('lab')}, student_id={first.get('student_id')}")
        return True
    except Exception as e:
        print(f"✗ Error: {type(e).__name__}: {e}")
        return False


async def test_full_sync():
    """Test full ETL sync."""
    print("\n=== Testing full sync() ===")
    try:
        from app.database import engine
        from sqlmodel.ext.asyncio.session import AsyncSession

        async with AsyncSession(engine) as session:
            result = await sync(session)
            print(f"✓ Sync completed!")
            print(f"  - New items: {result.get('new_items', 'N/A')}")
            print(f"  - New records: {result.get('new_records', 'N/A')}")
            print(f"  - Total records: {result.get('total_records', 'N/A')}")
        return True
    except Exception as e:
        print(f"✗ Error: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return False


async def main():
    """Run all tests."""
    print("=" * 60)
    print("ETL Pipeline Test Suite")
    print("=" * 60)

    results = {
        "fetch_items": await test_fetch_items(),
        "fetch_logs": await test_fetch_logs(),
        "full_sync": await test_full_sync(),
    }

    print("\n" + "=" * 60)
    print("Summary:")
    for test, passed in results.items():
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"  {status}: {test}")
    print("=" * 60)

    return 0 if all(results.values()) else 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
