#!/usr/bin/env python3
"""Test script for fetch_items function."""

import asyncio
import sys
from pathlib import Path

# Add backend to path
backend_dir = str(Path(__file__).parent / "backend")
if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir)

from app.etl import fetch_items


async def main():
    """Test fetch_items."""
    print("Testing fetch_items()...")
    try:
        items = await fetch_items()
        print(f"✓ Success! Fetched {len(items)} items")
        if items:
            print("\nFirst 3 items:")
            for item in items[:3]:
                print(f"  - {item}")
    except Exception as e:
        print(f"✗ Error: {type(e).__name__}: {e}")
        return 1
    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
