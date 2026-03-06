#!/usr/bin/env python3
"""Clean up duplicate items from the database."""

import asyncio
import sys
from pathlib import Path

backend_dir = str(Path(__file__).parent / "backend")
if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir)

from app.database import engine
from app.models.item import ItemRecord
from app.models.learner import Learner
from app.models.interaction import InteractionLog
from sqlmodel import select, delete
from sqlmodel.ext.asyncio.session import AsyncSession


async def cleanup():
    """Remove duplicate items, keeping the oldest."""
    async with AsyncSession(engine) as session:
        # Clean duplicate ItemRecords (labs and tasks)
        print("Cleaning duplicate ItemRecords...")
        
        # Get all distinct type/title combinations
        stmt = select(ItemRecord.type, ItemRecord.title, ItemRecord.parent_id)
        result = await session.exec(stmt)
        rows = result.all()
        
        seen = set()
        duplicates = []
        
        for row in rows:
            key = (row.type, row.title, row.parent_id)
            if key in seen:
                duplicates.append(key)
            else:
                seen.add(key)
        
        # Remove duplicates (keep first occurrence)
        removed = 0
        for key in duplicates:
            stmt = select(ItemRecord).where(
                ItemRecord.type == key[0],
                ItemRecord.title == key[1],
            )
            if key[2] is not None:
                stmt = stmt.where(ItemRecord.parent_id == key[2])
            
            result = await session.exec(stmt)
            items = result.all()
            
            # Keep the first one, delete the rest
            for item in items[1:]:
                await session.delete(item)
                removed += 1
        
        if removed > 0:
            print(f"  Removed {removed} duplicate ItemRecords")
        else:
            print("  No duplicates found")
        
        await session.commit()
        
        # Clean duplicate Learners
        print("Cleaning duplicate Learners...")
        stmt = select(Learner.external_id)
        result = await session.exec(stmt)
        external_ids = result.all()
        
        seen = set()
        dup_ids = []
        for eid in external_ids:
            if eid in seen:
                dup_ids.append(eid)
            else:
                seen.add(eid)
        
        removed = 0
        for eid in dup_ids:
            stmt = delete(Learner).where(Learner.external_id == eid)
            await session.exec(stmt)
            removed += 1
        
        if removed > 0:
            print(f"  Removed {removed} duplicate Learners")
        
        await session.commit()
        
        # Clean duplicate InteractionLogs
        print("Cleaning duplicate InteractionLogs...")
        stmt = select(InteractionLog.external_id)
        result = await session.exec(stmt)
        log_ids = result.all()
        
        seen = set()
        dup_log_ids = []
        for lid in log_ids:
            if lid in seen:
                dup_log_ids.append(lid)
            else:
                seen.add(lid)
        
        removed = 0
        for lid in dup_log_ids:
            stmt = delete(InteractionLog).where(InteractionLog.external_id == lid)
            await session.exec(stmt)
            removed += 1
        
        if removed > 0:
            print(f"  Removed {removed} duplicate InteractionLogs")
        
        await session.commit()
        
        print("✓ Cleanup complete!")


if __name__ == "__main__":
    asyncio.run(cleanup())
