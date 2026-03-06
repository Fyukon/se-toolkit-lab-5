"""ETL pipeline: fetch data from the autochecker API and load it into the database.

The autochecker dashboard API provides two endpoints:
- GET /api/items — lab/task catalog
- GET /api/logs  — anonymized check results (supports ?since= and ?limit= params)

Both require HTTP Basic Auth (email + password from settings).
"""

from datetime import datetime

import httpx
from sqlmodel.ext.asyncio.session import AsyncSession

from app.settings import settings


class ETLException(Exception):
    """Base exception for ETL pipeline errors."""

    pass


class ETLEXtractException(ETLException):
    """Raised when extraction from API fails."""

    pass


class ETLLoadException(ETLException):
    """Raised when loading to database fails."""

    pass


# ---------------------------------------------------------------------------
# Extract — fetch data from the autochecker API
# ---------------------------------------------------------------------------


async def fetch_items() -> list[dict]:
    """Fetch the lab/task catalog from the autochecker API.

    Returns:
        List of item dicts with keys: lab, task, title, type.

    Raises:
        ETLEXtractException: If the API request fails.
    """
    url = f"{settings.autochecker_api_url}/api/items"
    auth = (settings.autochecker_email, settings.autochecker_password)

    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url, auth=auth)
            response.raise_for_status()
            return response.json()
    except httpx.HTTPStatusError as e:
        raise ETLEXtractException(
            f"Failed to fetch items: {e.response.status_code} {e.response.text}"
        ) from e
    except httpx.RequestError as e:
        raise ETLEXtractException(f"Request failed: {e}") from e


async def fetch_logs(since: datetime | None = None) -> list[dict]:
    """Fetch check results from the autochecker API.

    Args:
        since: Optional timestamp to fetch logs since (for incremental sync).

    Returns:
        Combined list of all log dicts from all pages.

    Raises:
        ETLEXtractException: If the API request fails.
    """
    url = f"{settings.autochecker_api_url}/api/logs"
    auth = (settings.autochecker_email, settings.autochecker_password)
    all_logs: list[dict] = []

    try:
        async with httpx.AsyncClient() as client:
            while True:
                params: dict = {"limit": 500}
                if since is not None:
                    params["since"] = since.isoformat()

                response = await client.get(url, auth=auth, params=params)
                response.raise_for_status()
                data = response.json()

                logs = data.get("logs", [])
                all_logs.extend(logs)

                has_more = data.get("has_more", False)
                if not has_more or not logs:
                    break

                last_log = logs[-1]
                since = datetime.fromisoformat(
                    last_log["submitted_at"].replace("Z", "+00:00")
                )

        return all_logs
    except httpx.HTTPStatusError as e:
        raise ETLEXtractException(
            f"Failed to fetch logs: {e.response.status_code} {e.response.text}"
        ) from e
    except httpx.RequestError as e:
        raise ETLEXtractException(f"Request failed: {e}") from e
    except (KeyError, ValueError) as e:
        raise ETLEXtractException(f"Invalid response format: {e}") from e


# ---------------------------------------------------------------------------
# Load — insert fetched data into the local database
# ---------------------------------------------------------------------------


async def load_items(items: list[dict], session: AsyncSession) -> int:
    """Load items (labs and tasks) into the database.

    Args:
        items: Raw item dicts from the API.
        session: Database session.

    Returns:
        Number of newly created items.
    """
    from app.models.item import ItemRecord
    from sqlmodel import select

    new_count = 0
    lab_map: dict[str, ItemRecord] = {}

    # Process labs first (type="lab")
    for item in items:
        if item.get("type") != "lab":
            continue

        title = item["title"]
        # Check if lab already exists
        stmt = select(ItemRecord).where(
            ItemRecord.type == "lab", ItemRecord.title == title
        )
        result = await session.exec(stmt)
        lab_record = result.first()  # Use first() instead of one_or_none()

        if lab_record is None:
            lab_record = ItemRecord(type="lab", title=title)
            session.add(lab_record)
            new_count += 1

        # Map lab short ID (e.g., "lab-01") to the record
        lab_short_id = item["lab"]
        lab_map[lab_short_id] = lab_record

    # Process tasks (type="task")
    for item in items:
        if item.get("type") != "task":
            continue

        lab_short_id = item["lab"]
        task_title = item["title"]
        parent_lab = lab_map.get(lab_short_id)

        if parent_lab is None:
            # Parent lab not found, skip this task
            continue

        # Check if task already exists with this title and parent_id
        stmt = select(ItemRecord).where(
            ItemRecord.type == "task",
            ItemRecord.title == task_title,
            ItemRecord.parent_id == parent_lab.id,
        )
        result = await session.exec(stmt)
        task_record = result.first()  # Use first() instead of one_or_none()

        if task_record is None:
            task_record = ItemRecord(
                type="task", title=task_title, parent_id=parent_lab.id
            )
            session.add(task_record)
            new_count += 1

    await session.commit()
    return new_count


async def load_logs(
    logs: list[dict], items_catalog: list[dict], session: AsyncSession
) -> int:
    """Load interaction logs into the database.

    Args:
        logs: Raw log dicts from the API.
        items_catalog: Raw item dicts from fetch_items() for mapping short IDs to titles.
        session: Database session.

    Returns:
        Number of newly created interactions.
    """
    from app.models.interaction import InteractionLog
    from app.models.item import ItemRecord
    from app.models.learner import Learner
    from sqlmodel import select

    # Build lookup: (lab_short_id, task_short_id_or_none) -> item title
    item_title_lookup: dict[tuple[str, str | None], str] = {}
    for item in items_catalog:
        lab_short_id = item["lab"]
        task_short_id = item.get("task")  # None for labs
        title = item["title"]
        item_title_lookup[(lab_short_id, task_short_id)] = title

    new_count = 0

    for log in logs:
        # 1. Find or create Learner
        external_id = log["student_id"]
        stmt = select(Learner).where(Learner.external_id == external_id)
        result = await session.exec(stmt)
        learner = result.first()  # Use first() to handle duplicates

        if learner is None:
            learner = Learner(
                external_id=external_id, student_group=log.get("group", "")
            )
            session.add(learner)
            await session.flush()  # Get learner.id

        # 2. Find the matching item
        lab_short_id = log["lab"]
        task_short_id = log.get("task")  # Can be None
        item_title = item_title_lookup.get((lab_short_id, task_short_id))

        if item_title is None:
            # No matching item found, skip this log
            continue

        stmt = select(ItemRecord).where(ItemRecord.title == item_title)
        result = await session.exec(stmt)
        item_record = result.first()  # Use first() to handle duplicates

        if item_record is None:
            # Item not in database, skip
            continue

        # 3. Check if InteractionLog already exists (idempotent upsert)
        log_external_id = log["id"]
        stmt = select(InteractionLog).where(
            InteractionLog.external_id == log_external_id
        )
        result = await session.exec(stmt)
        existing_interaction = result.first()  # Use first() to handle duplicates

        if existing_interaction is not None:
            # Already exists, skip
            continue

        # 4. Create InteractionLog
        interaction = InteractionLog(
            external_id=log_external_id,
            learner_id=learner.id,
            item_id=item_record.id,
            kind="attempt",
            score=log.get("score"),
            checks_passed=log.get("passed"),
            checks_total=log.get("total"),
            created_at=datetime.fromisoformat(
                log["submitted_at"].replace("Z", "+00:00")
            ),
        )
        session.add(interaction)
        new_count += 1

    await session.commit()
    return new_count


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


async def sync(session: AsyncSession) -> dict:
    """Run the full ETL pipeline.

    Args:
        session: Database session.

    Returns:
        Dict with new_records, total_records, and new_items counts.
    """
    from app.models.interaction import InteractionLog
    from sqlmodel import select

    # Step 1: Fetch items from the API and load them
    raw_items = await fetch_items()
    new_items_count = await load_items(raw_items, session)

    # Step 2: Determine the last synced timestamp
    stmt = select(InteractionLog).order_by(InteractionLog.created_at.desc()).limit(1)
    result = await session.exec(stmt)
    last_interaction = result.one_or_none()

    since = last_interaction.created_at if last_interaction else None

    # Step 3: Fetch logs since that timestamp and load them
    raw_logs = await fetch_logs(since=since)
    new_logs_count = await load_logs(raw_logs, raw_items, session)

    # Get total records count
    total_stmt = select(InteractionLog)
    total_result = await session.exec(total_stmt)
    total_records = len(total_result.all())

    return {
        "new_records": new_logs_count,
        "total_records": total_records,
        "new_items": new_items_count,
    }
