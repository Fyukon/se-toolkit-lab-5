"""Router for analytics endpoints.

Each endpoint performs SQL aggregation queries on the interaction data
populated by the ETL pipeline. All endpoints require a `lab` query
parameter to filter results by lab (e.g., "lab-01").
"""

from fastapi import APIRouter, Depends, Query
from sqlalchemy import case, func, distinct
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.database import get_session
from app.models.interaction import InteractionLog
from app.models.item import ItemRecord
from app.models.learner import Learner

router = APIRouter()


async def _get_lab_and_task_ids(session: AsyncSession, lab: str) -> tuple[int, list[int]]:
    """Get lab ID and list of task IDs for a given lab identifier.
    
    Args:
        session: Database session.
        lab: Lab identifier (e.g., "lab-04").
    
    Returns:
        Tuple of (lab_id, list_of_task_ids).
    """
    # Convert "lab-04" to "Lab 04" for title matching
    lab_title_pattern = f"%{lab.replace('-', ' ').title()}%"
    
    # Find the lab
    lab_stmt = select(ItemRecord).where(
        ItemRecord.type == "lab",
        ItemRecord.title.ilike(lab_title_pattern)
    )
    lab_result = await session.exec(lab_stmt)
    lab_item = lab_result.first()
    
    if not lab_item:
        return (0, [])
    
    # Find all tasks belonging to this lab
    task_stmt = select(ItemRecord).where(
        ItemRecord.type == "task",
        ItemRecord.parent_id == lab_item.id
    )
    task_result = await session.exec(task_stmt)
    task_ids = [task.id for task in task_result.all()]
    
    return (lab_item.id, task_ids)


@router.get("/scores")
async def get_scores(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Score distribution histogram for a given lab.
    
    Returns:
        JSON array with four buckets: [{"bucket": "0-25", "count": N}, ...]
    """
    # Get lab and task IDs
    lab_id, task_ids = await _get_lab_and_task_ids(session, lab)
    
    if not task_ids:
        # Return empty buckets
        return [
            {"bucket": "0-25", "count": 0},
            {"bucket": "26-50", "count": 0},
            {"bucket": "51-75", "count": 0},
            {"bucket": "76-100", "count": 0},
        ]
    
    # Query interactions and bucket them using CASE
    stmt = select(
        case(
            (InteractionLog.score <= 25, "0-25"),
            (InteractionLog.score <= 50, "26-50"),
            (InteractionLog.score <= 75, "51-75"),
            else_="76-100"
        ).label("bucket"),
        func.count(InteractionLog.id).label("count")
    ).where(
        InteractionLog.item_id.in_(task_ids),
        InteractionLog.score.isnot(None)
    ).group_by(
        case(
            (InteractionLog.score <= 25, "0-25"),
            (InteractionLog.score <= 50, "26-50"),
            (InteractionLog.score <= 75, "51-75"),
            else_="76-100"
        )
    )
    
    result = await session.exec(stmt)
    rows = result.all()
    
    # Build result with all buckets
    bucket_counts = {"0-25": 0, "26-50": 0, "51-75": 0, "76-100": 0}
    for row in rows:
        bucket_counts[row.bucket] = row.count
    
    return [
        {"bucket": "0-25", "count": bucket_counts["0-25"]},
        {"bucket": "26-50", "count": bucket_counts["26-50"]},
        {"bucket": "51-75", "count": bucket_counts["51-75"]},
        {"bucket": "76-100", "count": bucket_counts["76-100"]},
    ]


@router.get("/pass-rates")
async def get_pass_rates(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-task pass rates for a given lab.

    Returns:
        JSON array: [{"task": "Repository Setup", "avg_score": 92.3, "attempts": 150}, ...]
    """
    # Get lab and task IDs
    lab_id, task_ids = await _get_lab_and_task_ids(session, lab)

    if not task_ids:
        return []

    # Query: for each task, compute avg_score and attempts
    stmt = (
        select(
            ItemRecord.title.label("task"),
            func.round(func.avg(InteractionLog.score), 1).label("avg_score"),
            func.count(InteractionLog.id).label("attempts"),
        )
        .join(InteractionLog, InteractionLog.item_id == ItemRecord.id)
        .where(ItemRecord.id.in_(task_ids))
        .group_by(ItemRecord.id, ItemRecord.title)
        .order_by(ItemRecord.title)
    )

    result = await session.exec(stmt)
    rows = result.all()

    return [
        {"task": row.task, "avg_score": float(row.avg_score) if row.avg_score else 0.0, "attempts": row.attempts}
        for row in rows
    ]


@router.get("/timeline")
async def get_timeline(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Submissions per day for a given lab.

    Returns:
        JSON array: [{"date": "2026-02-28", "submissions": 45}, ...]
    """
    # Get lab and task IDs
    lab_id, task_ids = await _get_lab_and_task_ids(session, lab)

    if not task_ids:
        return []

    # Query: group interactions by date using DATE() function
    # Use func.date() for SQLite and PostgreSQL compatibility
    date_expr = func.date(InteractionLog.created_at)
    
    stmt = (
        select(
            date_expr.label("date"),
            func.count(InteractionLog.id).label("submissions"),
        )
        .where(InteractionLog.item_id.in_(task_ids))
        .group_by(date_expr)
        .order_by(date_expr)
    )

    result = await session.exec(stmt)
    rows = result.all()

    return [
        {"date": str(row.date), "submissions": row.submissions}
        for row in rows
    ]


@router.get("/groups")
async def get_groups(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-group performance for a given lab.

    Returns:
        JSON array: [{"group": "B23-CS-01", "avg_score": 78.5, "students": 25}, ...]
    """
    # Get lab and task IDs
    lab_id, task_ids = await _get_lab_and_task_ids(session, lab)

    if not task_ids:
        return []

    # Query: join interactions with learners, group by student_group
    stmt = (
        select(
            Learner.student_group.label("group"),
            func.round(func.avg(InteractionLog.score), 1).label("avg_score"),
            func.count(distinct(Learner.id)).label("students"),
        )
        .join(InteractionLog, InteractionLog.learner_id == Learner.id)
        .where(InteractionLog.item_id.in_(task_ids))
        .group_by(Learner.student_group)
        .order_by(Learner.student_group)
    )

    result = await session.exec(stmt)
    rows = result.all()

    return [
        {
            "group": row.group,
            "avg_score": float(row.avg_score) if row.avg_score else 0.0,
            "students": row.students,
        }
        for row in rows
    ]
