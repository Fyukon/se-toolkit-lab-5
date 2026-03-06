"""Router for the ETL pipeline endpoint."""

from fastapi import APIRouter, Depends, HTTPException, status
from sqlmodel.ext.asyncio.session import AsyncSession

from app.database import get_session
from app.etl import ETLEXtractException, ETLLoadException, sync

router = APIRouter()


@router.post("/sync")
async def post_sync(session: AsyncSession = Depends(get_session)):
    """Trigger a data sync from the autochecker API.

    Fetches the latest items and logs, loads them into the database,
    and returns a summary of what was synced.

    Returns:
        Dict with new_records, total_records, and new_items counts.

    Raises:
        HTTPException: If the sync fails (500 for server errors, 502 for API errors).
    """
    try:
        return await sync(session)
    except ETLEXtractException as e:
        # API extraction failed (e.g., 401 Unauthorized, 404, etc.)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Failed to fetch data from external API: {str(e)}",
        ) from e
    except ETLLoadException as e:
        # Database load failed
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to load data into database: {str(e)}",
        ) from e
    except Exception as e:
        # Unexpected error
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Unexpected error during sync: {str(e)}",
        ) from e
