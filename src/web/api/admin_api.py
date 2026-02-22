from __future__ import annotations

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException, Request

from ..entity.response import AdminOverviewResponse
from ..service import AdminService
from utils.auth_guard import assert_admin_user


router = APIRouter(prefix="/admin", tags=["admin"])
_admin_service = AdminService()


def _ok(data=None, message: str = "ok"):
    return {
        "success": True,
        "message": message,
        "data": data,
    }


@router.get("/stats")
def get_admin_stats(request: Request):
    assert_admin_user(request)
    data = AdminOverviewResponse.from_dict(_admin_service.get_overview_stats()).to_dict()
    return _ok(data=data, message="admin statistics fetched")


@router.get("/sql-execution-logs")
def get_sql_execution_logs(
    request: Request,
    user_id: Optional[int] = None,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    query: Optional[str] = None,
    min_latency: int = 0,
    status: Optional[str] = None,
):
    assert_admin_user(request)
    try:
        data = _admin_service.list_sql_execution_logs(
            user_id=user_id,
            start_time=start_time,
            end_time=end_time,
            query=query,
            min_latency=min_latency,
            status=status,
        )
        return _ok(data=data, message="sql execution logs fetched")
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))
