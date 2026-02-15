from __future__ import annotations

from fastapi import APIRouter, Request

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
    data = _admin_service.get_overview_stats()
    return _ok(data=data, message="admin statistics fetched")

