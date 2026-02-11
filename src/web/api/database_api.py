from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, HTTPException, Request

from ..entity.request import DatabaseCreateRequest, DatabaseUpdateRequest
from ..service import DatabaseService, UserService


router = APIRouter(prefix="/databases", tags=["databases"])
_database_service = DatabaseService()
_user_service = UserService()
SESSION_COOKIE_KEY = "spatial_session_id"


def _ok(data=None, message: str = "ok"):
    return {
        "success": True,
        "message": message,
        "data": data,
    }


def _assert_login(request: Request) -> int:
    session_id = request.cookies.get(SESSION_COOKIE_KEY)
    if not session_id:
        raise HTTPException(status_code=401, detail="Not logged in.")
    payload = _user_service.get_session(session_id=session_id)
    if not payload:
        raise HTTPException(status_code=401, detail="Not logged in.")
    user = payload.get("user") or {}
    user_id = user.get("id")
    if user_id is None:
        raise HTTPException(status_code=401, detail="Invalid session.")
    return int(user_id)


@router.post("")
def insert_database(body: DatabaseCreateRequest, request: Request):
    current_user_id = _assert_login(request)
    if current_user_id != body.user_id:
        raise HTTPException(status_code=403, detail="Can only insert database links for current user.")
    try:
        data = _database_service.insert_database(
            user_id=body.user_id,
            db_type=body.type,
            url=body.url,
            schema=body.schema_list,
            db_username=body.db_username,
            db_password=body.db_password,
        )
        return _ok(data=data, message="database link inserted")
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.put("/{link_id}")
def update_database(link_id: int, body: DatabaseUpdateRequest, request: Request):
    _assert_login(request)
    try:
        data = _database_service.update_database(
            link_id=link_id,
            db_type=body.type,
            url=body.url,
            schema=body.schema_list,
            db_username=body.db_username,
            db_password=body.db_password,
        )
        return _ok(data=data, message="database link updated")
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.delete("/{link_id}")
def delete_database(link_id: int, request: Request):
    _assert_login(request)
    deleted = _database_service.delete_database(link_id=link_id)
    if not deleted:
        raise HTTPException(status_code=404, detail=f"database link not found: link_id={link_id}")
    return _ok(data={"deleted": True}, message="database link deleted")


@router.get("")
def list_databases(request: Request, user_id: Optional[int] = None):
    current_user_id = _assert_login(request)
    target_user_id = user_id if user_id is not None else current_user_id
    if int(target_user_id) != int(current_user_id):
        raise HTTPException(status_code=403, detail="Can only list current user's database links.")
    data = _database_service.list_databases(user_id=target_user_id)
    return _ok(data=data, message="database links listed")
