from __future__ import annotations

from typing import Any, Dict

from fastapi import HTTPException, Request

from web.service.session_service import get_global_session_service


SESSION_COOKIE_KEY = "spatial_session_id"


def get_login_user(request: Request) -> Dict[str, Any]:
    session_id = request.cookies.get(SESSION_COOKIE_KEY)
    if not session_id:
        raise HTTPException(status_code=401, detail="Not logged in.")

    session_service = get_global_session_service()
    payload = session_service.get_session(session_id=session_id)
    if not payload:
        raise HTTPException(status_code=401, detail="Not logged in.")

    user = payload.get("user") or {}
    user_id = user.get("id")
    if user_id is None:
        raise HTTPException(status_code=401, detail="Invalid session.")

    role = str(user.get("role") or "user").strip().lower()
    if role not in {"user", "admin"}:
        role = "user"
    status = str(user.get("status") or "active").strip().lower()
    if status not in {"active", "disabled"}:
        status = "active"
    if status != "active":
        raise HTTPException(status_code=403, detail="User is disabled.")

    return {
        "id": int(user_id),
        "username": str(user.get("username") or ""),
        "role": role,
        "status": status,
        "session_id": str(payload.get("session_id") or session_id),
    }


def assert_login(request: Request) -> int:
    return int(get_login_user(request).get("id"))


def assert_admin_user(request: Request) -> Dict[str, Any]:
    user = get_login_user(request)
    if user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin role required.")
    return user
