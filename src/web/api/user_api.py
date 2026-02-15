from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request, Response

from ..entity.request import LoginRequest, UserCreateRequest, UserUpdateRequest
from ..service import UserService


router = APIRouter(prefix="/users", tags=["users"])
auth_router = APIRouter(prefix="/auth", tags=["auth"])

_user_service = UserService()
SESSION_COOKIE_KEY = "spatial_session_id"


def _ok(data=None, message: str = "ok"):
    return {
        "success": True,
        "message": message,
        "data": data,
    }


@router.post("")
def insert_user(body: UserCreateRequest):
    try:
        data = _user_service.insert_user(
            username=body.username,
            password=body.password,
            role=body.role,
        )
        return _ok(data=data, message="user inserted")
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.put("/{user_id}")
def update_user(user_id: int, body: UserUpdateRequest):
    try:
        data = _user_service.update_user(
            user_id=user_id,
            username=body.username,
            password=body.password,
            role=body.role,
        )
        return _ok(data=data, message="user updated")
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.delete("/{user_id}")
def delete_user(user_id: int):
    try:
        deleted = _user_service.delete_user(user_id=user_id)
        if not deleted:
            raise HTTPException(status_code=404, detail=f"user not found: user_id={user_id}")
        return _ok(data={"deleted": True}, message="user deleted")
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@auth_router.post("/login")
def login(body: LoginRequest, response: Response):
    try:
        data = _user_service.login(username=body.username, password=body.password)
        response.set_cookie(
            key=SESSION_COOKIE_KEY,
            value=data["session_id"],
            httponly=True,
            samesite="lax",
        )
        return _ok(data=data, message="login success")
    except Exception as exc:
        raise HTTPException(status_code=401, detail=str(exc))


@auth_router.post("/logout")
def logout(request: Request, response: Response):
    session_id = request.cookies.get(SESSION_COOKIE_KEY)
    if not session_id:
        raise HTTPException(status_code=401, detail="Not logged in.")
    deleted = _user_service.logout(session_id=session_id)
    response.delete_cookie(key=SESSION_COOKIE_KEY)
    return _ok(data={"deleted": deleted}, message="logout success")


@auth_router.get("/session")
def get_current_session(request: Request):
    session_id = request.cookies.get(SESSION_COOKIE_KEY)
    if not session_id:
        raise HTTPException(status_code=401, detail="Not logged in.")

    data = _user_service.get_session(session_id=session_id)
    if data is None:
        raise HTTPException(status_code=401, detail="Session expired or invalid.")

    return _ok(data=data, message="session found")
