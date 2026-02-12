from __future__ import annotations

from typing import Optional
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from fastapi import APIRouter, HTTPException, Request

from ..entity.request import DatabaseCreateRequest, DatabaseSchemaProbeRequest, DatabaseUpdateRequest
from ..entity.response import DatabasePublicResponse
from ..service import DatabaseService, UserService
from tools.db_connector import JdbcDatabaseTool


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


def _to_public_database(payload):
    return DatabasePublicResponse.from_dict(payload).to_dict()


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


def _patch_jdbc_auth(jdbc_url: str, db_type: str, username: Optional[str], password: Optional[str]) -> str:
    url = str(jdbc_url or "").strip()
    if not url.startswith("jdbc:"):
        raise ValueError("jdbc_url must start with jdbc:")

    if str(db_type).strip().lower() != "postgis":
        return url

    user = str(username).strip() if username is not None else ""
    pwd = str(password) if password is not None else ""
    if not user:
        return url

    body = url[5:]
    if not body.startswith("postgresql://"):
        return url

    parsed = urlparse(body)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    if "user" not in query and "password" not in query:
        query["user"] = user
        if pwd:
            query["password"] = pwd
        rebuilt = parsed._replace(query=urlencode(query))
        return "jdbc:" + urlunparse(rebuilt)
    return url


@router.post("")
def insert_database(body: DatabaseCreateRequest, request: Request):
    current_user_id = _assert_login(request)
    try:
        data = _database_service.insert_database(
            user_id=current_user_id,
            name=body.name,
            db_type=body.type,
            url=body.url,
            schema=body.schema_list,
            db_username=body.db_username,
            db_password=body.db_password,
        )
        return _ok(data=_to_public_database(data), message="database link inserted")
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.put("/{link_id}")
def update_database(link_id: int, body: DatabaseUpdateRequest, request: Request):
    _assert_login(request)
    try:
        data = _database_service.update_database(
            link_id=link_id,
            name=body.name,
            db_type=body.type,
            url=body.url,
            schema=body.schema_list,
            db_username=body.db_username,
            db_password=body.db_password,
        )
        return _ok(data=_to_public_database(data), message="database link updated")
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
def list_databases(request: Request):
    current_user_id = _assert_login(request)
    rows = _database_service.list_databases(user_id=current_user_id)
    data = [_to_public_database(x) for x in rows]
    return _ok(data=data, message="database links listed")


@router.get("/{database_id}/objects")
def list_tables_and_views(
    database_id: int,
    schema: str,
    request: Request,
):
    current_user_id = _assert_login(request)
    db = _database_service.get_database(link_id=database_id)
    if db is None:
        raise HTTPException(status_code=404, detail=f"database link not found: link_id={database_id}")
    if int(db.get("user_id")) != int(current_user_id):
        raise HTTPException(status_code=403, detail="Can only access current user's database link.")

    try:
        data = _database_service.list_tables_and_views(
            link_id=database_id,
            schema=schema,
        )
        return _ok(data=data, message="tables/views fetched")
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.get("/{database_id}/fields")
def get_fields(
    database_id: int,
    schema: str,
    object_name: str,
    object_type: str,
    request: Request,
):
    current_user_id = _assert_login(request)
    db = _database_service.get_database(link_id=database_id)
    if db is None:
        raise HTTPException(status_code=404, detail=f"database link not found: link_id={database_id}")
    if int(db.get("user_id")) != int(current_user_id):
        raise HTTPException(status_code=403, detail="Can only access current user's database link.")

    try:
        fields = _database_service.get_object_fields(
            link_id=database_id,
            schema=schema,
            object_name=object_name,
            object_type=object_type,
        )
        return _ok(data=fields, message="fields fetched")
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.get("/{database_id}/columns")
def get_columns(
    database_id: int,
    schema: str,
    object_name: str,
    object_type: str,
    request: Request,
):
    # Alias of /fields for clearer API semantics.
    return get_fields(
        database_id=database_id,
        schema=schema,
        object_name=object_name,
        object_type=object_type,
        request=request,
    )


@router.get("/{database_id}/ddl")
def get_ddl(
    database_id: int,
    schema: str,
    object_name: str,
    object_type: str,
    request: Request,
):
    current_user_id = _assert_login(request)
    db = _database_service.get_database(link_id=database_id)
    if db is None:
        raise HTTPException(status_code=404, detail=f"database link not found: link_id={database_id}")
    if int(db.get("user_id")) != int(current_user_id):
        raise HTTPException(status_code=403, detail="Can only access current user's database link.")

    try:
        data = _database_service.get_object_ddl(
            link_id=database_id,
            schema=schema,
            object_name=object_name,
            object_type=object_type,
        )
        return _ok(data=data, message="ddl fetched")
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.get("/{database_id}/samples")
def get_sample_data(
    database_id: int,
    schema: str,
    object_name: str,
    request: Request,
    object_type: Optional[str] = None,
    page: int = 1,
    page_size: int = 20,
):
    current_user_id = _assert_login(request)
    db = _database_service.get_database(link_id=database_id)
    if db is None:
        raise HTTPException(status_code=404, detail=f"database link not found: link_id={database_id}")
    if int(db.get("user_id")) != int(current_user_id):
        raise HTTPException(status_code=403, detail="Can only access current user's database link.")

    try:
        data = _database_service.get_sample_data_page(
            link_id=database_id,
            schema=schema,
            object_name=object_name,
            object_type=object_type,
            page=page,
            page_size=page_size,
        )
        return _ok(data=data, message="sample data fetched")
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.post("/schemas")
def probe_schemas(body: DatabaseSchemaProbeRequest, request: Request):
    _assert_login(request)
    patched_jdbc_url = _patch_jdbc_auth(
        jdbc_url=body.jdbc_url,
        db_type=body.type,
        username=body.username,
        password=body.password,
    )
    tool = None
    try:
        tool = JdbcDatabaseTool(jdbc_url=patched_jdbc_url)
        metadata = tool.get_metadata(schema=None, include_views=False)
        schemas = metadata.get("schemas") or []
        schema_list = [str(s) for s in schemas if s is not None and str(s).strip() != ""]
        return schema_list
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    finally:
        if tool is not None:
            tool.close()
