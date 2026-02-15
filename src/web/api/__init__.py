from .admin_api import router as admin_router
from .chat_api import router as chat_router
from .database_api import router as database_router
from .user_api import auth_router, router as user_router

__all__ = [
    "admin_router",
    "user_router",
    "database_router",
    "chat_router",
    "auth_router",
]
