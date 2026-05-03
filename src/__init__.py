import os

os.environ.setdefault('HF_ENDPOINT', 'https://hf-mirror.com')

from .logging_config import setup_logging

__all__ = ["setup_logging"]
