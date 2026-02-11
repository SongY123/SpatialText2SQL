"""
日志工具模块
根据配置文件设置日志系统，同时输出到文件和控制台
"""
import logging
import sys
from pathlib import Path
from logging.handlers import RotatingFileHandler


_logger_instance = None


def setup_logger(name: str = "SpatialText2SQL") -> logging.Logger:

    from utils.config_loader import get_config

    logger = logging.getLogger(name)

    # 避免重复添加handlers
    if logger.handlers:
        return logger

    # 从配置文件读取日志配置
    log_level = get_config("logging.level", "INFO")
    log_format = get_config("logging.format", "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    log_file = get_config("logging.file_path", "logs/app.log")
    max_bytes = get_config("logging.max_file_size_mb", 10) * 1024 * 1024
    backup_count = get_config("logging.backup_count", 5)
    console_enabled = get_config("logging.console", True)

    # 设置日志级别
    logger.setLevel(getattr(logging, log_level.upper()))

    # 创建格式化器
    formatter = logging.Formatter(log_format)

    # 配置文件处理器（带日志轮转）
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding='utf-8'
        )
        file_handler.setFormatter(formatter)
        file_handler.setLevel(getattr(logging, log_level.upper()))
        logger.addHandler(file_handler)

    # 配置控制台处理器
    if console_enabled:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        console_handler.setLevel(getattr(logging, log_level.upper()))
        logger.addHandler(console_handler)

    return logger


def get_logger(name: str = None) -> logging.Logger:
    """
    获取logger实例（延迟初始化）

    Args:
        name: logger名称，如果为None则返回根logger

    Returns:
        logger实例
    """
    global _logger_instance

    if _logger_instance is None:
        _logger_instance = setup_logger()

    if name:
        return _logger_instance.getChild(name)
    return _logger_instance


# 提供一个便捷的 logger 属性（延迟初始化）
class LazyLogger:
    """延迟初始化的 logger 包装器"""

    def __getattr__(self, name):
        global _logger_instance
        if _logger_instance is None:
            _logger_instance = setup_logger()
        return getattr(_logger_instance, name)


logger = LazyLogger()
