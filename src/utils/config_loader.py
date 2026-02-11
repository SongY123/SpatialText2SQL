import os
import yaml
from typing import Any, Dict

class ConfigLoader:
    _config: Dict[str, Any] = {}

    @classmethod
    def load_config(cls, config_path: str):
        """加载 YAML 配置文件"""
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"配置文件未找到: {config_path}")

        with open(config_path, "r", encoding="utf-8") as f:
            cls._config = yaml.safe_load(f)

        # 设置 NO_PROXY 环境变量，让 Ollama 等本地服务绕过代理
        cls._setup_no_proxy()

        return cls._config

    @classmethod
    def _setup_no_proxy(cls):
        """设置 NO_PROXY 环境变量"""
        # 仅当配置里存在 ollama host 时才设置 NO_PROXY，避免与 preprocess 配置冲突
        ollama_host = cls.get("model.ollama.host")
        if not ollama_host:
            return

        # 提取主机名和 IP
        import re
        match = re.search(r'://([^:/]+)', str(ollama_host))
        if match:
            host = match.group(1)
        else:
            host = str(ollama_host)

        # 构建 NO_PROXY 列表
        no_proxy_list = [
            "localhost",
            "127.0.0.1",
            "::1",
            host,  # 添加 Ollama 服务器地址
        ]

        no_proxy_str = ",".join(no_proxy_list)

        # 设置环境变量（大小写都设置以兼容不同系统）
        os.environ["NO_PROXY"] = no_proxy_str
        os.environ["no_proxy"] = no_proxy_str

        print(f"[CONFIG] NO_PROXY 已设置: {no_proxy_str}")

    @classmethod
    def get_config(cls) -> Dict[str, Any]:
        """获取配置对象，如果未加载则报错"""
        if not cls._config:
            raise RuntimeError("配置未初始化，请先调用 load_config(path)")
        return cls._config

    @classmethod
    def get(cls, key_path: str, default: Any = None) -> Any:
        """根据路径获取配置项，如 'server.port'"""
        config = cls.get_config()
        keys = key_path.split(".")
        value = config
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default
        return value

# 全局便捷方法
def get_config(key_path: str, default: Any = None) -> Any:
    return ConfigLoader.get(key_path, default)
