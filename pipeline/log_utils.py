"""日志与输出工具模块"""
from __future__ import annotations

import os
import datetime as dt_module


def _bool_runtime_value(value, default=False):
    """将配置值转换为布尔值"""
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def quiet_runtime_output_enabled():
    """检查是否启用了静默输出模式"""
    from pipeline.config import get_config
    return _bool_runtime_value(get_config("QUIET_RUNTIME_OUTPUT", True), default=True)


def runtime_console_print(message="", level="INFO", force=False, end="\n"):
    """运行时控制台输出，静默模式下只输出 WARNING 和 ERROR 级别"""
    normalized_level = str(level or "INFO").strip().upper() or "INFO"
    if not force and quiet_runtime_output_enabled() and normalized_level not in {"WARNING", "ERROR"}:
        return
    print(message, end=end, flush=True)


def clear_runtime_output_if_needed():
    """根据需要清空运行时输出"""
    if not quiet_runtime_output_enabled():
        return False

    try:
        from IPython.display import clear_output
        clear_output(wait=True)
        return True
    except Exception:
        try:
            if os.name == "nt":
                os.system("cls")
            else:
                runtime_console_print("\033[2J\033[H", force=True, end="")
            return True
        except Exception:
            return False


class SimpleLogger:
    """简单的运行时日志记录器"""
    def _now(self):
        return dt_module.datetime.now().strftime("%H:%M:%S")

    def info(self, msg, *args):
        text = msg % args if args else msg
        runtime_console_print(f"{self._now()} [INFO] {text}", level="INFO")

    def warning(self, msg, *args):
        text = msg % args if args else msg
        runtime_console_print(f"{self._now()} [WARNING] {text}", level="WARNING")

    def error(self, msg, *args):
        text = msg % args if args else msg
        runtime_console_print(f"{self._now()} [ERROR] {text}", level="ERROR")


# 全局日志实例
log = SimpleLogger()
