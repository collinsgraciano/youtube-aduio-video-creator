"""工具函数模块 - 通用工具函数"""
from __future__ import annotations

import os
import re
import json
import csv
import shutil
import hashlib
import time
import requests
from pathlib import Path
from urllib.parse import urlparse, urlunparse

from pipeline.log_utils import log


# 非法文件名字符正则
_ILLEGAL_CHARS = re.compile(r'[\\/:*?"<>|\x00-\x1f]')


def sanitize_filename(name: str) -> str:
    """去除文件名中的非法字符，限制长度"""
    name = _ILLEGAL_CHARS.sub("_", name).strip()
    return name[:100] if len(name) > 100 else name


def normalize_text_items(value):
    """
    兼容历史云端返回的文本集合格式：
    - None / 空值
    - Python list/tuple/set
    - 普通逗号分隔字符串: "a,b"
    - PostgreSQL array literal: {"a","b"}
    """
    if value is None:
        return []

    raw_items = []

    if isinstance(value, (list, tuple, set)):
        raw_items = list(value)
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            return []

        if text.startswith("{") and text.endswith("}"):
            inner = text[1:-1].strip()
            if not inner:
                return []
            raw_items = re.split(r'[,"\s]+', inner)
            raw_items = [x.strip().strip('"') for x in raw_items if x.strip().strip('"')]
        else:
            raw_items = [item.strip() for item in text.split(",") if item.strip()]
    else:
        try:
            raw_items = list(value)
        except Exception:
            return []

    seen = set()
    result = []
    for item in raw_items:
        text = str(item or "").strip()
        if text and text not in seen:
            seen.add(text)
            result.append(text)
    return result


def make_json_compatible(value):
    """递归将值转换为 JSON 可序列化的类型"""
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, (list, tuple)):
        return [make_json_compatible(item) for item in value]
    if isinstance(value, dict):
        return {str(k): make_json_compatible(v) for k, v in value.items()}
    return str(value)


def append_unique_text_items(existing_value, additions):
    """向现有文本集合中添加不重复的项目（字符串形式）"""
    items = set(normalize_text_items(existing_value))
    for addition in normalize_text_items(additions):
        items.add(addition)
    return ", ".join(sorted(items))


def build_supabase_text_update(existing_value, additions, prefer="auto"):
    """构建用于 Supabase 文本字段更新的值"""
    result = append_unique_text_items(existing_value, additions)
    return result


def parse_text_list_config(value):
    """解析配置中的文本列表（逗号或换行分隔）"""
    items = []
    for chunk in str(value or "").replace("\r", "\n").split("\n"):
        for part in chunk.split(","):
            item = part.strip()
            if item:
                items.append(item)
    return items


def write_json_file(path, data):
    """写入 JSON 文件"""
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return path


def read_json_file(path, default=None):
    """读取 JSON 文件"""
    if not path or not os.path.exists(path):
        return default

    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        log.warning("JSON 读取失败 %s: %s", path, e)
        return default


def format_seconds_hhmmss(total_seconds):
    """将秒数格式化为 HH:MM:SS"""
    seconds = max(0, int(total_seconds or 0))
    return f"{seconds // 3600:02d}:{(seconds % 3600) // 60:02d}:{seconds % 60:02d}"


def download_file(url: str, save_path: str, retries: int = 3) -> bool:
    """
    通用文件下载函数。
    先用 wget（若可用），回退到 requests 流式下载。
    """
    from pipeline.config import get_config
    from pipeline.log_utils import runtime_console_print

    max_retries = max(1, int(retries or get_config("MAX_RETRIES", 3)))

    if not url or not save_path:
        return False

    os.makedirs(os.path.dirname(save_path) or ".", exist_ok=True)

    wget_binary = shutil.which("wget")
    if wget_binary:
        for attempt in range(1, max_retries + 1):
            if os.path.exists(save_path):
                try:
                    os.remove(save_path)
                except Exception:
                    pass

            cmd = [
                wget_binary,
                "-O", save_path,
                "--tries=1",
                "--timeout=30",
                "--read-timeout=30",
                "--retry-connrefused",
                "--waitretry=5",
                url,
            ]
            try:
                result = __import__("subprocess").run(cmd, capture_output=True, text=True, timeout=3600)
                if result.returncode == 0 and os.path.exists(save_path) and os.path.getsize(save_path) > 0:
                    return True
            except Exception as e:
                runtime_console_print(f"⚠️ wget 下载第 {attempt}/{max_retries} 次失败: {e}", level="WARNING")

            time.sleep(min(10, attempt * 2))

    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, stream=True, timeout=(30, 120), allow_redirects=True)
            resp.raise_for_status()
            with open(save_path, "wb") as handle:
                for chunk in resp.iter_content(chunk_size=1024 * 512):
                    if chunk:
                        handle.write(chunk)
            if os.path.getsize(save_path) > 0:
                return True
        except Exception as e:
            runtime_console_print(f"⚠️ requests 下载第 {attempt}/{max_retries} 次失败: {e}", level="WARNING")
            if os.path.exists(save_path):
                try:
                    os.remove(save_path)
                except Exception:
                    pass
            time.sleep(min(10, attempt * 2))

    return False


def clear_folder(path: str) -> None:
    """清空文件夹内容"""
    if not os.path.exists(path):
        return
    for item in os.listdir(path):
        item_path = os.path.join(path, item)
        try:
            if os.path.isfile(item_path) or os.path.islink(item_path):
                os.unlink(item_path)
            elif os.path.isdir(item_path):
                shutil.rmtree(item_path, ignore_errors=True)
        except Exception as e:
            log.warning("清理文件失败: %s", e)


def safe_music_output_path(target_dir, original_name):
    """生成安全的输出路径，避免文件名冲突"""
    base_name = os.path.basename(original_name or "").strip()
    if not base_name:
        base_name = "music.mp3"

    stem, ext = os.path.splitext(base_name)
    candidate = os.path.join(target_dir, base_name)
    counter = 2
    while os.path.exists(candidate):
        candidate = os.path.join(target_dir, f"{stem}_{counter}{ext}")
        counter += 1
    return candidate


def normalize_runtime_source(source, default="database"):
    """规整化配置来源标识"""
    mode = str(source or default).strip().lower()
    if not mode:
        return default
    if mode in {"supabase", "postgres", "postgresql", "db"}:
        return "database"
    return mode