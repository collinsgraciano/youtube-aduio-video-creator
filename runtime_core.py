"""Remote runtime core for the Colab audiobook pipeline."""
from __future__ import annotations

from psycopg import connect, sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

DEFAULT_RUNTIME_CONFIG = {'POSTGRES_DSN': '',
 'YOUTUBE_CHANNEL_NAME': '',
 'MAX_PROCESS_COUNT': 10,
 'PROJECT_FLAG': '',
 'OUTPUT_ROOT': '/content/',
 'TARGET_CATEGORY': '文学小说',
 'DOWNLOAD_WORKERS': 4,
 'REQUEST_DELAY': 0.3,
 'REQUEST_TIMEOUT': 300,
 'MODELSCOPE_IMAGE_CONNECT_TIMEOUT': 300,
 'MODELSCOPE_IMAGE_READ_TIMEOUT': 300,
 'MODELSCOPE_IMAGE_POLL_CONNECT_TIMEOUT': 300,
 'MODELSCOPE_IMAGE_POLL_READ_TIMEOUT': 300,
 'MODELSCOPE_TOKEN_SWITCH_DELAY_SECONDS': 30,
 'API_PRIORITY_ORDER': 'modelscope,sensenova',
 'MAX_RETRIES': 3,
 'AUDIO_DOWNLOAD_CONNECT_TIMEOUT': 20,
 'AUDIO_DOWNLOAD_READ_TIMEOUT': 90,
 'AUDIO_DOWNLOAD_MAX_RETRY_ATTEMPTS': 12,
 'AUDIO_DOWNLOAD_MAX_TOTAL_SECONDS': 1800,
 'AUDIO_DOWNLOAD_STUCK_LOG_INTERVAL_SECONDS': 30,
 'SKIP_EXISTING': True,
 'FORCE_REPROCESS': False,
 'MAX_RUNTIME_HOURS': 11.5,
 'STOP_BUFFER_MINUTES': 20,
 'LONG_AUDIO_SPLIT_TRIGGER_HOURS': 12.0,
 'LONG_AUDIO_PART_TARGET_HOURS': 11.8,
 'BOOK_STATE_TABLE': 'book_processing_states',
 'CLEANUP_COMPLETED_SPLIT_STATES': True,
 'PRIORITIZE_INTERRUPTED_BOOKS': True,
 'QUIET_RUNTIME_OUTPUT': True,
 'ENABLE_DEEPFILTER': True,
 'segment_duration_minutes': 60,
 'DEEPFILTER_WORKERS': 2,
 'ENABLE_COVER_GENERATION': True,
 'MODELSCOPE_TOKEN_SOURCE': 'database',
 'CLOUD_RUNTIME_SETTINGS_TABLE': 'channel_runtime_settings',
 'MODELSCOPE_TOKEN_TABLE': 'modelscope_tokens',
 'MODELSCOPE_TOKEN': '',
 'ENABLE_SEO_GENERATION': True,
 'ENABLE_YOUTUBE_UPLOAD': True,
 'YOUTUBE_PRIVACY_STATUS': 'schedule',
 'YOUTUBE_SCHEDULE_AFTER_HOURS': 24,
 'YOUTUBE_DAILY_PUBLISH_LIMIT': 3,
 'YOUTUBE_CATEGORY_ID': '',
 'YOUTUBE_DEFAULT_LANGUAGE': 'zh-CN',
 'ENABLE_YOUTUBE_TRADITIONAL_LOCALIZATION': True,
 'YOUTUBE_LOCALIZATION_LOCALES': 'zh-TW,zh-HK,zh-SG,zh-Hant',
 'YOUTUBE_TRADITIONAL_LOCALE': 'zh-TW',
 'YOUTUBE_TRADITIONAL_OPENCC_CONFIG': 's2t',
 'ENABLE_AUTO_INSTALL_OPENCC': True,
 'APPEND_TAGS_TO_TITLE': False,
 'APPEND_TAGS_TO_DESC': True,
 'ENABLE_VIDEO_GENERATION': True,
 'VIDEO_RESOLUTION': '1080p',
 'DOWNLOAD_FROM_BUCKETS': True,
 'HF_MUSIC_DOWNLOAD_METHOD': 'datasets_zip_urls',
 'HF_DATASET_ZIP_URLS_SOURCE': 'database',
 'HF_DATASET_ZIP_URLS': '',
 'BUCKET_IDS_SOURCE': 'database',
 'BUCKET_IDS': '',
 'HF_TOKEN': '',
 'LOCAL_MUSIC_DIR': '/content/music',
 'ENABLE_BGM_MIX': True,
 'MUSIC_DIR': '/content/music',
 'VOLUME_OFFSET_DB': -25,
 'HIGHPASS_FREQ': 150,
 'FADE_DURATION_MS': 3000,
 'MIN_VOLUME_DB': -40,
 'ENABLE_DYNAMIC_VOLUME': True,
 'ENABLE_SPECTRAL_SHAPING': True,
 'STEREO_OFFSET': 0.0}

def apply_runtime_config(runtime_config: dict | None = None):
    merged = dict(DEFAULT_RUNTIME_CONFIG)
    if runtime_config:
        merged.update(runtime_config)

    if not str(merged.get("PROJECT_FLAG", "") or "").strip():
        merged["PROJECT_FLAG"] = str(merged.get("YOUTUBE_CHANNEL_NAME", "") or "").strip()

    if not str(merged.get("MUSIC_DIR", "") or "").strip():
        merged["MUSIC_DIR"] = str(merged.get("LOCAL_MUSIC_DIR", "") or "").strip()

    globals().update(merged)
    return merged


apply_runtime_config()

import os
import random
import shutil
import subprocess
import tempfile
import time
import zipfile
from pathlib import Path
from urllib.parse import urlparse, urlunparse

import requests


SUPPORTED_AUDIO_EXTENSIONS = (".mp3", ".wav", ".flac", ".m4a", ".ogg", ".aac", ".wma")
POSTGRES_SCHEMA = "public"


def parse_text_list_config(value):
    items = []
    for chunk in str(value or "").replace("\r", "\n").split("\n"):
        for part in chunk.split(","):
            item = part.strip()
            if item:
                items.append(item)
    return items


def normalize_runtime_source(source, default="database"):
    mode = str(source or default).strip().lower()
    if not mode:
        return default
    if mode in {"supabase", "postgres", "postgresql", "db"}:
        return "database"
    return mode


def get_postgres_dsn(optional=False):
    dsn = str(globals().get("POSTGRES_DSN", "") or "").strip()
    if not dsn and not optional:
        raise RuntimeError("POSTGRES_DSN 未初始化，请先配置 PostgreSQL 连接串。")
    return dsn


def get_public_table_identifier(table_name):
    normalized_name = str(table_name or "").strip()
    if not normalized_name:
        raise RuntimeError("数据库表名不能为空。")
    return sql.Identifier(POSTGRES_SCHEMA, normalized_name)


def execute_postgres_fetchone(statement, params=None, optional=False):
    dsn = get_postgres_dsn(optional=optional)
    if not dsn:
        return None

    with connect(dsn, autocommit=True, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(statement, params or ())
            row = cur.fetchone()
            return dict(row) if row else None


def execute_postgres_fetchall(statement, params=None, optional=False):
    dsn = get_postgres_dsn(optional=optional)
    if not dsn:
        return []

    with connect(dsn, autocommit=True, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(statement, params or ())
            rows = cur.fetchall() or []
            return [dict(row) for row in rows]


def execute_postgres(statement, params=None, optional=False):
    dsn = get_postgres_dsn(optional=optional)
    if not dsn:
        return 0

    with connect(dsn, autocommit=True, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(statement, params or ())
            return cur.rowcount


def execute_postgres_fetchval(statement, params=None, optional=False):
    row = execute_postgres_fetchone(statement, params=params, optional=optional)
    if not row:
        return None
    return next(iter(row.values()))


def load_cloud_music_runtime_setting(setting_key):
    table_name = str(globals().get("CLOUD_RUNTIME_SETTINGS_TABLE", "") or "channel_runtime_settings").strip() or "channel_runtime_settings"
    shared_scope = "__shared__"
    key = str(setting_key or "").strip()
    if not key:
        return ""

    try:
        if not get_postgres_dsn(optional=True):
            return ""

        table_sql = get_public_table_identifier(table_name)
        shared_row = execute_postgres_fetchone(
            sql.SQL(
                """
                SELECT setting_value
                FROM {}
                WHERE channel_name = %s AND setting_key = %s
                ORDER BY updated_at DESC
                LIMIT 1
                """
            ).format(table_sql),
            (shared_scope, key),
            optional=True,
        )
        if shared_row:
            return str(shared_row.get("setting_value") or "")

        legacy_row = execute_postgres_fetchone(
            sql.SQL(
                """
                SELECT setting_value
                FROM {}
                WHERE setting_key = %s
                ORDER BY updated_at DESC
                LIMIT 1
                """
            ).format(table_sql),
            (key,),
            optional=True,
        )
        if legacy_row:
            runtime_console_print(f"⚠️ 共享云端配置 {key} 暂未设置，当前临时回退到历史记录中的最新值。", level="WARNING")
            return str(legacy_row.get("setting_value") or "")

        return ""
    except Exception as e:
        runtime_console_print(f"⚠️ 读取数据库运行配置 {key} 失败，先回退到本地值: {e}", level="WARNING")
        return ""


def resolve_music_runtime_setting(setting_key, local_value, source="database"):
    mode = normalize_runtime_source(source, default="database")
    local_text = str(local_value or "")

    if mode not in {"database", "local"}:
        runtime_console_print(f"⚠️ {setting_key} 的来源配置无效，当前回退到本地值。", level="WARNING")
        return local_text

    if mode == "local":
        return local_text

    cloud_value = load_cloud_music_runtime_setting(setting_key)
    if str(cloud_value).strip():
        runtime_console_print(f"☁️ 已从数据库读取 {setting_key}", level="INFO")
        return str(cloud_value)

    if str(local_text).strip():
        runtime_console_print(f"⚠️ 数据库中未找到 {setting_key}，当前运行临时回退到本地值。", level="WARNING")
    return local_text


def apply_music_download_runtime_overrides():
    globals()["HF_DATASET_ZIP_URLS"] = resolve_music_runtime_setting(
        "HF_DATASET_ZIP_URLS",
        globals().get("HF_DATASET_ZIP_URLS", ""),
        globals().get("HF_DATASET_ZIP_URLS_SOURCE", "database"),
    )
    globals()["BUCKET_IDS"] = resolve_music_runtime_setting(
        "BUCKET_IDS",
        globals().get("BUCKET_IDS", ""),
        globals().get("BUCKET_IDS_SOURCE", "database"),
    )


def build_hf_download_headers():
    token = str(HF_TOKEN or "").strip()
    if not token:
        return {}
    return {"Authorization": f"Bearer {token}"}


def normalize_hf_dataset_download_url(url):
    raw = str(url or "").strip()
    if not raw:
        return ""

    parsed = urlparse(raw)
    path = parsed.path
    if "/blob/" in path:
        path = path.replace("/blob/", "/resolve/", 1)
    elif "/resolve/" not in path and parsed.netloc.endswith("huggingface.co"):
        path = path.rstrip("/") + "/resolve/main"

    query = parsed.query
    if "download=" not in query.lower():
        query = f"{query}&download=true" if query else "download=true"

    return urlunparse((parsed.scheme, parsed.netloc, path, parsed.params, query, parsed.fragment))


def safe_music_output_path(target_dir, original_name):
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


def download_file_with_wget(download_url, output_path, headers=None, retries=3):
    headers = headers or {}
    wget_binary = shutil.which("wget")
    if not wget_binary:
        return False

    for attempt in range(1, retries + 1):
        if os.path.exists(output_path):
            os.remove(output_path)

        cmd = [
            wget_binary,
            "-O",
            output_path,
            "--tries=1",
            "--timeout=30",
            "--read-timeout=30",
            "--retry-connrefused",
            "--waitretry=5",
            download_url,
        ]
        for key, value in headers.items():
            cmd.insert(-1, "--header")
            cmd.insert(-1, f"{key}: {value}")

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)
            if result.returncode == 0 and os.path.exists(output_path) and os.path.getsize(output_path) > 0:
                return True
        except Exception as e:
            runtime_console_print(f"⚠️ wget 下载第 {attempt}/{retries} 次失败: {e}", level="WARNING")

        time.sleep(min(10, attempt * 2))

    return False


def download_file_with_requests(download_url, output_path, headers=None, retries=3):
    headers = headers or {}
    temp_path = output_path + ".tmp"

    for attempt in range(1, retries + 1):
        try:
            with requests.get(download_url, headers=headers, stream=True, timeout=(30, 120), allow_redirects=True) as response:
                response.raise_for_status()
                with open(temp_path, "wb") as handle:
                    for chunk in response.iter_content(chunk_size=1024 * 512):
                        if chunk:
                            handle.write(chunk)

            if os.path.exists(output_path):
                os.remove(output_path)
            shutil.move(temp_path, output_path)
            if os.path.getsize(output_path) > 0:
                return True
        except Exception as e:
            runtime_console_print(f"⚠️ requests 下载第 {attempt}/{retries} 次失败: {e}", level="WARNING")
            if os.path.exists(temp_path):
                os.remove(temp_path)
            time.sleep(min(10, attempt * 2))

    return False


def extract_audio_files_from_zip(zip_path, output_dir, allowed_exts=SUPPORTED_AUDIO_EXTENSIONS):
    extracted_paths = []
    os.makedirs(output_dir, exist_ok=True)

    with zipfile.ZipFile(zip_path, "r") as archive:
        for member in archive.infolist():
            if member.is_dir():
                continue

            ext = os.path.splitext(member.filename)[1].lower()
            if ext not in allowed_exts:
                continue

            output_path = safe_music_output_path(output_dir, member.filename)
            with archive.open(member, "r") as source, open(output_path, "wb") as target:
                shutil.copyfileobj(source, target)
            extracted_paths.append(output_path)

    return extracted_paths


def download_music_from_dataset_urls():
    url_candidates = parse_text_list_config(HF_DATASET_ZIP_URLS)
    if not url_candidates:
        runtime_console_print("⚠️ 未配置有效的 HF_DATASET_ZIP_URLS，跳过下载。", level="WARNING")
        return False

    selected_input_url = random.choice(url_candidates)
    selected_download_url = normalize_hf_dataset_download_url(selected_input_url)
    headers = build_hf_download_headers()

    os.makedirs(LOCAL_MUSIC_DIR, exist_ok=True)
    globals()["MUSIC_DIR"] = LOCAL_MUSIC_DIR

    temp_dir = tempfile.mkdtemp(prefix="hf_music_zip_")
    archive_name = os.path.basename(urlparse(selected_download_url).path) or "music_bundle.zip"
    archive_path = os.path.join(temp_dir, archive_name)

    runtime_console_print(f"🎲 已随机选择 Datasets 音乐包: {selected_input_url}", level="INFO")
    runtime_console_print(f"⬇️ 准备下载 ZIP: {selected_download_url}", level="INFO")

    try:
        ok = download_file_with_wget(selected_download_url, archive_path, headers=headers)
        if not ok:
            runtime_console_print("⚠️ wget 下载未成功，切换到 requests 流式下载...", level="WARNING")
            ok = download_file_with_requests(selected_download_url, archive_path, headers=headers)

        if not ok:
            raise RuntimeError("ZIP 下载失败，已尝试 wget 与 requests 两种方式")

        extracted = extract_audio_files_from_zip(archive_path, LOCAL_MUSIC_DIR)
        if not extracted:
            raise RuntimeError("ZIP 下载成功，但解压后未找到任何支持的音频文件")

        runtime_console_print(f"✅ Datasets ZIP 下载并解压完成，共导入 {len(extracted)} 个音频文件到 {LOCAL_MUSIC_DIR}", level="INFO")
        return True
    except Exception as e:
        runtime_console_print(f"❌ Datasets ZIP 下载失败: {e}", level="ERROR")
        return False
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def download_music_from_buckets():
    from huggingface_hub import list_bucket_tree, download_bucket_files, login

    bucket_list = [b.strip() for b in BUCKET_IDS.split(",") if b.strip()]
    if not bucket_list or bucket_list[0].startswith("username/my-bucket"):
        runtime_console_print("⚠️ 未配置有效的 BUCKET_IDS，跳过下载。", level="WARNING")
        return False

    selected_bucket = random.choice(bucket_list)
    runtime_console_print(f"🎲 已随机选择 Bucket: {selected_bucket}", level="INFO")

    if HF_TOKEN.strip():
        runtime_console_print("🔑 正在使用 Token 登录 Hugging Face...", level="INFO")
        login(token=HF_TOKEN.strip())

    os.makedirs(LOCAL_MUSIC_DIR, exist_ok=True)
    globals()["MUSIC_DIR"] = LOCAL_MUSIC_DIR

    try:
        runtime_console_print(f"🔍 正在检索 Bucket {selected_bucket} 中的音频文件...", level="INFO")
        music_files = [
            item for item in list_bucket_tree(selected_bucket, recursive=True)
            if item.type == "file" and item.path.lower().endswith(SUPPORTED_AUDIO_EXTENSIONS)
        ]

        if not music_files:
            runtime_console_print(f"⚠️ 在 Bucket '{selected_bucket}' 中未找到任何音频文件。", level="WARNING")
            return False

        runtime_console_print(f"⬇️ 发现 {len(music_files)} 首音乐，开始下载到 {LOCAL_MUSIC_DIR}...", level="INFO")
        download_bucket_files(
            selected_bucket,
            files=[(f, safe_music_output_path(LOCAL_MUSIC_DIR, f.path)) for f in music_files],
        )
        runtime_console_print("✅ Hugging Face Buckets 版权音乐同步完成！", level="INFO")
        return True
    except Exception as e:
        runtime_console_print(f"❌ Buckets 下载失败，请检查 Bucket 名称、路径或 Token: {e}", level="ERROR")
        return False


def sync_music_library_if_enabled():
    apply_music_download_runtime_overrides()

    if DOWNLOAD_FROM_BUCKETS:
        selected_method = str(HF_MUSIC_DOWNLOAD_METHOD or "datasets_zip_urls").strip().lower()
        if selected_method == "buckets":
            return download_music_from_buckets()
        return download_music_from_dataset_urls()

    runtime_console_print("⏭️ 已关闭版权音乐自动同步。", level="INFO")
    return False


import os
import re
import json
import csv
import time
import math
import shutil
import random
import logging
import subprocess
import tempfile
import hashlib
import traceback
import requests
import concurrent.futures
from pathlib import Path
from dataclasses import dataclass, field
from tqdm.auto import tqdm
from concurrent.futures import ThreadPoolExecutor
from pydub import AudioSegment

import datetime as dt_module


def _bool_runtime_value(value, default=False):
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def quiet_runtime_output_enabled():
    return _bool_runtime_value(globals().get("QUIET_RUNTIME_OUTPUT", True), default=True)


def runtime_console_print(message="", level="INFO", force=False, end="\n"):
    normalized_level = str(level or "INFO").strip().upper() or "INFO"
    if not force and quiet_runtime_output_enabled() and normalized_level not in {"WARNING", "ERROR"}:
        return
    print(message, end=end, flush=True)


def clear_runtime_output_if_needed():
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

log = SimpleLogger()

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
            try:
                raw_items = next(
                    csv.reader(
                        [inner],
                        skipinitialspace=True,
                        quotechar='"',
                        escapechar="\\",
                    )
                )
            except Exception:
                raw_items = inner.split(",")
        else:
            raw_items = text.split(",")
    else:
        raw_items = [value]

    normalized = []
    seen = set()
    for item in raw_items:
        text = str(item).strip().strip('"').strip()
        if not text or text in seen:
            continue
        normalized.append(text)
        seen.add(text)
    return normalized


def make_json_compatible(value):
    """Recursively convert runtime objects into JSON-safe values."""
    if isinstance(value, dict):
        return {str(key): make_json_compatible(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [make_json_compatible(item) for item in value]
    if isinstance(value, (dt_module.datetime, dt_module.date, dt_module.time)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    return value


def append_unique_text_items(existing_value, additions):
    items = normalize_text_items(existing_value)
    seen = set(items)
    for item in normalize_text_items(additions):
        if item in seen:
            continue
        items.append(item)
        seen.add(item)
    return items


def build_supabase_text_update(existing_value, additions, prefer="auto"):
    merged = append_unique_text_items(existing_value, additions)

    mode = (prefer or "auto").strip().lower()
    if mode == "array":
        return merged
    if mode == "string":
        return ",".join(merged)

    if isinstance(existing_value, (list, tuple, set)):
        return merged
    return ",".join(merged)


def download_file(url: str, save_path: str, retries: int = MAX_RETRIES) -> bool:
    """
    下载文件到指定路径。
    使用临时文件 + rename 确保原子性，指数退避重试。
    """
    if os.path.exists(save_path) and os.path.getsize(save_path) > 0:
        return True

    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    tmp_path = save_path + ".tmp"

    for attempt in range(retries):
        try:
            resp = requests.get(url, timeout=REQUEST_TIMEOUT, stream=True)
            resp.raise_for_status()
            with open(tmp_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=64 * 1024):
                    f.write(chunk)

            # 验证文件大小
            expected = resp.headers.get("Content-Length")
            actual = os.path.getsize(tmp_path)
            if expected and int(expected) != actual:
                log.warning("文件大小不匹配: 预期=%s 实际=%s", expected, actual)
                os.remove(tmp_path)
                continue

            shutil.move(tmp_path, save_path)
            return True
        except Exception as e:
            wait = 2 ** attempt
            log.warning("下载失败（第%d/%d次，等%ds）: %s", attempt + 1, retries, wait, e)
            time.sleep(wait)

    if os.path.exists(tmp_path):
        os.remove(tmp_path)
    return False


def download_audio_file(url: str, save_path: str, timeout_seconds: int = 300) -> dict:
    """
    章节音频专用下载：
    - 单次请求拆分为连接超时 + 读超时
    - 失败后按上限重试，避免单个坏链接无限卡死整本书
    - 继续使用临时文件 + rename，避免生成坏文件
    """
    if os.path.exists(save_path) and os.path.getsize(save_path) > 0:
        return {"ok": True, "attempts": 0, "elapsed_seconds": 0.0, "error": ""}

    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    tmp_path = save_path + ".tmp"
    attempt = 0
    started_at = time.time()
    last_error = ""
    connect_timeout = max(3, int(globals().get("AUDIO_DOWNLOAD_CONNECT_TIMEOUT", 20) or 20))
    read_timeout = max(5, int(globals().get("AUDIO_DOWNLOAD_READ_TIMEOUT", timeout_seconds) or timeout_seconds))
    max_attempts = max(1, int(globals().get("AUDIO_DOWNLOAD_MAX_RETRY_ATTEMPTS", 12) or 12))
    max_total_seconds = max(read_timeout, int(globals().get("AUDIO_DOWNLOAD_MAX_TOTAL_SECONDS", 1800) or 1800))

    while attempt < max_attempts:
        attempt += 1
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

            with requests.get(url, timeout=(connect_timeout, read_timeout), stream=True) as resp:
                resp.raise_for_status()
                with open(tmp_path, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=64 * 1024):
                        if chunk:
                            f.write(chunk)

                expected = resp.headers.get("Content-Length")
            actual = os.path.getsize(tmp_path)
            if expected and int(expected) != actual:
                last_error = f"文件大小不匹配: 预期={expected} 实际={actual}"
                log.warning(
                    "章节音频下载大小不匹配，将继续重试: 预期=%s 实际=%s 文件=%s",
                    expected,
                    actual,
                    os.path.basename(save_path),
                )
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
                wait = min(60, max(2, 2 ** min(attempt - 1, 5)))
                if attempt >= max_attempts or (time.time() - started_at + wait) > max_total_seconds:
                    break
                time.sleep(wait)
                continue

            shutil.move(tmp_path, save_path)
            if attempt > 1:
                log.info("章节音频下载重试后成功: %s（第 %d 次）", os.path.basename(save_path), attempt)
            return {
                "ok": True,
                "attempts": attempt,
                "elapsed_seconds": round(time.time() - started_at, 1),
                "error": "",
            }
        except Exception as e:
            last_error = str(e)
            wait = min(60, max(2, 2 ** min(attempt - 1, 5)))
            log.warning(
                "章节音频下载失败，将继续重试（第 %d/%d 次，%ds 后重试）: %s | %s",
                attempt,
                max_attempts,
                wait,
                os.path.basename(save_path),
                e,
            )
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass
            if attempt >= max_attempts or (time.time() - started_at + wait) > max_total_seconds:
                break
            time.sleep(wait)

    elapsed_seconds = round(time.time() - started_at, 1)
    if os.path.exists(tmp_path):
        try:
            os.remove(tmp_path)
        except Exception:
            pass

    log.error(
        "章节音频下载达到上限，停止重试: %s | 已尝试 %d 次，耗时 %.1fs | 最后错误: %s",
        os.path.basename(save_path),
        attempt,
        elapsed_seconds,
        last_error or "未知错误",
    )

    return {
        "ok": False,
        "attempts": attempt,
        "elapsed_seconds": elapsed_seconds,
        "error": last_error or "未知错误",
    }
def merge_audio_ffmpeg(mp3_paths: list, output_path: str) -> bool:
    """
    使用 ffmpeg concat demuxer 合并多个 mp3 文件。
    零内存占用、无损（直接复制流）。
    """
    if not mp3_paths:
        log.warning("没有音频文件可合并")
        return False

    if SKIP_EXISTING and os.path.exists(output_path) and os.path.getsize(output_path) > 0:
        log.info("合并文件已存在，跳过: %s", os.path.basename(output_path))
        return True

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    list_file = output_path + ".filelist.txt"

    try:
        with open(list_file, "w", encoding="utf-8") as f:
            for p in mp3_paths:
                escaped = p.replace("'", "'\\''")
                f.write(f"file '{escaped}'\n")

        log.info("开始合并 %d 个音频...", len(mp3_paths))
        tmp_output = output_path + ".merging.mp3"
        result = subprocess.run(
            ["ffmpeg", "-y", "-f", "concat", "-safe", "0",
             "-i", list_file, "-c", "copy", tmp_output],
            capture_output=True, text=True, timeout=3600,
        )

        if result.returncode != 0:
            log.error("ffmpeg 合并失败: %s", result.stderr[-500:] if result.stderr else "")
            if os.path.exists(tmp_output):
                os.remove(tmp_output)
            return False

        shutil.move(tmp_output, output_path)
        log.info("✅ 合并完成：%s", os.path.basename(output_path))
        return True
    except subprocess.TimeoutExpired:
        log.error("ffmpeg 合并超时")
        return False
    except Exception as e:
        log.error("合并失败: %s", e)
        return False
    finally:
        if os.path.exists(list_file):
            os.remove(list_file)



def clear_folder(path: str) -> None:
    os.makedirs(path, exist_ok=True)
    for name in os.listdir(path):
        target = os.path.join(path, name)
        try:
            if os.path.isdir(target):
                shutil.rmtree(target)
            else:
                os.remove(target)
        except Exception as e:
            log.warning("清理目录失败: %s", e)


DEEP_FILTER_PATH = "/content/deep-filter-0.5.6-x86_64-unknown-linux-musl"
DEEP_FILTER_DRIVE = "/content/deep-filter-0.5.6-x86_64-unknown-linux-musl1"


def setup_deep_filter():
    if not os.path.exists(DEEP_FILTER_PATH):
        if not os.path.exists(DEEP_FILTER_DRIVE):
            subprocess.run(
                [
                    "wget",
                    "https://github.com/Rikorose/DeepFilterNet/releases/download/v0.5.6/deep-filter-0.5.6-x86_64-unknown-linux-musl",
                    "-P",
                    "/content/",
                ],
                check=True,
            )
            subprocess.run(["chmod", "+x", DEEP_FILTER_PATH], check=True)
            shutil.copy(DEEP_FILTER_PATH, DEEP_FILTER_DRIVE)
        else:
            shutil.copy(DEEP_FILTER_DRIVE, DEEP_FILTER_PATH)
            subprocess.run(["chmod", "+x", DEEP_FILTER_PATH], check=True)
    runtime_console_print("✅ DeepFilter 就绪", level="INFO")


if ENABLE_DEEPFILTER:
    setup_deep_filter()


def split_audio_to_wav(input_file, output_dir, seg_minutes=60, sr=16000):
    r = subprocess.run(
        [
            "ffprobe",
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            input_file,
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    total = float(r.stdout.strip())
    seg_sec = seg_minutes * 60
    n = math.ceil(total / seg_sec)
    os.makedirs(output_dir, exist_ok=True)
    for i in range(n):
        start = i * seg_sec
        dur = min(seg_sec, total - start)
        out = os.path.join(output_dir, f"segment_{i + 1:03d}.wav")
        subprocess.run(
            [
                "ffmpeg",
                "-ss",
                str(start),
                "-t",
                str(dur),
                "-i",
                input_file,
                "-vn",
                "-ar",
                str(sr),
                "-ac",
                "2",
                "-sample_fmt",
                "s16",
                "-acodec",
                "pcm_s16le",
                "-y",
                out,
            ],
            capture_output=True,
            check=True,
        )


def _df_process_wav(wav_file, output_dir):
    subprocess.run([DEEP_FILTER_PATH, wav_file, "--output-dir", output_dir], check=True)
    return os.path.join(output_dir, os.path.basename(wav_file))


def df_and_merge_wav(input_dir, output_dir, final_output, max_workers=1):
    os.makedirs(output_dir, exist_ok=True)
    wavs = sorted(
        [os.path.join(input_dir, f) for f in os.listdir(input_dir) if f.endswith(".wav")],
        key=os.path.getmtime,
    )
    renamed = []
    for idx, f in enumerate(wavs, 1):
        np_ = os.path.join(input_dir, f"{idx}.wav")
        os.rename(f, np_)
        renamed.append(np_)
    worker_count = max(1, min(int(max_workers or 1), len(renamed) or 1))
    with ThreadPoolExecutor(max_workers=worker_count) as ex:
        processed = list(ex.map(lambda f: _df_process_wav(f, output_dir), renamed))
    processed.sort(key=lambda x: int(os.path.splitext(os.path.basename(x))[0]))
    combined = AudioSegment.empty()
    for f in processed:
        combined += AudioSegment.from_wav(f)
    combined.export(final_output, format="wav")
    log.info("降噪合并完成: %s", final_output)


def denoise_audio(audio_path, segment_workers=1):
    source = Path(audio_path)
    job_dir = Path(tempfile.mkdtemp(prefix="deepfilter_job_"))
    split_dir = job_dir / "segments"
    df_dir = job_dir / "df"
    denoised = job_dir / f"denoised_{sanitize_filename(source.stem)}.wav"
    log.info("🔧 开始降噪: %s", source.name)

    try:
        clear_folder(str(split_dir))
        split_audio_to_wav(audio_path, str(split_dir), segment_duration_minutes)
        clear_folder(str(df_dir))
        df_and_merge_wav(str(split_dir), str(df_dir), str(denoised), max_workers=segment_workers)
        log.info("✅ 降噪完成: %s", source.name)
        return str(denoised), str(job_dir)
    except Exception:
        shutil.rmtree(job_dir, ignore_errors=True)
        raise


def denoise_audio_keep_format(audio_path: str, output_path: str = "", segment_workers=1) -> str:
    if not ENABLE_DEEPFILTER:
        return audio_path

    source = Path(audio_path)
    suffix = source.suffix.lower() or ".wav"
    target = Path(output_path) if output_path else source.with_name(f"{source.stem}_denoised{suffix}")

    if SKIP_EXISTING and target.exists() and target.stat().st_size > 0:
        log.info("复用已降噪音频: %s", target.name)
        return str(target)

    temp_wav, job_dir = denoise_audio(audio_path, segment_workers=segment_workers)
    os.makedirs(target.parent, exist_ok=True)

    try:
        if target.suffix.lower() == ".wav":
            if target.exists():
                target.unlink()
            shutil.move(temp_wav, str(target))
        else:
            cmd = ["ffmpeg", "-y", "-i", temp_wav]
            if target.suffix.lower() == ".mp3":
                cmd += ["-codec:a", "libmp3lame", "-b:a", "192k"]
            elif target.suffix.lower() in {".m4a", ".aac"}:
                cmd += ["-codec:a", "aac", "-b:a", "192k"]
            elif target.suffix.lower() == ".flac":
                cmd += ["-codec:a", "flac"]
            elif target.suffix.lower() == ".ogg":
                cmd += ["-codec:a", "libvorbis", "-qscale:a", "5"]
            cmd.append(str(target))
            subprocess.run(cmd, capture_output=True, check=True)
        log.info("✅ 降噪音频已写回: %s", target.name)
        return str(target)
    finally:
        shutil.rmtree(job_dir, ignore_errors=True)


def denoise_audio_paths_parallel(audio_paths, output_paths=None, max_workers=2):
    if not audio_paths:
        return []

    total = len(audio_paths)
    worker_count = max(1, min(int(max_workers or 1), total))
    results = {}

    if output_paths is not None and len(output_paths) != total:
        raise ValueError("output_paths length must match audio_paths length")

    def _run(item):
        idx, path = item
        log.info("  DeepFilter %d/%d -> %s", idx + 1, total, os.path.basename(path))
        output_path = output_paths[idx] if output_paths is not None else ""
        return idx, denoise_audio_keep_format(path, output_path=output_path, segment_workers=1)

    with ThreadPoolExecutor(max_workers=worker_count) as ex:
        futures = {ex.submit(_run, item): item[0] for item in enumerate(audio_paths)}
        for future in tqdm(concurrent.futures.as_completed(futures), total=total, desc="DeepFilter双线程降噪", unit="轨"):
            idx, out_path = future.result()
            results[idx] = out_path

    return [results[i] for i in range(total)]


def parse_duration_to_seconds(value):
    if value is None:
        return 0

    text = str(value).strip()
    if not text:
        return 0

    try:
        parts = [int(p) for p in text.split(":")]
    except ValueError:
        return 0

    if len(parts) == 3:
        return parts[0] * 3600 + parts[1] * 60 + parts[2]
    if len(parts) == 2:
        return parts[0] * 60 + parts[1]
    if len(parts) == 1:
        return parts[0]
    return 0


def probe_audio_duration_seconds(audio_path):
    if not audio_path or not os.path.exists(audio_path):
        return None

    try:
        result = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-show_entries",
                "format=duration",
                "-of",
                "default=noprint_wrappers=1:nokey=1",
                audio_path,
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        return max(0, int(round(float(result.stdout.strip()))))
    except Exception:
        try:
            return max(0, int(round(len(AudioSegment.from_file(audio_path)) / 1000)))
        except Exception:
            return None


def estimate_chapter_duration_seconds(chapter):
    if not isinstance(chapter, dict):
        return 1

    direct_value = chapter.get("duration_seconds")
    if isinstance(direct_value, (int, float)) and direct_value > 0:
        return max(1, int(round(float(direct_value))))

    for key in ("long", "duration", "audioDuration", "audio_duration"):
        value = chapter.get(key)
        if isinstance(value, (int, float)) and value > 0:
            return max(1, int(round(float(value))))
        seconds = parse_duration_to_seconds(value)
        if seconds > 0:
            return seconds

    return 1


def get_explicit_chapter_duration_seconds(chapter):
    if not isinstance(chapter, dict):
        return None

    direct_value = chapter.get("duration_seconds")
    if isinstance(direct_value, (int, float)) and direct_value > 0:
        return max(1, int(round(float(direct_value))))

    for key in ("long", "duration", "audioDuration", "audio_duration"):
        value = chapter.get(key)
        if isinstance(value, (int, float)) and value > 0:
            return max(1, int(round(float(value))))
        seconds = parse_duration_to_seconds(value)
        if seconds > 0:
            return seconds

    return None


def get_explicit_total_book_duration_seconds(chapters_sorted):
    if not chapters_sorted:
        return 0

    total_seconds = 0
    for chapter in chapters_sorted:
        chapter_seconds = get_explicit_chapter_duration_seconds(chapter)
        if chapter_seconds is None:
            return None
        total_seconds += chapter_seconds
    return total_seconds


def format_seconds_hhmmss(total_seconds):
    seconds = max(0, int(total_seconds or 0))
    return f"{seconds // 3600:02d}:{(seconds % 3600) // 60:02d}:{seconds % 60:02d}"


MIN_BOOK_DURATION_SECONDS = 30 * 60


def build_split_part_plans(chapters_sorted):
    split_trigger_seconds = max(1, int(float(LONG_AUDIO_SPLIT_TRIGGER_HOURS or 12.0) * 3600))
    part_target_seconds = max(1, int(float(LONG_AUDIO_PART_TARGET_HOURS or 11.8) * 3600))

    chapter_items = []
    total_estimated_seconds = 0
    for source_index, chapter in enumerate(chapters_sorted, start=1):
        estimated_seconds = estimate_chapter_duration_seconds(chapter)
        total_estimated_seconds += estimated_seconds
        chapter_items.append(
            {
                "source_index": source_index,
                "chapter": chapter,
                "chapter_id": chapter.get("id", source_index),
                "title": chapter.get("title", f"chapter_{source_index:04d}"),
                "estimated_seconds": estimated_seconds,
            }
        )

    if total_estimated_seconds <= split_trigger_seconds or not chapter_items:
        return {
            "split_mode": False,
            "split_trigger_seconds": split_trigger_seconds,
            "part_target_seconds": part_target_seconds,
            "estimated_total_seconds": total_estimated_seconds,
            "parts": [
                {
                    "part_index": 1,
                    "chapter_start_index": chapter_items[0]["source_index"] if chapter_items else 1,
                    "chapter_end_index": chapter_items[-1]["source_index"] if chapter_items else 0,
                    "estimated_duration_seconds": total_estimated_seconds,
                    "items": chapter_items,
                }
            ],
        }

    parts = []
    current_items = []
    current_seconds = 0

    def flush_current():
        nonlocal current_items, current_seconds
        if not current_items:
            return
        parts.append(
            {
                "part_index": len(parts) + 1,
                "chapter_start_index": current_items[0]["source_index"],
                "chapter_end_index": current_items[-1]["source_index"],
                "estimated_duration_seconds": current_seconds,
                "items": current_items,
            }
        )
        current_items = []
        current_seconds = 0

    for item in chapter_items:
        item_seconds = item["estimated_seconds"]
        if current_items and current_seconds + item_seconds > part_target_seconds:
            flush_current()
        current_items.append(item)
        current_seconds += item_seconds
        if item_seconds > part_target_seconds:
            log.warning(
                "章节 %s 预估时长 %s 已超过单片目标时长 %s，将单独作为一个分片处理。",
                item.get("title") or item.get("chapter_id"),
                format_seconds_hhmmss(item_seconds),
                format_seconds_hhmmss(part_target_seconds),
            )
            flush_current()

    flush_current()

    return {
        "split_mode": True,
        "split_trigger_seconds": split_trigger_seconds,
        "part_target_seconds": part_target_seconds,
        "estimated_total_seconds": total_estimated_seconds,
        "parts": parts,
    }


def build_split_plan_signature(chapters_sorted, split_plan):
    payload = {
        "project_flag": PROJECT_FLAG,
        "split_trigger_hours": LONG_AUDIO_SPLIT_TRIGGER_HOURS,
        "part_target_hours": LONG_AUDIO_PART_TARGET_HOURS,
        "enable_deepfilter": ENABLE_DEEPFILTER,
        "enable_bgm_mix": ENABLE_BGM_MIX,
        "enable_video_generation": ENABLE_VIDEO_GENERATION,
        "enable_youtube_upload": ENABLE_YOUTUBE_UPLOAD,
        "video_resolution": VIDEO_RESOLUTION,
        "youtube_channel_name": YOUTUBE_CHANNEL_NAME,
        "chapters": [
            {
                "id": chapter.get("id"),
                "title": chapter.get("title"),
                "long": chapter.get("long"),
            }
            for chapter in chapters_sorted
        ],
        "parts": [
            {
                "part_index": part["part_index"],
                "chapter_start_index": part["chapter_start_index"],
                "chapter_end_index": part["chapter_end_index"],
                "estimated_duration_seconds": part["estimated_duration_seconds"],
                "chapter_ids": [item.get("chapter_id") for item in part.get("items", [])],
            }
            for part in split_plan.get("parts", [])
        ],
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return hashlib.md5(raw).hexdigest()


def write_json_file(path, data):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return path


def read_json_file(path, default=None):
    if not path or not os.path.exists(path):
        return default

    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        log.warning("JSON 读取失败 %s: %s", path, e)
        return default


def _normalize_local_path_for_compare(path):
    text = str(path or "").strip()
    if not text:
        return ""
    return os.path.normcase(os.path.abspath(text))


def _capture_local_file_signature(path):
    normalized_path = _normalize_local_path_for_compare(path)
    signature = {"path": normalized_path}
    if not normalized_path or not os.path.exists(path):
        return signature

    stat = os.stat(path)
    signature["size"] = int(stat.st_size)
    signature["mtime_ns"] = int(getattr(stat, "st_mtime_ns", int(stat.st_mtime * 1_000_000_000)))
    return signature


def persist_youtube_upload_receipt(
    receipt_path,
    video_path,
    upload_result,
    channel_name="",
    title="",
    privacy_status="",
    category_id="",
    schedule_after_hours=0,
):
    if not isinstance(upload_result, dict):
        return ""

    youtube_url = str(upload_result.get("youtube_url") or "").strip()
    video_id = str(upload_result.get("video_id") or "").strip()
    if not youtube_url and not video_id:
        return ""

    payload = {
        "receipt_version": 1,
        "saved_at": dt_module.datetime.now().isoformat(),
        "channel_name": str(channel_name or "").strip(),
        "title": str(title or upload_result.get("title") or "").strip(),
        "privacy_status": str(privacy_status or "").strip(),
        "category_id": str(category_id or "").strip(),
        "schedule_after_hours": int(schedule_after_hours or 0),
        "video_file": _capture_local_file_signature(video_path),
        "video_id": video_id,
        "youtube_url": youtube_url,
        "uploaded_at": str(upload_result.get("uploaded_at") or "").strip(),
        "publish_at": str(upload_result.get("publish_at") or "").strip(),
        "schedule_reason": str(upload_result.get("schedule_reason") or "").strip(),
    }
    write_json_file(receipt_path, payload)
    return receipt_path


def load_youtube_upload_receipt(receipt_path, video_path="", channel_name=""):
    receipt = read_json_file(receipt_path, default={}) or {}
    if not isinstance(receipt, dict) or (not receipt.get("youtube_url") and not receipt.get("video_id")):
        fallback_report = read_json_file(os.path.join(os.path.dirname(receipt_path), "book_result.json"), default={}) or {}
        fallback_result = fallback_report.get("result") if isinstance(fallback_report, dict) else {}
        if isinstance(fallback_result, dict) and (
            str(fallback_result.get("youtube_url") or "").strip() or str(fallback_result.get("youtube_urls") or "").strip()
        ):
            fallback_url = str(fallback_result.get("youtube_url") or "").strip()
            if "\n" in fallback_url:
                fallback_url = fallback_url.splitlines()[0].strip()
            receipt = {
                "channel_name": "",
                "title": str(fallback_result.get("seo_title") or "").strip(),
                "video_file": _capture_local_file_signature(fallback_result.get("video_path")),
                "video_id": "",
                "youtube_url": fallback_url,
                "uploaded_at": str(fallback_result.get("youtube_publish_at") or "").strip(),
                "publish_at": str(fallback_result.get("youtube_publish_at") or "").strip(),
                "schedule_reason": str(fallback_result.get("youtube_schedule_reason") or "").strip(),
            }
    if not isinstance(receipt, dict):
        return {}

    youtube_url = str(receipt.get("youtube_url") or "").strip()
    video_id = str(receipt.get("video_id") or "").strip()
    if not youtube_url and not video_id:
        return {}

    expected_channel = str(channel_name or "").strip()
    receipt_channel = str(receipt.get("channel_name") or "").strip()
    if expected_channel and receipt_channel and receipt_channel != expected_channel:
        return {}

    if video_path:
        current_signature = _capture_local_file_signature(video_path)
        if not current_signature.get("path") or int(current_signature.get("size") or 0) <= 0:
            return {}

        receipt_signature = receipt.get("video_file") or {}
        receipt_path_text = _normalize_local_path_for_compare(receipt_signature.get("path"))
        if receipt_path_text and receipt_path_text != current_signature["path"]:
            return {}

        if receipt_signature.get("size") is not None and int(receipt_signature.get("size") or 0) != int(current_signature.get("size") or 0):
            return {}

        if receipt_signature.get("mtime_ns") is not None and int(receipt_signature.get("mtime_ns") or 0) != int(current_signature.get("mtime_ns") or 0):
            return {}

    return {
        "video_id": video_id,
        "youtube_url": youtube_url,
        "uploaded_at": str(receipt.get("uploaded_at") or "").strip(),
        "publish_at": str(receipt.get("publish_at") or "").strip(),
        "schedule_reason": str(receipt.get("schedule_reason") or "").strip(),
        "title": str(receipt.get("title") or "").strip(),
    }


def get_book_state_table_name():
    return str(BOOK_STATE_TABLE or "book_processing_states").strip() or "book_processing_states"


def get_modelscope_token_table_name():
    return str(globals().get("MODELSCOPE_TOKEN_TABLE", "") or "modelscope_tokens").strip() or "modelscope_tokens"


def get_cloud_runtime_settings_table_name():
    return str(globals().get("CLOUD_RUNTIME_SETTINGS_TABLE", "") or "channel_runtime_settings").strip() or "channel_runtime_settings"


def get_shared_cloud_runtime_scope_key():
    return "__shared__"


def load_modelscope_token_from_supabase(channel_name=None):
    table_name = get_modelscope_token_table_name()
    shared_scope = get_shared_cloud_runtime_scope_key()
    channel = str(channel_name or globals().get("YOUTUBE_CHANNEL_NAME", "") or "").strip()
    table_sql = get_public_table_identifier(table_name)

    try:
        shared_row = execute_postgres_fetchone(
            sql.SQL(
                """
                SELECT token_text
                FROM {}
                WHERE channel_name = %s
                LIMIT 1
                """
            ).format(table_sql),
            (shared_scope,),
        )
        if shared_row:
            return str(shared_row.get("token_text") or "").strip()

        if channel:
            legacy_row = execute_postgres_fetchone(
                sql.SQL(
                    """
                    SELECT token_text
                    FROM {}
                    WHERE channel_name = %s
                    LIMIT 1
                    """
                ).format(table_sql),
                (channel,),
            )
            if legacy_row:
                return str(legacy_row.get("token_text") or "").strip()

        fallback_row = execute_postgres_fetchone(
            sql.SQL(
                """
                SELECT token_text
                FROM {}
                ORDER BY updated_at DESC
                LIMIT 1
                """
            ).format(table_sql)
        )
        if fallback_row:
            return str(fallback_row.get("token_text") or "").strip()
        return ""
    except Exception as e:
        raise RuntimeError(f"从数据库读取 ModelScope Token 失败，请检查表 {table_name}: {e}")


def save_modelscope_token_to_supabase(channel_name, token_text):
    token_value = str(token_text or "").strip()
    table_name = get_modelscope_token_table_name()
    shared_scope = get_shared_cloud_runtime_scope_key()
    table_sql = get_public_table_identifier(table_name)

    if not token_value:
        raise RuntimeError("MODELSCOPE_TOKEN 为空，无法写入数据库")

    try:
        execute_postgres(
            sql.SQL(
                """
                INSERT INTO {} (channel_name, token_text, updated_at)
                VALUES (%s, %s, %s)
                ON CONFLICT (channel_name)
                DO UPDATE SET
                  token_text = EXCLUDED.token_text,
                  updated_at = EXCLUDED.updated_at
                """
            ).format(table_sql),
            (shared_scope, token_value, dt_module.datetime.now().isoformat()),
        )
    except Exception as e:
        raise RuntimeError(f"写入数据库 ModelScope Token 失败，请检查表 {table_name}: {e}")

    return f"postgres:{table_name}:{shared_scope}"


def delete_modelscope_token_from_supabase(channel_name):
    table_name = get_modelscope_token_table_name()
    shared_scope = get_shared_cloud_runtime_scope_key()
    table_sql = get_public_table_identifier(table_name)
    try:
        execute_postgres(
            sql.SQL("DELETE FROM {} WHERE channel_name = %s").format(table_sql),
            (shared_scope,),
        )
        return True
    except Exception as e:
        raise RuntimeError(f"删除数据库 ModelScope Token 失败，请检查表 {table_name}: {e}")


def load_cloud_runtime_setting_from_supabase(channel_name, setting_key):
    key = str(setting_key or "").strip()
    channel = str(channel_name or globals().get("YOUTUBE_CHANNEL_NAME", "") or "").strip()
    shared_scope = get_shared_cloud_runtime_scope_key()
    if not key:
        return ""

    table_name = get_cloud_runtime_settings_table_name()
    table_sql = get_public_table_identifier(table_name)

    try:
        shared_row = execute_postgres_fetchone(
            sql.SQL(
                """
                SELECT setting_value
                FROM {}
                WHERE channel_name = %s AND setting_key = %s
                LIMIT 1
                """
            ).format(table_sql),
            (shared_scope, key),
        )
        if shared_row:
            return str(shared_row.get("setting_value") or "")

        if channel:
            legacy_row = execute_postgres_fetchone(
                sql.SQL(
                    """
                    SELECT setting_value
                    FROM {}
                    WHERE channel_name = %s AND setting_key = %s
                    LIMIT 1
                    """
                ).format(table_sql),
                (channel, key),
            )
            if legacy_row:
                return str(legacy_row.get("setting_value") or "")

        fallback_row = execute_postgres_fetchone(
            sql.SQL(
                """
                SELECT setting_value
                FROM {}
                WHERE setting_key = %s
                ORDER BY updated_at DESC
                LIMIT 1
                """
            ).format(table_sql),
            (key,),
        )
        if fallback_row:
            return str(fallback_row.get("setting_value") or "")
        return ""
    except Exception as e:
        raise RuntimeError(f"从数据库读取云端运行配置 {key} 失败，请检查表 {table_name}: {e}")


def save_cloud_runtime_setting_to_supabase(channel_name, setting_key, setting_value):
    key = str(setting_key or "").strip()
    value = str(setting_value or "")
    table_name = get_cloud_runtime_settings_table_name()
    shared_scope = get_shared_cloud_runtime_scope_key()
    table_sql = get_public_table_identifier(table_name)

    if not key:
        raise RuntimeError("setting_key 为空，无法将云端运行配置写入数据库")

    try:
        execute_postgres(
            sql.SQL(
                """
                INSERT INTO {} (channel_name, setting_key, setting_value, updated_at)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (channel_name, setting_key)
                DO UPDATE SET
                  setting_value = EXCLUDED.setting_value,
                  updated_at = EXCLUDED.updated_at
                """
            ).format(table_sql),
            (shared_scope, key, value, dt_module.datetime.now().isoformat()),
        )
    except Exception as e:
        raise RuntimeError(f"写入数据库云端运行配置 {key} 失败，请检查表 {table_name}: {e}")

    return f"postgres:{table_name}:{shared_scope}:{key}"


def delete_cloud_runtime_setting_from_supabase(channel_name, setting_key):
    key = str(setting_key or "").strip()
    shared_scope = get_shared_cloud_runtime_scope_key()
    if not key:
        return False

    table_name = get_cloud_runtime_settings_table_name()
    table_sql = get_public_table_identifier(table_name)
    try:
        execute_postgres(
            sql.SQL("DELETE FROM {} WHERE channel_name = %s AND setting_key = %s").format(table_sql),
            (shared_scope, key),
        )
        return True
    except Exception as e:
        raise RuntimeError(f"删除数据库云端运行配置 {key} 失败，请检查表 {table_name}: {e}")


def resolve_cloud_text_setting(setting_key, local_value="", source="database", channel_name=None):
    mode = normalize_runtime_source(source, default="database")
    local_text = str(local_value or "")

    if mode not in {"database", "local"}:
        raise RuntimeError(f"{setting_key} 的来源配置只能是 'database' 或 'local'")

    if mode == "local":
        return local_text

    try:
        cloud_value = load_cloud_runtime_setting_from_supabase(channel_name, setting_key)
    except Exception as e:
        log.warning("读取数据库运行配置 %s 失败，当前回退到本地值: %s", setting_key, e)
        return local_text

    if str(cloud_value).strip():
        log.info("已从数据库读取全局共享云端配置 %s", setting_key)
        return str(cloud_value)

    if str(local_text).strip():
        log.warning(
            "数据库中未找到全局共享云端配置 %s，当前运行临时回退到本地值；如需持久保存，请手动开启云端运行配置同步单元",
            setting_key,
        )
        return local_text

    return local_text


def resolve_modelscope_token(channel_name=None):
    source = normalize_runtime_source(globals().get("MODELSCOPE_TOKEN_SOURCE", "database"), default="database")
    local_token = str(globals().get("MODELSCOPE_TOKEN", "") or "").strip()
    channel = str(channel_name or globals().get("YOUTUBE_CHANNEL_NAME", "") or "").strip()

    if source not in {"database", "local"}:
        raise RuntimeError("MODELSCOPE_TOKEN_SOURCE 只能是 'database' 或 'local'")

    if source == "local":
        if not local_token:
            raise RuntimeError("MODELSCOPE_TOKEN_SOURCE=local，但 MODELSCOPE_TOKEN 为空")

        save_cloud_runtime_setting_to_supabase(channel, "MODELSCOPE_TOKEN", local_token)
        save_modelscope_token_to_supabase(channel, local_token)
        log.info("已按本地模式读取 MODELSCOPE_TOKEN，并同步回写到数据库全局共享云端配置")
        return local_token

    cloud_token = ""
    try:
        cloud_token = str(load_cloud_runtime_setting_from_supabase(channel, "MODELSCOPE_TOKEN") or "").strip()
    except Exception as e:
        log.warning("读取云端运行配置表中的 MODELSCOPE_TOKEN 失败，将继续尝试兼容旧表: %s", e)

    if cloud_token:
        log.info("已从数据库读取全局共享 ModelScope Token")
        return cloud_token

    legacy_token = load_modelscope_token_from_supabase(channel)
    if legacy_token:
        try:
            save_cloud_runtime_setting_to_supabase(channel, "MODELSCOPE_TOKEN", legacy_token)
        except Exception as e:
            log.warning("已从旧的 modelscope_tokens 表读到 ModelScope Token，但补写到云端运行配置表失败: %s", e)
        log.info("已从数据库旧表读取 ModelScope Token，并补写到全局共享云端配置表")
        return legacy_token

    if local_token:
        save_cloud_runtime_setting_to_supabase(channel, "MODELSCOPE_TOKEN", local_token)
        save_modelscope_token_to_supabase(channel, local_token)
        log.warning("数据库中未找到全局共享 ModelScope Token，已自动用本地 MODELSCOPE_TOKEN 回填云端")
        return local_token

    raise RuntimeError(
        "数据库中未找到全局共享 ModelScope Token，且本地 MODELSCOPE_TOKEN 也为空，无法继续 AI 生成"
    )


def build_split_state_ref(book_id, project_flag=None):
    flag = str(PROJECT_FLAG if project_flag is None else project_flag).strip()
    return f"postgres:{get_book_state_table_name()}:{flag}:{book_id}"


def _read_bool_runtime_config(name, default=False):
    value = globals().get(name, default)
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _should_cleanup_completed_split_states():
    return _read_bool_runtime_config("CLEANUP_COMPLETED_SPLIT_STATES", False)


def _build_split_part_lookup_key(part_like):
    if not isinstance(part_like, dict):
        return ()

    chapter_ids = part_like.get("chapter_ids") or []
    normalized_ids = tuple(str(item).strip() for item in chapter_ids if str(item).strip())
    if normalized_ids:
        return ("chapter_ids",) + normalized_ids

    start_index = str(part_like.get("chapter_start_index") or "").strip()
    end_index = str(part_like.get("chapter_end_index") or "").strip()
    if start_index or end_index:
        return ("range", start_index, end_index)

    part_index = str(part_like.get("part_index") or "").strip()
    return ("part_index", part_index) if part_index else ()


def _split_part_has_uploaded_video(part_state):
    if not isinstance(part_state, dict):
        return False
    return bool(str(part_state.get("video_id") or "").strip() or str(part_state.get("youtube_url") or "").strip())


def _is_split_playlist_required(part_count):
    return bool(int(part_count or 0) > 1 and ENABLE_YOUTUBE_UPLOAD and str(YOUTUBE_CHANNEL_NAME or "").strip())


def _split_part_is_completed(part_state):
    if not isinstance(part_state, dict):
        return False

    if str(part_state.get("status") or "").strip().lower() == "completed":
        return True

    if ENABLE_YOUTUBE_UPLOAD and str(YOUTUBE_CHANNEL_NAME or "").strip():
        return _split_part_has_uploaded_video(part_state)
    if ENABLE_VIDEO_GENERATION:
        return _is_nonempty_local_file(part_state.get("video_path"))
    return _is_nonempty_local_file(part_state.get("audio_path"))


def _reconcile_split_part_state(part_state):
    if not isinstance(part_state, dict):
        return False
    if not _split_part_is_completed(part_state):
        return False

    changed = False
    if str(part_state.get("status") or "").strip().lower() != "completed":
        part_state["status"] = "completed"
        changed = True
    if not str(part_state.get("completed_at") or "").strip():
        part_state["completed_at"] = dt_module.datetime.now().isoformat()
        changed = True
    if str(part_state.get("last_stage") or "").strip() != "completed":
        part_state["last_stage"] = "completed"
        changed = True
    if str(part_state.get("error") or "").strip():
        part_state["error"] = ""
        changed = True
    return changed


def evaluate_split_completion_state(state):
    if not isinstance(state, dict):
        state = {}

    parts = state.get("parts", [])
    if not isinstance(parts, list):
        parts = []

    for item in parts:
        _reconcile_split_part_state(item)

    part_count = max(1, int(state.get("part_count") or len(parts) or 1))
    completed_part_count = sum(1 for item in parts if _split_part_is_completed(item))
    playlist_state = get_split_playlist_state(state) if state.get("mode") == "split_upload" else {}
    playlist_required = bool(state.get("mode") == "split_upload" and _is_split_playlist_required(part_count))
    playlist_completed = (
        not playlist_required
        or (
            bool(str(playlist_state.get("playlist_id") or "").strip())
            and str(playlist_state.get("status") or "").strip().lower() == "completed"
        )
    )

    return {
        "part_count": part_count,
        "completed_part_count": completed_part_count,
        "all_parts_completed": completed_part_count >= part_count,
        "playlist_required": playlist_required,
        "playlist_completed": playlist_completed,
        "fully_completed": completed_part_count >= part_count and playlist_completed,
    }


def normalize_split_state_from_row(row):
    state = row.get("state_json") or {}
    if isinstance(state, str):
        try:
            state = json.loads(state)
        except Exception:
            state = {}
    if not isinstance(state, dict):
        state = {}
    state = make_json_compatible(state)

    book_id = str(row.get("book_id") or state.get("book_id") or "").strip()
    state["book_id"] = book_id
    state["book_name"] = row.get("book_name") or state.get("book_name", "")
    state["category"] = row.get("category") or state.get("category", "")
    state["pending_resume"] = bool(
        row.get("pending_resume") if row.get("pending_resume") is not None else state.get("pending_resume")
    )
    state["status"] = row.get("state_status") or state.get("status", "")
    state["current_part_index"] = (
        row.get("current_part_index")
        if row.get("current_part_index") is not None
        else state.get("current_part_index")
    )
    state["completed_part_count"] = int(
        row.get("completed_part_count")
        if row.get("completed_part_count") is not None
        else state.get("completed_part_count", 0)
    )
    state["part_count"] = int(row.get("part_count") if row.get("part_count") is not None else state.get("part_count", 0))
    state["updated_at"] = make_json_compatible(row.get("updated_at")) or state.get("updated_at", "")
    state["created_at"] = make_json_compatible(row.get("created_at")) or state.get("created_at", "")
    state["state_path"] = build_split_state_ref(book_id, row.get("project_flag"))
    return state


def load_split_processing_state(book_record):
    table_name = get_book_state_table_name()
    book_id = str(book_record.get("book_id") or "").strip()
    project_flag = str(PROJECT_FLAG or "").strip()

    if not book_id:
        return None

    table_sql = get_public_table_identifier(table_name)
    try:
        row = execute_postgres_fetchone(
            sql.SQL(
                """
                SELECT
                  book_id,
                  project_flag,
                  book_name,
                  category,
                  pending_resume,
                  state_status,
                  current_part_index,
                  completed_part_count,
                  part_count,
                  updated_at,
                  created_at,
                  state_json
                FROM {}
                WHERE book_id = %s AND project_flag = %s
                ORDER BY updated_at DESC
                LIMIT 1
                """
            ).format(table_sql),
            (book_id, project_flag),
        )
        if not row:
            return None
        return normalize_split_state_from_row(row)
    except Exception as e:
        raise RuntimeError(f"从数据库读取断点状态失败，请检查表 {table_name}: {e}")


def _build_split_state_completeness_rank(state):
    if not isinstance(state, dict) or not state:
        return (-1, -1, -1, -1)

    progress = evaluate_split_completion_state(state)
    playlist_state = get_split_playlist_state(state) if state.get("mode") == "split_upload" else {}
    return (
        int(progress.get("completed_part_count") or 0),
        1 if bool(progress.get("playlist_completed")) else 0,
        1 if str(playlist_state.get("playlist_id") or "").strip() else 0,
        1 if str(playlist_state.get("status") or "").strip().lower() == "completed" else 0,
    )


def reload_split_processing_state(book_record, fallback_state=None, book_name=""):
    loaded_state = load_split_processing_state(book_record)
    if not isinstance(loaded_state, dict) or not loaded_state:
        return fallback_state if isinstance(fallback_state, dict) else loaded_state

    if not isinstance(fallback_state, dict) or not fallback_state:
        return loaded_state

    loaded_rank = _build_split_state_completeness_rank(loaded_state)
    fallback_rank = _build_split_state_completeness_rank(fallback_state)
    if loaded_rank < fallback_rank:
        label = str(
            book_name
            or book_record.get("book_name")
            or fallback_state.get("book_name")
            or loaded_state.get("book_name")
            or book_record.get("book_id")
            or "unknown-book"
        ).strip()
        loaded_playlist = get_split_playlist_state(loaded_state) if loaded_state.get("mode") == "split_upload" else {}
        fallback_playlist = (
            get_split_playlist_state(fallback_state) if fallback_state.get("mode") == "split_upload" else {}
        )
        log.warning(
            "[%s] Reloaded split state looks older than the in-memory state; keeping the more complete local state. "
            "loaded_rank=%s local_rank=%s loaded_playlist_id=%s local_playlist_id=%s loaded_playlist_status=%s local_playlist_status=%s",
            label,
            loaded_rank,
            fallback_rank,
            str(loaded_playlist.get("playlist_id") or ""),
            str(fallback_playlist.get("playlist_id") or ""),
            str(loaded_playlist.get("status") or ""),
            str(fallback_playlist.get("status") or ""),
        )
        return fallback_state

    return loaded_state


def _save_split_processing_state_raw(book_record, state):
    now = dt_module.datetime.now().isoformat()
    parts = state.get("parts", [])
    progress = evaluate_split_completion_state(state)
    completed_count = progress["completed_part_count"]
    state["completed_part_count"] = completed_count
    state["part_count"] = progress["part_count"]
    pending_parts = [item.get("part_index") for item in parts if not _split_part_is_completed(item)]
    state["current_part_index"] = pending_parts[0] if pending_parts else None
    state["updated_at"] = now

    if progress["fully_completed"]:
        state["status"] = "completed"
        state["pending_resume"] = False
        state["completed_at"] = state.get("completed_at") or now
    else:
        state["status"] = "in_progress"
        state["pending_resume"] = True
        state.pop("completed_at", None)

    book_id = str(book_record.get("book_id") or state.get("book_id") or "").strip()
    project_flag = str(PROJECT_FLAG or "").strip()
    table_name = get_book_state_table_name()
    table_sql = get_public_table_identifier(table_name)
    state_ref = build_split_state_ref(book_id, project_flag)
    state["state_path"] = state_ref
    state["book_id"] = book_id
    state["book_name"] = book_record.get("book_name") or state.get("book_name", "")
    state["category"] = book_record.get("category") or state.get("category", "")
    state["created_at"] = state.get("created_at") or now
    # Keep the in-memory nested dict/list objects stable so any existing
    # `part_state` / `playlist_state` references remain valid after a save.
    state_json_payload = make_json_compatible(state)

    try:
        execute_postgres(
            sql.SQL(
                """
                INSERT INTO {} (
                  book_id,
                  project_flag,
                  book_name,
                  category,
                  pending_resume,
                  state_status,
                  current_part_index,
                  completed_part_count,
                  part_count,
                  updated_at,
                  created_at,
                  state_json
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (book_id, project_flag)
                DO UPDATE SET
                  book_name = EXCLUDED.book_name,
                  category = EXCLUDED.category,
                  pending_resume = EXCLUDED.pending_resume,
                  state_status = EXCLUDED.state_status,
                  current_part_index = EXCLUDED.current_part_index,
                  completed_part_count = EXCLUDED.completed_part_count,
                  part_count = EXCLUDED.part_count,
                  updated_at = EXCLUDED.updated_at,
                  created_at = EXCLUDED.created_at,
                  state_json = EXCLUDED.state_json
                """
            ).format(table_sql),
            (
                book_id,
                project_flag,
                state["book_name"],
                state["category"],
                bool(state.get("pending_resume", False)),
                state.get("status", "in_progress"),
                state.get("current_part_index"),
                int(state.get("completed_part_count") or 0),
                int(state.get("part_count") or 1),
                state["updated_at"],
                state["created_at"],
                Jsonb(state_json_payload),
            ),
        )
    except Exception as e:
        raise RuntimeError(f"写入数据库断点状态失败，请检查表 {table_name}: {e}")

    return state_ref


def _truncate_split_state_debug_value(value, limit=240):
    text = str(value or "").strip()
    if len(text) <= limit:
        return text
    return text[:limit] + "...<truncated>"


def _extract_youtube_video_id(value):
    text = str(value or "").strip()
    if not text:
        return ""

    if re.fullmatch(r"[A-Za-z0-9_-]{11}", text):
        return text

    try:
        parsed = urlparse(text)
        host = (parsed.netloc or "").lower()
        if "youtu.be" in host:
            return parsed.path.strip("/").split("/")[0]
        if "youtube.com" in host:
            query_id = parse_qs(parsed.query).get("v", [""])[0].strip()
            if query_id:
                return query_id
            parts = [part for part in parsed.path.split("/") if part]
            if "embed" in parts:
                idx = parts.index("embed")
                if idx + 1 < len(parts):
                    return parts[idx + 1]
            if "shorts" in parts:
                idx = parts.index("shorts")
                if idx + 1 < len(parts):
                    return parts[idx + 1]
    except Exception:
        return ""

    return ""


def _fetch_video_rows_by_id_with_client(youtube, video_ids):
    normalized_ids = []
    seen_ids = set()
    for value in video_ids or []:
        video_id = _extract_youtube_video_id(value)
        if not video_id or video_id in seen_ids:
            continue
        seen_ids.add(video_id)
        normalized_ids.append(video_id)

    if not normalized_ids:
        return {}

    rows_by_id = {}
    for row in _fetch_video_status_rows_with_client(youtube, normalized_ids):
        video_id = str(row.get("id") or "").strip()
        if video_id:
            rows_by_id[video_id] = row
    return rows_by_id


def _wait_for_live_video_rows_with_client(youtube, video_ids, max_attempts=3, context_label=""):
    ordered_ids = []
    seen_ids = set()
    for value in video_ids or []:
        video_id = _extract_youtube_video_id(value)
        if not video_id or video_id in seen_ids:
            continue
        seen_ids.add(video_id)
        ordered_ids.append(video_id)

    if not ordered_ids:
        return {}, []

    max_attempts = max(1, int(max_attempts or 1))
    rows_by_id = {}
    missing_ids = list(ordered_ids)
    for attempt_index in range(1, max_attempts + 1):
        rows_by_id = _fetch_video_rows_by_id_with_client(youtube, ordered_ids)
        missing_ids = [video_id for video_id in ordered_ids if video_id not in rows_by_id]
        if not missing_ids or attempt_index >= max_attempts:
            break

        wait_seconds = min(10, 1 + attempt_index)
        if context_label:
            log.warning(
                "[%s] Waiting for YouTube videos to become readable. attempt=%d/%d missing=%s sleep=%ds",
                context_label,
                attempt_index,
                max_attempts,
                ",".join(missing_ids[:10]),
                wait_seconds,
            )
        else:
            log.warning(
                "Waiting for YouTube videos to become readable. attempt=%d/%d missing=%s sleep=%ds",
                attempt_index,
                max_attempts,
                ",".join(missing_ids[:10]),
                wait_seconds,
            )
        time.sleep(wait_seconds)

    return rows_by_id, missing_ids


def _apply_video_match_to_split_part(part_state, match):
    if not isinstance(part_state, dict) or not isinstance(match, dict):
        return False

    changed = False
    old_video_id = str(part_state.get("video_id") or "").strip()
    updated_values = {
        "video_id": str(match.get("video_id") or "").strip(),
        "youtube_url": str(match.get("youtube_url") or "").strip(),
        "uploaded_at": str(match.get("uploaded_at") or "").strip(),
        "publish_at": str(match.get("publish_at") or "").strip(),
        "schedule_reason": str(match.get("schedule_reason") or "").strip(),
    }
    resolved_title = str(match.get("title") or part_state.get("youtube_title") or "").strip()
    if resolved_title:
        updated_values["youtube_title"] = resolved_title

    for key, value in updated_values.items():
        if str(part_state.get(key) or "").strip() == value:
            continue
        part_state[key] = value
        changed = True

    if old_video_id and old_video_id != updated_values.get("video_id", "") and str(part_state.get("playlist_item_id") or "").strip():
        part_state["playlist_item_id"] = ""
        changed = True

    if _split_part_has_uploaded_video(part_state):
        if str(part_state.get("status") or "").strip().lower() != "completed":
            part_state["status"] = "completed"
            changed = True
        if not str(part_state.get("completed_at") or "").strip():
            part_state["completed_at"] = dt_module.datetime.now().isoformat()
            changed = True
        if str(part_state.get("last_stage") or "").strip() != "completed":
            part_state["last_stage"] = "completed"
            changed = True
        if str(part_state.get("error") or "").strip():
            part_state["error"] = ""
            changed = True

    return changed


def _reset_split_part_upload_state(part_state, reason=""):
    if not isinstance(part_state, dict):
        return False

    changed = False
    for key in ["video_id", "youtube_url", "uploaded_at", "publish_at", "schedule_reason", "playlist_item_id"]:
        if not str(part_state.get(key) or "").strip():
            continue
        part_state[key] = ""
        changed = True

    if str(part_state.get("status") or "").strip().lower() != "pending":
        part_state["status"] = "pending"
        changed = True
    if str(part_state.get("completed_at") or "").strip():
        part_state["completed_at"] = ""
        changed = True
    if str(part_state.get("last_stage") or "").strip() != "upload_recovery_pending":
        part_state["last_stage"] = "upload_recovery_pending"
        changed = True

    normalized_reason = str(reason or "").strip()
    if str(part_state.get("error") or "").strip() != normalized_reason:
        part_state["error"] = normalized_reason
        changed = True

    return changed


def _build_expected_split_upload_title(result, book_name, category, part_index, part_count):
    title, _, _ = build_youtube_payload(
        result,
        book_name,
        category,
        youtube_chapters="",
        title_prefix=f"{part_index}-" if int(part_count or 0) > 1 else "",
        part_hint="",
        include_youtube_chapters=False,
        include_part_hint=False,
    )
    return str(title or "").strip()[:100]


def reconcile_split_part_upload_states(result, state, split_plan, book_name, category):
    channel_name = str(YOUTUBE_CHANNEL_NAME or "").strip()
    if not ENABLE_YOUTUBE_UPLOAD or not channel_name:
        return {"changed": False, "recovered": [], "reset": []}

    part_count = len(split_plan.get("parts", [])) or 1
    candidates = []
    candidate_video_ids = []
    changed = False

    for part_plan in split_plan.get("parts", []):
        part_state = get_split_part_state(state, part_plan["part_index"])
        if not isinstance(part_state, dict):
            continue

        current_status = str(part_state.get("status") or "").strip().lower()
        candidate_video_id = _extract_youtube_video_id(part_state.get("video_id")) or _extract_youtube_video_id(part_state.get("youtube_url"))
        has_upload_state = bool(
            candidate_video_id
            or str(part_state.get("youtube_url") or "").strip()
            or str(part_state.get("uploaded_at") or "").strip()
            or current_status == "completed"
        )
        if not has_upload_state:
            continue

        expected_title = str(part_state.get("youtube_title") or "").strip()
        if not expected_title:
            expected_title = _build_expected_split_upload_title(
                result,
                book_name,
                category,
                part_plan["part_index"],
                part_count,
            )
            if expected_title and str(part_state.get("youtube_title") or "").strip() != expected_title:
                part_state["youtube_title"] = expected_title
                changed = True

        candidates.append(
            {
                "part_plan": part_plan,
                "part_state": part_state,
                "candidate_video_id": candidate_video_id,
                "expected_title": expected_title,
            }
        )
        if candidate_video_id:
            candidate_video_ids.append(candidate_video_id)

    if not candidates:
        return {"changed": changed, "recovered": [], "reset": []}

    youtube = authenticate_youtube_from_supabase(channel_name)
    if not youtube:
        return {"changed": changed, "recovered": [], "reset": []}

    live_rows_by_id, _ = _wait_for_live_video_rows_with_client(
        youtube,
        candidate_video_ids,
        max_attempts=2,
        context_label=book_name,
    )

    title_index = None
    recovered = []
    reset = []

    for candidate in candidates:
        part_plan = candidate["part_plan"]
        part_state = candidate["part_state"]
        part_index = int(part_plan["part_index"])
        candidate_video_id = candidate["candidate_video_id"]
        expected_title = candidate["expected_title"]

        if candidate_video_id and candidate_video_id in live_rows_by_id:
            match = _build_existing_video_match_from_row(live_rows_by_id[candidate_video_id])
            if expected_title and not str(match.get("title") or "").strip():
                match["title"] = expected_title
            if _apply_video_match_to_split_part(part_state, match):
                changed = True
            continue

        recovered_match = {}
        if expected_title:
            if title_index is None:
                title_index = _build_channel_video_title_index_with_client(youtube)
            recovered_match = dict(title_index.get(_normalize_youtube_title_key(expected_title), {})) or {}

        if recovered_match:
            old_video_id = candidate_video_id
            if _apply_video_match_to_split_part(part_state, recovered_match):
                changed = True
            new_video_id = str(recovered_match.get("video_id") or "").strip()
            recovered.append((part_index, old_video_id, new_video_id, expected_title))
            log.warning(
                "[%s] Split part %d/%d recovered a stale YouTube upload reference by exact title match. old_video_id=%s new_video_id=%s title=%s",
                book_name,
                part_index,
                part_count,
                old_video_id or "<empty>",
                new_video_id or "<empty>",
                expected_title or "<empty>",
            )
            continue

        missing_reason = (
            f"Missing uploaded YouTube video for split part {part_index}/{part_count}: "
            f"video_id={candidate_video_id or '<empty>'} title={expected_title or '<empty>'}"
        )
        if _reset_split_part_upload_state(part_state, reason=missing_reason):
            changed = True
        reset.append((part_index, candidate_video_id, expected_title))
        log.warning(
            "[%s] Split part %d/%d references a missing YouTube video and will resume from local artifacts before re-upload. video_id=%s title=%s",
            book_name,
            part_index,
            part_count,
            candidate_video_id or "<empty>",
            expected_title or "<empty>",
        )

    return {"changed": changed, "recovered": recovered, "reset": reset}


def _build_split_state_debug_payload(book_record, state):
    safe_state = state if isinstance(state, dict) else {}
    safe_book = book_record if isinstance(book_record, dict) else {}
    progress = evaluate_split_completion_state(safe_state)
    playlist_state = get_split_playlist_state(safe_state) if safe_state.get("mode") == "split_upload" else {}

    parts_summary = []
    for item in safe_state.get("parts", []) or []:
        if not isinstance(item, dict):
            continue
        audio_path = str(item.get("audio_path") or "").strip()
        video_path = str(item.get("video_path") or "").strip()
        parts_summary.append(
            {
                "part_index": item.get("part_index"),
                "status": str(item.get("status") or ""),
                "last_stage": str(item.get("last_stage") or ""),
                "error": _truncate_split_state_debug_value(item.get("error")),
                "has_audio_path": bool(audio_path),
                "has_video_path": bool(video_path),
                "has_video_id": bool(str(item.get("video_id") or "").strip()),
                "has_youtube_url": bool(str(item.get("youtube_url") or "").strip()),
                "audio_file": os.path.basename(audio_path) if audio_path else "",
                "video_file": os.path.basename(video_path) if video_path else "",
                "youtube_title": _truncate_split_state_debug_value(item.get("youtube_title"), limit=120),
            }
        )

    payload = {
        "book_id": str(safe_book.get("book_id") or safe_state.get("book_id") or "").strip(),
        "project_flag": str(PROJECT_FLAG or "").strip(),
        "book_name": str(safe_book.get("book_name") or safe_state.get("book_name") or "").strip(),
        "category": str(safe_book.get("category") or safe_state.get("category") or "").strip(),
        "state_table": str(get_book_state_table_name() or "").strip(),
        "state_status": str(safe_state.get("status") or ""),
        "pending_resume": bool(safe_state.get("pending_resume")),
        "last_stage": str(safe_state.get("last_stage") or ""),
        "last_error": _truncate_split_state_debug_value(safe_state.get("last_error")),
        "current_part_index": safe_state.get("current_part_index"),
        "completed_part_count": progress["completed_part_count"],
        "part_count": progress["part_count"],
        "playlist_required": progress["playlist_required"],
        "playlist_completed": progress["playlist_completed"],
        "playlist_status": str(playlist_state.get("status") or ""),
        "playlist_id": _truncate_split_state_debug_value(playlist_state.get("playlist_id"), limit=80),
        "playlist_url": _truncate_split_state_debug_value(playlist_state.get("playlist_url"), limit=120),
        "parts": parts_summary,
    }
    return make_json_compatible(payload)


def _maybe_log_split_state_persisted(book_record, state, state_ref):
    if not isinstance(state, dict):
        return

    last_stage = str(state.get("last_stage") or "").strip()
    if not last_stage:
        return

    book_name = str(book_record.get("book_name") or state.get("book_name") or state.get("book_id") or "unknown-book").strip()
    progress = evaluate_split_completion_state(state)
    part_count = max(1, int(progress.get("part_count") or 1))
    completed_part_count = max(0, int(progress.get("completed_part_count") or 0))
    last_error = _truncate_split_state_debug_value(state.get("last_error"), limit=160)

    match = re.fullmatch(r"part_(\d+)_(.+)", last_stage)
    if match:
        part_index = int(match.group(1))
        suffix = match.group(2)
        part_state = get_split_part_state(state, part_index) or {}

        if suffix == "upload_persisted":
            if not _split_part_has_uploaded_video(part_state):
                return
            log.info(
                "[%s] 分片 %d/%d 的上传回执已写入数据库续跑状态（进度 %d/%d，state=%s）",
                book_name,
                part_index,
                part_count,
                completed_part_count,
                part_count,
                state_ref,
            )
            return

        if suffix == "completed":
            if not _split_part_is_completed(part_state):
                return
            log.info(
                "[%s] 分片 %d/%d 已处理完成，当前状态已写入数据库（进度 %d/%d，state=%s）",
                book_name,
                part_index,
                part_count,
                completed_part_count,
                part_count,
                state_ref,
            )
            return

        if suffix == "failed":
            if str(part_state.get("status") or "").strip().lower() != "failed":
                return
            log.warning(
                "[%s] 分片 %d/%d 的失败状态已写入数据库（进度 %d/%d，state=%s，error=%s）",
                book_name,
                part_index,
                part_count,
                completed_part_count,
                part_count,
                state_ref,
                last_error,
            )
            return

    if last_stage == "playlist_completed":
        if not progress["playlist_completed"]:
            return
        log.info(
            "[%s] 播放列表完成状态已写入数据库（进度 %d/%d，state=%s）",
            book_name,
            completed_part_count,
            part_count,
            state_ref,
        )
        return

    if last_stage == "playlist_failed":
        log.warning(
            "[%s] 播放列表失败状态已写入数据库（进度 %d/%d，state=%s，error=%s）",
            book_name,
            completed_part_count,
            part_count,
            state_ref,
            last_error,
        )
        return

    if last_stage == "all_parts_completed":
        if not progress["fully_completed"]:
            return
        log.info(
            "[%s] 多 P 最终完成状态已写入数据库（进度 %d/%d，state=%s）",
            book_name,
            completed_part_count,
            part_count,
            state_ref,
        )


def save_split_processing_state(book_record, state):
    try:
        state_ref = _save_split_processing_state_raw(book_record, state)
    except Exception as e:
        debug_payload = _build_split_state_debug_payload(book_record, state)
        debug_text = json.dumps(debug_payload, ensure_ascii=False, sort_keys=True)
        book_label = debug_payload.get("book_name") or debug_payload.get("book_id") or "unknown-book"
        log.error("[%s] 保存续跑状态失败，调试详情: %s", book_label, debug_text)
        log.error("[%s] 保存续跑状态异常堆栈: %s", book_label, traceback.format_exc())
        raise RuntimeError(f"保存续跑状态失败，调试详情: {debug_text} | 原始异常: {e}") from e
    _maybe_log_split_state_persisted(book_record, state, state_ref)
    return state_ref


def delete_split_processing_state(book_record, only_if_completed=False):
    book_id = str(book_record.get("book_id") or "").strip()
    project_flag = str(PROJECT_FLAG or "").strip()
    if not book_id:
        return False

    table_name = get_book_state_table_name()
    table_sql = get_public_table_identifier(table_name)

    try:
        statement = sql.SQL("DELETE FROM {} WHERE book_id = %s AND project_flag = %s").format(table_sql)
        params = [book_id, project_flag]
        if only_if_completed:
            statement += sql.SQL(" AND state_status = %s")
            params.append("completed")
        execute_postgres(statement, tuple(params))
        return True
    except Exception as e:
        raise RuntimeError(f"删除数据库分片状态失败，请检查表 {table_name}: {e}")


def cleanup_completed_split_states(project_flag=None, category=None):
    if not _should_cleanup_completed_split_states():
        return 0

    table_name = get_book_state_table_name()
    table_sql = get_public_table_identifier(table_name)
    flag = str(PROJECT_FLAG if project_flag is None else project_flag).strip()
    category_name = str(TARGET_CATEGORY if category is None else category).strip()
    total_deleted = 0

    while True:
        try:
            statement = sql.SQL(
                """
                SELECT book_id, project_flag
                FROM {}
                WHERE state_status = %s
                """
            ).format(table_sql)
            params = ["completed"]
            if flag:
                statement += sql.SQL(" AND project_flag = %s")
                params.append(flag)
            if category_name:
                statement += sql.SQL(" AND category = %s")
                params.append(category_name)
            statement += sql.SQL(" ORDER BY updated_at ASC LIMIT %s")
            params.append(100)
            rows = execute_postgres_fetchall(statement, tuple(params))
        except Exception as e:
            raise RuntimeError(f"清理数据库已完成分片状态失败，请检查表 {table_name}: {e}")

        if not rows:
            break

        for row in rows:
            current_book_id = str(row.get("book_id") or "").strip()
            current_flag = str(row.get("project_flag") or "").strip()
            if not current_book_id:
                continue
            try:
                deleted = execute_postgres(
                    sql.SQL(
                        """
                        DELETE FROM {}
                        WHERE book_id = %s AND project_flag = %s AND state_status = %s
                        """
                    ).format(table_sql),
                    (current_book_id, current_flag, "completed"),
                )
                if deleted > 0:
                    total_deleted += 1
            except Exception as delete_error:
                log.warning("清理已完成分片状态失败 book_id=%s: %s", current_book_id, delete_error)

        if len(rows) < 100:
            break

    return total_deleted


def apply_cloud_runtime_overrides():
    applied = {}

    try:
        original_dataset_urls = globals().get("HF_DATASET_ZIP_URLS", "")
        resolved_dataset_urls = resolve_cloud_text_setting(
            "HF_DATASET_ZIP_URLS",
            local_value=original_dataset_urls,
            source=globals().get("HF_DATASET_ZIP_URLS_SOURCE", "database"),
        )
        globals()["HF_DATASET_ZIP_URLS"] = resolved_dataset_urls
        if str(resolved_dataset_urls) != str(original_dataset_urls):
            applied["HF_DATASET_ZIP_URLS"] = resolved_dataset_urls
    except Exception as e:
        log.warning("应用云端 HF_DATASET_ZIP_URLS 失败: %s", e)

    try:
        original_bucket_ids = globals().get("BUCKET_IDS", "")
        resolved_bucket_ids = resolve_cloud_text_setting(
            "BUCKET_IDS",
            local_value=original_bucket_ids,
            source=globals().get("BUCKET_IDS_SOURCE", "database"),
        )
        globals()["BUCKET_IDS"] = resolved_bucket_ids
        if str(resolved_bucket_ids) != str(original_bucket_ids):
            applied["BUCKET_IDS"] = resolved_bucket_ids
    except Exception as e:
        log.warning("应用云端 BUCKET_IDS 失败: %s", e)

    return applied


def initialize_split_processing_state(book_record, book_dir, chapters_sorted, split_plan):
    signature = build_split_plan_signature(chapters_sorted, split_plan)
    existing = load_split_processing_state(book_record) or {}

    reuse_existing = isinstance(existing, dict) and existing.get("plan_signature") == signature
    compatible_reuse = isinstance(existing, dict) and bool(existing.get("parts"))
    existing_parts_by_index = {}
    existing_parts_by_key = {}
    if compatible_reuse:
        for item in existing.get("parts", []):
            if not isinstance(item, dict):
                continue
            if str(item.get("part_index", "")).isdigit():
                existing_parts_by_index[int(item.get("part_index"))] = item
            part_key = _build_split_part_lookup_key(item)
            if part_key and part_key not in existing_parts_by_key:
                existing_parts_by_key[part_key] = item

    parts_state = []
    matched_existing_parts = 0
    for part in split_plan.get("parts", []):
        part_key = _build_split_part_lookup_key(
            {
                "part_index": part["part_index"],
                "chapter_start_index": part["chapter_start_index"],
                "chapter_end_index": part["chapter_end_index"],
                "chapter_ids": [item.get("chapter_id") for item in part.get("items", [])],
            }
        )
        previous = {}
        if reuse_existing:
            previous = existing_parts_by_index.get(part["part_index"], {})
        if not previous and part_key:
            previous = existing_parts_by_key.get(part_key, {})
        if previous:
            matched_existing_parts += 1
        parts_state.append(
            {
                "part_index": part["part_index"],
                "chapter_start_index": part["chapter_start_index"],
                "chapter_end_index": part["chapter_end_index"],
                "estimated_duration_seconds": part["estimated_duration_seconds"],
                "chapter_ids": [item.get("chapter_id") for item in part.get("items", [])],
                "status": previous.get("status", "pending") if previous else "pending",
                "started_at": previous.get("started_at", ""),
                "completed_at": previous.get("completed_at", ""),
                "last_stage": previous.get("last_stage", ""),
                "audio_path": previous.get("audio_path", ""),
                "video_path": previous.get("video_path", ""),
                "video_id": previous.get("video_id", ""),
                "uploaded_at": previous.get("uploaded_at", ""),
                "publish_at": previous.get("publish_at", ""),
                "schedule_reason": previous.get("schedule_reason", ""),
                "youtube_url": previous.get("youtube_url", ""),
                "youtube_title": previous.get("youtube_title", ""),
                "youtube_chapters": previous.get("youtube_chapters", ""),
                "playlist_item_id": previous.get("playlist_item_id", ""),
                "error": previous.get("error", ""),
                "actual_duration_seconds": previous.get("actual_duration_seconds", 0),
            }
        )

    structure_compatible = bool(parts_state) and matched_existing_parts == len(parts_state)
    if structure_compatible and compatible_reuse and not reuse_existing:
        log.info("检测到分片结构兼容，虽然计划签名变化，仍继续复用已有多 P 状态以避免重复上传。")
    state = {
        "state_version": 5,
        "mode": "split_upload",
        "book_id": str(book_record.get("book_id", "")),
        "book_name": book_record.get("book_name", ""),
        "category": book_record.get("category", ""),
        "plan_signature": signature,
        "split_trigger_seconds": split_plan.get("split_trigger_seconds"),
        "part_target_seconds": split_plan.get("part_target_seconds"),
        "estimated_total_seconds": split_plan.get("estimated_total_seconds", 0),
        "part_count": len(parts_state),
        "parts": parts_state,
        "shared_assets": existing.get("shared_assets", {}) if structure_compatible else {},
        "playlist": existing.get("playlist", {}) if structure_compatible else {},
        "last_stage": existing.get("last_stage", "plan_ready") if structure_compatible else "plan_ready",
        "last_error": existing.get("last_error", "") if structure_compatible else "",
        "pending_resume": bool(existing.get("pending_resume")) if structure_compatible else True,
        "created_at": existing.get("created_at") if compatible_reuse else dt_module.datetime.now().isoformat(),
    }
    state_ref = save_split_processing_state(book_record, state)
    return state_ref, state


def get_split_part_state(state, part_index):
    for item in state.get("parts", []):
        if int(item.get("part_index", 0)) == int(part_index):
            return item
    return None


def _book_has_project_status(book_record_or_status, project_flag=None):
    flag = str(PROJECT_FLAG if project_flag is None else project_flag).strip()
    if not flag:
        return False

    status_value = book_record_or_status
    if isinstance(book_record_or_status, dict):
        status_value = book_record_or_status.get("status")

    return flag in set(normalize_text_items(status_value))


def list_interrupted_book_states(book_rows_by_id=None):
    states = {}
    table_name = get_book_state_table_name()
    table_sql = get_public_table_identifier(table_name)
    project_flag = str(PROJECT_FLAG or "").strip()
    page_size = 100
    offset = 0
    book_rows_by_id = book_rows_by_id if isinstance(book_rows_by_id, dict) else {}

    while True:
        try:
            statement = sql.SQL(
                """
                SELECT
                  book_id,
                  project_flag,
                  book_name,
                  category,
                  pending_resume,
                  state_status,
                  current_part_index,
                  completed_part_count,
                  part_count,
                  updated_at,
                  created_at,
                  state_json
                FROM {}
                WHERE project_flag = %s
                """
            ).format(table_sql)
            params = [project_flag]
            if TARGET_CATEGORY.strip():
                statement += sql.SQL(" AND category = %s")
                params.append(TARGET_CATEGORY.strip())

            statement += sql.SQL(" ORDER BY updated_at DESC LIMIT %s OFFSET %s")
            params.extend([page_size, offset])
            rows = execute_postgres_fetchall(statement, tuple(params))
        except Exception as e:
            raise RuntimeError(f"查询数据库未完成断点状态失败，请检查表 {table_name}: {e}")

        if not rows:
            break

        for row in rows:
            state = normalize_split_state_from_row(row)
            state_mode = str(state.get("mode") or "").strip().lower()
            if state_mode not in {"split_upload", "standard_upload"}:
                continue

            book_id = str(state.get("book_id") or "").strip()
            if not book_id:
                continue

            book_record = book_rows_by_id.get(book_id, {})
            already_processed = _book_has_project_status(book_record, project_flag=project_flag)

            if state_mode == "standard_upload":
                if already_processed:
                    try:
                        if delete_split_processing_state({"book_id": book_id}, only_if_completed=False):
                            log.info(
                                "[%s] 检测到 books.status 已包含当前频道，已补删残留的单 P book_processing_states。",
                                state.get("book_name") or book_id,
                            )
                    except Exception as delete_error:
                        log.warning(
                            "[%s] books.status 已标记成功，但补删残留单 P book_processing_states 失败: %s",
                            state.get("book_name") or book_id,
                            delete_error,
                        )
                    continue

                existing = states.get(book_id)
                if not existing or str(state.get("updated_at", "")) > str(existing.get("updated_at", "")):
                    states[book_id] = state
                continue

            progress = evaluate_split_completion_state(state)
            state["completed_part_count"] = progress["completed_part_count"]
            state["part_count"] = progress["part_count"]
            state["status"] = "completed" if progress["fully_completed"] else "in_progress"
            state["pending_resume"] = not progress["fully_completed"]
            if already_processed and progress["fully_completed"]:
                try:
                    if delete_split_processing_state({"book_id": book_id}, only_if_completed=False):
                        log.info(
                            "[%s] 检测到 books.status 已包含当前频道，已补删残留的 book_processing_states。",
                            state.get("book_name") or book_id,
                        )
                except Exception as delete_error:
                    log.warning(
                        "[%s] books.status 已标记成功，但补删残留 book_processing_states 失败: %s",
                        state.get("book_name") or book_id,
                        delete_error,
                    )
                continue

            if already_processed:
                log.warning(
                    "[%s] books.status 已包含当前频道，但残留多 P 状态未完成；本次启动将忽略这条残留状态。",
                    state.get("book_name") or book_id,
                )
                continue

            existing = states.get(book_id)
            if not existing or str(state.get("updated_at", "")) > str(existing.get("updated_at", "")):
                states[book_id] = state

        if len(rows) < page_size:
            break
        offset += page_size

    return states


def finalize_book_result(result, book_dir, book_record=None):
    if bool(getattr(result, "skipped", False)):
        result.audio_ready = False
        result.video_ready = False
        result.upload_ready = False
        result.pending_resume = False
        result.success = False
        return result

    part_count = max(1, int(getattr(result, "part_count", 1) or 1))
    completed_part_count = max(0, int(getattr(result, "completed_part_count", 0) or 0))

    if getattr(result, "split_mode", False) or part_count > 1:
        playlist_required = bool(getattr(result, "playlist_required", False))
        playlist_completed = not playlist_required or bool(getattr(result, "playlist_completed", False))
        all_parts_completed = completed_part_count >= part_count

        result.audio_ready = all_parts_completed
        result.video_ready = all_parts_completed if ENABLE_VIDEO_GENERATION else result.audio_ready
        result.upload_ready = (
            all_parts_completed and (not playlist_required or playlist_completed)
            if ENABLE_YOUTUBE_UPLOAD
            else result.video_ready
        )
        computed_pending_resume = (not all_parts_completed) or (playlist_required and not playlist_completed)
        stale_pending_resume = bool(getattr(result, "pending_resume", False)) and not computed_pending_resume
        result.pending_resume = computed_pending_resume
        required_stages = [result.audio_ready]
        if ENABLE_VIDEO_GENERATION:
            required_stages.append(result.video_ready)
        if ENABLE_YOUTUBE_UPLOAD:
            required_stages.append(result.upload_ready)
        result.success = all(required_stages) and all_parts_completed and playlist_completed and not result.pending_resume
        if stale_pending_resume:
            log.warning(
                "[%s] Clearing stale pending_resume during final split evaluation. completed=%d/%d playlist_required=%s playlist_completed=%s state=%s",
                result.book_name,
                completed_part_count,
                part_count,
                playlist_required,
                playlist_completed,
                getattr(result, "state_path", ""),
            )
    else:
        result.audio_ready = bool(result.merged_audio_path and os.path.exists(result.merged_audio_path))
        result.video_ready = bool(result.video_path and os.path.exists(result.video_path))
        result.upload_ready = bool(result.youtube_url)

        required_stages = [result.audio_ready]
        if ENABLE_VIDEO_GENERATION:
            required_stages.append(result.video_ready)
        if ENABLE_YOUTUBE_UPLOAD:
            required_stages.append(result.upload_ready)

        result.success = all(required_stages)

    if not result.success and not result.error:
        if bool(getattr(result, "pending_resume", False)):
            result.error = "长音频分片处理中断，已记录进度，等待下次续跑"
        elif not result.audio_ready:
            result.error = "音频成品未准备完成"
        elif ENABLE_VIDEO_GENERATION and not result.video_ready:
            result.error = "MP4 成品未准备完成"
        elif ENABLE_YOUTUBE_UPLOAD and not result.upload_ready:
            result.error = "YouTube 上传未完成"

    if getattr(result, "split_mode", False) and not result.success:
        log.error(
            "[%s] Split finalization failed: completed_part_count=%d part_count=%d pending_resume=%s playlist_required=%s playlist_completed=%s audio_ready=%s video_ready=%s upload_ready=%s state=%s error=%s",
            result.book_name,
            completed_part_count,
            part_count,
            bool(getattr(result, "pending_resume", False)),
            bool(getattr(result, "playlist_required", False)),
            bool(getattr(result, "playlist_completed", False)),
            bool(getattr(result, "audio_ready", False)),
            bool(getattr(result, "video_ready", False)),
            bool(getattr(result, "upload_ready", False)),
            getattr(result, "state_path", ""),
            str(getattr(result, "error", "") or ""),
        )

    report = {
        "generated_at": dt_module.datetime.now().isoformat(),
        "book_dir": book_dir,
        "result": dict(result.__dict__),
    }
    if book_record is not None:
        report["source"] = {
            "book_id": book_record.get("book_id"),
            "book_name": book_record.get("book_name"),
            "category": book_record.get("category"),
        }

    report_path = os.path.join(book_dir, "book_result.json")
    try:
        write_json_file(report_path, report)
    except Exception as e:
        log.warning("单书结果写入失败: %s", e)

    log.info("🏆 本书《%s》全程线走完。状态：%s", result.book_name, "✅" if result.success else "❌")
    return result


def collect_runtime_config_snapshot():
    return {
        "database_backend": "postgresql",
        "postgres_dsn_configured": bool(get_postgres_dsn(optional=True)),
        "project_flag": PROJECT_FLAG,
        "target_category": TARGET_CATEGORY,
        "max_process_count": MAX_PROCESS_COUNT,
        "max_runtime_hours": MAX_RUNTIME_HOURS,
        "stop_buffer_minutes": STOP_BUFFER_MINUTES,
        "long_audio_split_trigger_hours": LONG_AUDIO_SPLIT_TRIGGER_HOURS,
        "long_audio_part_target_hours": LONG_AUDIO_PART_TARGET_HOURS,
        "book_state_table": BOOK_STATE_TABLE,
        "prioritize_interrupted_books": PRIORITIZE_INTERRUPTED_BOOKS,
        "output_root": OUTPUT_ROOT,
        "download_workers": DOWNLOAD_WORKERS,
        "audio_download_connect_timeout": AUDIO_DOWNLOAD_CONNECT_TIMEOUT,
        "audio_download_read_timeout": AUDIO_DOWNLOAD_READ_TIMEOUT,
        "audio_download_max_retry_attempts": AUDIO_DOWNLOAD_MAX_RETRY_ATTEMPTS,
        "audio_download_max_total_seconds": AUDIO_DOWNLOAD_MAX_TOTAL_SECONDS,
        "audio_download_stuck_log_interval_seconds": AUDIO_DOWNLOAD_STUCK_LOG_INTERVAL_SECONDS,
        "hf_music_download_enabled": DOWNLOAD_FROM_BUCKETS,
        "hf_music_download_method": HF_MUSIC_DOWNLOAD_METHOD,
        "enable_deepfilter": ENABLE_DEEPFILTER,
        "deepfilter_workers": DEEPFILTER_WORKERS,
        "enable_bgm_mix": ENABLE_BGM_MIX,
        "music_dir": MUSIC_DIR,
        "enable_cover_generation": ENABLE_COVER_GENERATION,
        "cloud_runtime_settings_table": CLOUD_RUNTIME_SETTINGS_TABLE,
        "modelscope_token_source": normalize_runtime_source(MODELSCOPE_TOKEN_SOURCE, default="database"),
        "modelscope_token_table": MODELSCOPE_TOKEN_TABLE,
        "modelscope_image_connect_timeout": MODELSCOPE_IMAGE_CONNECT_TIMEOUT,
        "modelscope_image_read_timeout": MODELSCOPE_IMAGE_READ_TIMEOUT,
        "modelscope_image_poll_connect_timeout": MODELSCOPE_IMAGE_POLL_CONNECT_TIMEOUT,
        "modelscope_image_poll_read_timeout": MODELSCOPE_IMAGE_POLL_READ_TIMEOUT,
        "modelscope_token_switch_delay_seconds": MODELSCOPE_TOKEN_SWITCH_DELAY_SECONDS,
        "enable_seo_generation": ENABLE_SEO_GENERATION,
        "hf_dataset_zip_urls_source": normalize_runtime_source(HF_DATASET_ZIP_URLS_SOURCE, default="database"),
        "bucket_ids_source": normalize_runtime_source(BUCKET_IDS_SOURCE, default="database"),
        "enable_video_generation": ENABLE_VIDEO_GENERATION,
        "enable_youtube_upload": ENABLE_YOUTUBE_UPLOAD,
        "youtube_channel_name": YOUTUBE_CHANNEL_NAME,
        "youtube_privacy_status": YOUTUBE_PRIVACY_STATUS,
        "youtube_schedule_after_hours": YOUTUBE_SCHEDULE_AFTER_HOURS,
        "youtube_schedule_local_timezone": "Asia/Shanghai",
        "youtube_daily_publish_limit": YOUTUBE_DAILY_PUBLISH_LIMIT,
    }


def save_run_summary(output_root, results, archive=True, extra=None):
    report_dir = os.path.join(output_root, "_run_reports")
    timestamp = dt_module.datetime.now().strftime("%Y%m%d_%H%M%S")
    success_items = [r for r in results if r.success]
    partial_items = [r for r in results if getattr(r, "pending_resume", False)]
    skipped_items = [r for r in results if getattr(r, "skipped", False)]
    failed_items = [
        r for r in results if not r.success and not getattr(r, "pending_resume", False) and not getattr(r, "skipped", False)
    ]
    summary = {
        "generated_at": dt_module.datetime.now().isoformat(),
        "config": collect_runtime_config_snapshot(),
        "total": len(results),
        "success": len(success_items),
        "partial": len(partial_items),
        "skipped": len(skipped_items),
        "failed": len(failed_items),
        "success_items": [
            {
                "book_id": r.book_id,
                "book_name": r.book_name,
                "youtube_url": r.youtube_url,
                "publish_at": getattr(r, "youtube_publish_at", ""),
                "schedule_reason": getattr(r, "youtube_schedule_reason", ""),
                "video_path": r.video_path,
            }
            for r in success_items
        ],
        "partial_items": [
            {
                "book_id": r.book_id,
                "book_name": r.book_name,
                "error": r.error,
                "state_ref": getattr(r, "state_path", ""),
                "completed_part_count": getattr(r, "completed_part_count", 0),
                "part_count": getattr(r, "part_count", 1),
            }
            for r in partial_items
        ],
        "skipped_items": [
            {
                "book_id": r.book_id,
                "book_name": r.book_name,
                "reason": getattr(r, "skipped_reason", "") or r.error,
                "deleted_from_books": bool(getattr(r, "deleted_from_books", False)),
            }
            for r in skipped_items
        ],
        "failed_items": [
            {
                "book_id": r.book_id,
                "book_name": r.book_name,
                "error": r.error,
            }
            for r in failed_items
        ],
        "items": [dict(r.__dict__) for r in results],
    }
    if extra:
        summary["runtime"] = extra

    latest_path = os.path.join(report_dir, "latest_run_summary.json")
    write_json_file(latest_path, summary)
    if archive:
        archive_path = os.path.join(report_dir, f"run_summary_{timestamp}.json")
        write_json_file(archive_path, summary)
        log.info("🧾 运行汇总已写入: %s", archive_path)
        return archive_path

    log.info("🧾 运行进度已更新: %s", latest_path)
    return latest_path


def get_remaining_runtime_seconds(run_started_at):
    try:
        budget_hours = float(MAX_RUNTIME_HOURS or 0)
    except Exception:
        budget_hours = 0

    if budget_hours <= 0:
        return None

    return budget_hours * 3600 - (time.time() - run_started_at)


def should_stop_before_next_book(run_started_at):
    remaining = get_remaining_runtime_seconds(run_started_at)
    if remaining is None:
        return False, None

    try:
        buffer_seconds = max(0, int(STOP_BUFFER_MINUTES or 0) * 60)
    except Exception:
        buffer_seconds = 0

    return remaining <= buffer_seconds, remaining


def validate_runtime_config():
    errors = []
    warnings = []
    ai_features_enabled = bool(ENABLE_COVER_GENERATION or ENABLE_SEO_GENERATION)
    modelscope_token_source = normalize_runtime_source(MODELSCOPE_TOKEN_SOURCE, default="database")
    local_modelscope_token = str(MODELSCOPE_TOKEN or "").strip()
    hf_dataset_zip_urls_source = normalize_runtime_source(HF_DATASET_ZIP_URLS_SOURCE, default="database")
    bucket_ids_source = normalize_runtime_source(BUCKET_IDS_SOURCE, default="database")

    globals()["MODELSCOPE_TOKEN_SOURCE"] = modelscope_token_source
    globals()["HF_DATASET_ZIP_URLS_SOURCE"] = hf_dataset_zip_urls_source
    globals()["BUCKET_IDS_SOURCE"] = bucket_ids_source

    if not get_postgres_dsn(optional=True):
        errors.append("POSTGRES_DSN 为空")
    if not str(OUTPUT_ROOT).strip():
        errors.append("OUTPUT_ROOT 为空")
    if not str(BOOK_STATE_TABLE).strip():
        errors.append("BOOK_STATE_TABLE 为空")
    if not str(CLOUD_RUNTIME_SETTINGS_TABLE).strip():
        errors.append("CLOUD_RUNTIME_SETTINGS_TABLE 为空")
    try:
        runtime_hours = float(MAX_RUNTIME_HOURS or 0)
    except Exception:
        runtime_hours = 0
    if runtime_hours >= 12:
        warnings.append("Colab 单次常见上限约 12 小时，建议 MAX_RUNTIME_HOURS 小于 12，给收尾留缓冲")
    try:
        split_trigger_hours = float(LONG_AUDIO_SPLIT_TRIGGER_HOURS or 12.0)
    except Exception:
        split_trigger_hours = 12.0
    try:
        part_target_hours = float(LONG_AUDIO_PART_TARGET_HOURS or 11.8)
    except Exception:
        part_target_hours = 11.8
    if split_trigger_hours <= 0:
        errors.append("LONG_AUDIO_SPLIT_TRIGGER_HOURS 必须大于 0")
    if part_target_hours <= 0:
        errors.append("LONG_AUDIO_PART_TARGET_HOURS 必须大于 0")
    if part_target_hours > split_trigger_hours:
        warnings.append("LONG_AUDIO_PART_TARGET_HOURS 大于触发阈值，建议设成略小于 12 小时更稳")
    if ENABLE_YOUTUBE_UPLOAD and not str(YOUTUBE_CHANNEL_NAME).strip():
        errors.append("已开启 YouTube 上传，但 YOUTUBE_CHANNEL_NAME 为空")
    if str(OUTPUT_ROOT).strip().startswith("/content") and "/drive/" not in str(OUTPUT_ROOT).strip():
        warnings.append("当前 OUTPUT_ROOT 位于 Colab 临时盘，断线或重启后文件会丢；长期自用更建议改到 Google Drive 路径")
    try:
        audio_connect_timeout = int(AUDIO_DOWNLOAD_CONNECT_TIMEOUT or 0)
    except Exception:
        audio_connect_timeout = 0
    try:
        audio_read_timeout = int(AUDIO_DOWNLOAD_READ_TIMEOUT or 0)
    except Exception:
        audio_read_timeout = 0
    try:
        audio_max_attempts = int(AUDIO_DOWNLOAD_MAX_RETRY_ATTEMPTS or 0)
    except Exception:
        audio_max_attempts = 0
    try:
        audio_max_total_seconds = int(AUDIO_DOWNLOAD_MAX_TOTAL_SECONDS or 0)
    except Exception:
        audio_max_total_seconds = 0
    try:
        audio_stuck_log_interval = int(AUDIO_DOWNLOAD_STUCK_LOG_INTERVAL_SECONDS or 0)
    except Exception:
        audio_stuck_log_interval = 0
    try:
        modelscope_image_connect_timeout = int(MODELSCOPE_IMAGE_CONNECT_TIMEOUT or 0)
    except Exception:
        modelscope_image_connect_timeout = 0
    try:
        modelscope_image_read_timeout = int(MODELSCOPE_IMAGE_READ_TIMEOUT or 0)
    except Exception:
        modelscope_image_read_timeout = 0
    try:
        modelscope_image_poll_connect_timeout = int(MODELSCOPE_IMAGE_POLL_CONNECT_TIMEOUT or 0)
    except Exception:
        modelscope_image_poll_connect_timeout = 0
    try:
        modelscope_image_poll_read_timeout = int(MODELSCOPE_IMAGE_POLL_READ_TIMEOUT or 0)
    except Exception:
        modelscope_image_poll_read_timeout = 0
    if audio_connect_timeout <= 0:
        errors.append("AUDIO_DOWNLOAD_CONNECT_TIMEOUT 必须大于 0")
    if audio_read_timeout <= 0:
        errors.append("AUDIO_DOWNLOAD_READ_TIMEOUT 必须大于 0")
    if audio_max_attempts <= 0:
        errors.append("AUDIO_DOWNLOAD_MAX_RETRY_ATTEMPTS 必须大于 0")
    if audio_max_total_seconds <= 0:
        errors.append("AUDIO_DOWNLOAD_MAX_TOTAL_SECONDS 必须大于 0")
    if audio_stuck_log_interval <= 0:
        errors.append("AUDIO_DOWNLOAD_STUCK_LOG_INTERVAL_SECONDS 必须大于 0")
    if modelscope_image_connect_timeout <= 0:
        errors.append("MODELSCOPE_IMAGE_CONNECT_TIMEOUT 蹇呴』澶т簬 0")
    if modelscope_image_read_timeout <= 0:
        errors.append("MODELSCOPE_IMAGE_READ_TIMEOUT 蹇呴』澶т簬 0")
    if modelscope_image_poll_connect_timeout <= 0:
        errors.append("MODELSCOPE_IMAGE_POLL_CONNECT_TIMEOUT 蹇呴』澶т簬 0")
    if modelscope_image_poll_read_timeout <= 0:
        errors.append("MODELSCOPE_IMAGE_POLL_READ_TIMEOUT 蹇呴』澶т簬 0")
    music_download_method = str(HF_MUSIC_DOWNLOAD_METHOD or "datasets_zip_urls").strip().lower()
    if DOWNLOAD_FROM_BUCKETS:
        if hf_dataset_zip_urls_source not in {"database", "local"}:
            errors.append("HF_DATASET_ZIP_URLS_SOURCE 只能是 database 或 local")
        if bucket_ids_source not in {"database", "local"}:
            errors.append("BUCKET_IDS_SOURCE 只能是 database 或 local")
        if music_download_method not in {"datasets_zip_urls", "buckets"}:
            errors.append("HF_MUSIC_DOWNLOAD_METHOD 只能是 datasets_zip_urls 或 buckets")
        elif music_download_method == "datasets_zip_urls":
            if hf_dataset_zip_urls_source == "local" and not str(HF_DATASET_ZIP_URLS).strip():
                warnings.append("已开启 Hugging Face 音乐下载，但 HF_DATASET_ZIP_URLS 为空；音乐下载阶段会跳过")
            elif hf_dataset_zip_urls_source == "database" and not str(HF_DATASET_ZIP_URLS).strip():
                warnings.append(
                    f"HF_DATASET_ZIP_URLS 默认读云端，且本地值为空；请确保数据库的 {CLOUD_RUNTIME_SETTINGS_TABLE} 表里已写入全局共享 HF_DATASET_ZIP_URLS"
                )
        else:
            bucket_ids = [x.strip() for x in str(BUCKET_IDS or "").split(",") if x.strip()]
            if bucket_ids_source == "local" and not bucket_ids:
                warnings.append("已选择 buckets 下载模式，但 BUCKET_IDS 为空；音乐下载阶段会跳过")
            elif bucket_ids_source == "database" and not bucket_ids:
                warnings.append(
                    f"BUCKET_IDS 默认读云端，且本地值为空；请确保数据库的 {CLOUD_RUNTIME_SETTINGS_TABLE} 表里已写入全局共享 BUCKET_IDS"
                )
    if ENABLE_BGM_MIX and not DOWNLOAD_FROM_BUCKETS:
        music_dir = str(MUSIC_DIR).strip()
        if not music_dir or not os.path.exists(music_dir):
            warnings.append("已开启 BGM 混音，但本地 MUSIC_DIR 不存在；若不下载音乐库则混音阶段会跳过")
    if ai_features_enabled:
        if modelscope_token_source not in {"database", "local"}:
            errors.append("MODELSCOPE_TOKEN_SOURCE 只能是 database 或 local")
        if not str(MODELSCOPE_TOKEN_TABLE).strip():
            errors.append("启用 AI 生成时，MODELSCOPE_TOKEN_TABLE 不能为空")
        if modelscope_token_source == "local" and not local_modelscope_token:
            errors.append("MODELSCOPE_TOKEN_SOURCE=local，但 MODELSCOPE_TOKEN 为空")
        if modelscope_token_source == "database" and not local_modelscope_token:
            warnings.append(
                f"MODELSCOPE_TOKEN_SOURCE=database 且本地 MODELSCOPE_TOKEN 为空；请确保数据库的 {MODELSCOPE_TOKEN_TABLE} 或 {CLOUD_RUNTIME_SETTINGS_TABLE} 表中已写入全局共享 token"
            )
    if str(YOUTUBE_PRIVACY_STATUS).strip().lower() == "schedule":
        try:
            hours = int(YOUTUBE_SCHEDULE_AFTER_HOURS or 0)
        except Exception:
            hours = 0
        if hours <= 0:
            warnings.append("YOUTUBE_PRIVACY_STATUS=schedule 但预约小时数不大于 0，将回退到最小值 1")

    for msg in warnings:
        log.warning("配置提醒：%s", msg)

    if errors:
        raise ValueError("；".join(errors))

    log.info("✅ 运行配置校验通过")


import os
import glob
import random
import math
from functools import lru_cache
import numpy as np
from scipy.signal import butter, sosfilt, stft, istft
from pydub import AudioSegment


@lru_cache(maxsize=8)
def load_music_segment_cached(music_path):
    """缓存少量 BGM 源文件，减少长批处理中重复解码的开销。"""
    return AudioSegment.from_file(music_path)


def analyze_audio(audio_segment):
    duration_ms = len(audio_segment)
    rms_dbfs = audio_segment.dBFS
    peak_dbfs = audio_segment.max_dBFS

    chunk_size_ms = 500
    chunks = [audio_segment[i:i + chunk_size_ms]
              for i in range(0, duration_ms, chunk_size_ms)
              if i + chunk_size_ms <= duration_ms]

    chunk_levels = []
    for chunk in chunks:
        try:
            level = chunk.dBFS
            if level > -60:
                chunk_levels.append(level)
        except Exception:
            pass
    dynamic_range_db = (max(chunk_levels) - min(chunk_levels)) if len(chunk_levels) >= 2 else 0
    return {
        "rms_dbfs": rms_dbfs, "peak_dbfs": peak_dbfs,
        "dynamic_range_db": dynamic_range_db, "duration_ms": duration_ms,
        "sample_rate": audio_segment.frame_rate, "channels": audio_segment.channels,
    }


def compute_volume_envelope(audio_segment, window_ms=200):
    duration_ms = len(audio_segment)
    envelope = []
    for i in range(0, duration_ms, window_ms):
        chunk = audio_segment[i:i + window_ms]
        if len(chunk) < 50:
            envelope.append(envelope[-1] if envelope else -60)
            continue
        try:
            level = max(chunk.dBFS, -60)
            envelope.append(level)
        except Exception:
            envelope.append(-60)
    return np.array(envelope), window_ms


def analyze_spectral_gaps(audio_segment, n_bands=8):
    sample_rate = audio_segment.frame_rate
    samples = np.array(audio_segment.get_array_of_samples(), dtype=np.float64)
    if audio_segment.channels > 1:
        samples = samples.reshape((-1, audio_segment.channels)).mean(axis=1)

    max_val = 2 ** (audio_segment.sample_width * 8 - 1)
    samples = samples / max_val
    nperseg = min(4096, len(samples))
    freqs, times, Zxx = stft(samples, fs=sample_rate, nperseg=nperseg)
    power = np.abs(Zxx) ** 2

    nyquist = sample_rate / 2
    max_freq = min(nyquist, 16000)
    band_edges = np.logspace(np.log10(150), np.log10(max_freq), n_bands + 1)

    band_energies = []
    for i in range(n_bands):
        mask = (freqs >= band_edges[i]) & (freqs < band_edges[i + 1])
        band_energies.append(power[mask].mean() if mask.any() else 1e-10)

    band_energies_db = 10 * np.log10(np.array(band_energies) + 1e-10)
    max_energy_db = band_energies_db.max()
    relative_db = band_energies_db - max_energy_db
    band_gains = np.clip(-relative_db * 0.3, 0, 6)
    return band_gains, band_edges


def apply_highpass_filter(audio_segment, cutoff_freq=150, order=4):
    sample_rate = audio_segment.frame_rate
    channels = audio_segment.channels
    samples = np.array(audio_segment.get_array_of_samples(), dtype=np.float64)
    if channels > 1:
        samples = samples.reshape((-1, channels))

    nyquist = sample_rate / 2.0
    sos = butter(order, min(cutoff_freq / nyquist, 0.99), btype='high', output='sos')

    if channels > 1:
        filtered = np.zeros_like(samples)
        for ch in range(channels):
            filtered[:, ch] = sosfilt(sos, samples[:, ch])
        filtered = filtered.flatten()
    else:
        filtered = sosfilt(sos, samples)

    max_val = 2 ** (audio_segment.sample_width * 8 - 1) - 1
    filtered = np.clip(filtered, -max_val, max_val).astype(
        np.int16 if audio_segment.sample_width == 2 else np.int32)

    return AudioSegment(data=filtered.tobytes(), sample_width=audio_segment.sample_width,
                        frame_rate=sample_rate, channels=channels)


def _shape_single_channel(samples, sample_rate, band_gains, band_edges):
    nperseg = min(4096, len(samples))
    freqs, times, Zxx = stft(samples, fs=sample_rate, nperseg=nperseg)

    gain_curve = np.ones(len(freqs))
    for i in range(len(band_gains)):
        mask = (freqs >= band_edges[i]) & (freqs < band_edges[i + 1])
        gain_curve[mask] = 10 ** (band_gains[i] / 20.0)

    Zxx_shaped = Zxx * gain_curve[:, np.newaxis]
    _, result = istft(Zxx_shaped, fs=sample_rate, nperseg=nperseg)

    if len(result) > len(samples):
        result = result[:len(samples)]
    elif len(result) < len(samples):
        result = np.pad(result, (0, len(samples) - len(result)))
    return result


def apply_spectral_shaping(audio_segment, band_gains, band_edges):
    sample_rate = audio_segment.frame_rate
    channels = audio_segment.channels
    samples = np.array(audio_segment.get_array_of_samples(), dtype=np.float64)

    if channels > 1:
        samples = samples.reshape((-1, channels))
        result_channels = [_shape_single_channel(samples[:, ch], sample_rate, band_gains, band_edges)
                           for ch in range(channels)]
        result = np.column_stack(result_channels).flatten()
    else:
        result = _shape_single_channel(samples, sample_rate, band_gains, band_edges)

    max_val = 2 ** (audio_segment.sample_width * 8 - 1) - 1
    result = np.clip(result, -max_val, max_val).astype(
        np.int16 if audio_segment.sample_width == 2 else np.int32)

    return AudioSegment(data=result.tobytes(), sample_width=audio_segment.sample_width,
                        frame_rate=sample_rate, channels=channels)


def apply_dynamic_volume(audio_segment, volume_envelope, window_ms, vol_offset_db=-25, min_vol_db=-40):
    duration_ms = len(audio_segment)
    envelope_median = np.median(volume_envelope)

    chunks = []
    for i, env_level in enumerate(volume_envelope):
        start_ms = i * window_ms
        end_ms = min(start_ms + window_ms, duration_ms)
        if start_ms >= duration_ms: break

        chunk = audio_segment[start_ms:end_ms]
        if len(chunk) < 10: continue

        deviation = env_level - envelope_median
        dynamic_adjust = np.clip(deviation * 0.4, -6, 6)
        target_volume = max(env_level + vol_offset_db + dynamic_adjust, min_vol_db)

        try:
            gain = np.clip(target_volume - chunk.dBFS, -40, 10)
            chunk = chunk.apply_gain(gain)
        except Exception:
            pass
        chunks.append(chunk)

    if not chunks: return audio_segment

    # [核心修复]: 一次性合并基于底层内存序列，无损杜绝 O(N^2) OOM 溢出及其引发的极长计算耗时
    raw_data = b"".join([c.raw_data for c in chunks])
    result = audio_segment._spawn(raw_data)

    if len(result) > duration_ms:
        result = result[:duration_ms]
    elif len(result) < duration_ms:
        result += AudioSegment.silent(duration=duration_ms - len(result),
                                      frame_rate=audio_segment.frame_rate)
    return result


def apply_stereo_offset(audio_segment, offset=0.3):
    if audio_segment.channels < 2:
        audio_segment = audio_segment.set_channels(2)

    samples = np.array(audio_segment.get_array_of_samples(), dtype=np.float64).reshape((-1, 2))
    left_gain = (1.0 - offset * 0.5) if offset > 0 else 1.0
    right_gain = 1.0 if offset > 0 else (1.0 + offset * 0.5)

    samples[:, 0] *= left_gain
    samples[:, 1] *= right_gain

    max_val = 2 ** (audio_segment.sample_width * 8 - 1) - 1
    result = np.clip(samples.flatten(), -max_val, max_val).astype(
        np.int16 if audio_segment.sample_width == 2 else np.int32)

    return AudioSegment(data=result.tobytes(), sample_width=audio_segment.sample_width,
                        frame_rate=audio_segment.frame_rate, channels=2)


def get_all_music_files(music_folder):
    supported_extensions = ("*.mp3", "*.wav", "*.flac", "*.ogg", "*.m4a", "*.aac", "*.wma")
    music_files = []
    for ext in supported_extensions:
        music_files.extend(glob.glob(os.path.join(music_folder, ext)))
        music_files.extend(glob.glob(os.path.join(music_folder, ext.upper())))
    music_files = list(set(music_files))
    if not music_files:
        raise FileNotFoundError(f"未找到可选的音乐文件: {music_folder}")
    return music_files


def prepare_copyright_music(music_files, target_duration_ms, original_audio,
                            original_analysis, vol_offset_db, hp_freq, fade_ms,
                            min_vol_db, dyn_vol, spec_shape, st_offset):
    log.info("🎞 开启随机连串版权音乐模式")

    # 全局分析一次原声的频谱空隙
    global_bg, global_be = None, None
    if spec_shape:
        log.info("  全局频谱空袭分析与嵌入检测")
        global_bg, global_be = analyze_spectral_gaps(original_audio)

    # 随机打乱音乐库
    shuffled_files = list(music_files)
    random.shuffle(shuffled_files)

    log.info("  BGM 随机拼接池大小: %d 首 | 目标: %d s", len(shuffled_files), target_duration_ms // 1000)

    looped = AudioSegment.empty()
    music_idx = 0

    while len(looped) < target_duration_ms:
        music_path = shuffled_files[music_idx % len(shuffled_files)]
        music_idx += 1

        segment = load_music_segment_cached(music_path)
        segment_duration = len(segment)

        if hp_freq > 0:
            segment = apply_highpass_filter(segment, cutoff_freq=hp_freq)
        if spec_shape:
            segment = apply_spectral_shaping(segment, global_bg, global_be)

        remaining = target_duration_ms - len(looped)

        if remaining < segment_duration:
            segment = segment[:remaining]
            segment = segment.fade_out(min(fade_ms, remaining // 4))
        else:
            segment = segment.fade_out(min(fade_ms, segment_duration // 4))

        if len(looped) > 0 and fade_ms > 0:
            afade = min(fade_ms, len(segment) // 4)
            if afade > 0:
                segment = segment.fade_in(afade)
                looped = looped.fade_out(afade)

        looped += segment

    looped = looped[:target_duration_ms]

    if dyn_vol:
        log.info("  全局动态音量包络跟踪")
        env, w_ms = compute_volume_envelope(original_audio)
        looped = apply_dynamic_volume(looped, env, w_ms, vol_offset_db, min_vol_db)
    else:
        target_volume = max(original_analysis["rms_dbfs"] + vol_offset_db, min_vol_db)
        looped = looped.apply_gain(target_volume - looped.dBFS)

    final_fade = min(fade_ms, target_duration_ms // 10)
    if final_fade > 100:
        looped = looped.fade_in(final_fade).fade_out(final_fade)

    if st_offset != 0.0:
        log.info("  立体声偏移: %.1f", st_offset)
        looped = apply_stereo_offset(looped, offset=st_offset)

    return looped


def mix_with_bgm(
    input_path: str, output_path: str, music_dir: str,
    *, volume_offset_db=-25, highpass_freq=150, fade_duration_ms=3000,
    min_volume_db=-40, dyn_vol=True, spec_shape=True, stereo_offset=0.0
) -> bool:
    try:
        music_files = get_all_music_files(music_dir)
        log.info("加载原音频: %s", os.path.basename(input_path))
        orig_audio = AudioSegment.from_file(input_path)

        analysis = analyze_audio(orig_audio)
        bgm_music = prepare_copyright_music(
            music_files, len(orig_audio), orig_audio, analysis, volume_offset_db,
            highpass_freq, fade_duration_ms, min_volume_db, dyn_vol, spec_shape, stereo_offset
        )

        # Format Alignment
        if orig_audio.frame_rate != bgm_music.frame_rate:
            bgm_music = bgm_music.set_frame_rate(orig_audio.frame_rate)
        if orig_audio.channels != bgm_music.channels:
            bgm_music = bgm_music.set_channels(orig_audio.channels)
        if len(bgm_music) > len(orig_audio):
            bgm_music = bgm_music[:len(orig_audio)]
        elif len(bgm_music) < len(orig_audio):
            bgm_music += AudioSegment.silent(duration=len(orig_audio)-len(bgm_music), frame_rate=orig_audio.frame_rate)

        log.info("🎛️ 混合音频叠加...")
        mixed = orig_audio.overlay(bgm_music)

        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        mixed.export(output_path, format="mp3", bitrate="192k")
        log.info("✅ 混音已保存: %s", os.path.basename(output_path))
        return True
    except Exception as e:
        log.error("音频混入失败: %s", e)
        return False


import os
import time
import json
import requests
from PIL import Image
from io import BytesIO


def normalize_modelscope_token_pool(token_value, preserve_list_reference=False):
    if isinstance(token_value, list) and preserve_list_reference:
        raw_items = token_value
    elif isinstance(token_value, (list, tuple, set)):
        raw_items = list(token_value)
    else:
        raw_items = str(token_value or "").split(",")

    normalized = []
    seen = set()
    for item in raw_items:
        text = str(item or "").strip()
        if not text or text in seen:
            continue
        normalized.append(text)
        seen.add(text)

    if isinstance(token_value, list) and preserve_list_reference:
        token_value[:] = normalized
        return token_value
    return normalized


def build_modelscope_token_pool(token_value, shuffle_once=False):
    normalized_tokens = normalize_modelscope_token_pool(token_value)
    if shuffle_once and len(normalized_tokens) > 1:
        random.shuffle(normalized_tokens)
    return normalized_tokens


def clone_modelscope_token_pool(token_value, shuffle_once=False):
    cloned_tokens = normalize_modelscope_token_pool(token_value)
    if shuffle_once and len(cloned_tokens) > 1:
        random.shuffle(cloned_tokens)
    return cloned_tokens


def build_modelscope_token_pool_bundle(token_value, shuffle_once=False):
    base_tokens = build_modelscope_token_pool(token_value, shuffle_once=shuffle_once)
    return {
        "text": list(base_tokens),
        "image": list(base_tokens),
    }


def _get_modelscope_active_tokens(token_pool):
    if isinstance(token_pool, list):
        return normalize_modelscope_token_pool(token_pool, preserve_list_reference=True)
    return normalize_modelscope_token_pool(token_pool)


def _get_modelscope_usage_token_pool(token_source, usage):
    if isinstance(token_source, dict):
        token_pool = token_source.get(usage)
        if isinstance(token_pool, list):
            return normalize_modelscope_token_pool(token_pool, preserve_list_reference=True)
        return normalize_modelscope_token_pool(token_pool)
    return _get_modelscope_active_tokens(token_source)


def _remove_modelscope_token_from_pool(token_pool, token_text):
    if not isinstance(token_pool, list):
        return False
    normalized_pool = normalize_modelscope_token_pool(token_pool, preserve_list_reference=True)
    token_value = str(token_text or "").strip()
    if not token_value:
        return False
    removed = False
    while token_value in normalized_pool:
        normalized_pool.remove(token_value)
        removed = True
    return removed


def is_modelscope_daily_quota_exceeded_error(error):
    text = str(error or "")
    lowered = text.lower()
    return (
        "you have exceeded today's quota" in lowered
        or ("try again tomorrow" in lowered and "quota" in lowered)
        or ("error code: 429" in lowered and "quota" in lowered)
    )


def is_modelscope_http_429_error(error):
    text = str(error or "")
    lowered = text.lower()
    return (
        is_modelscope_daily_quota_exceeded_error(error)
        or "429 client error" in lowered
        or "too many requests" in lowered
        or "status code 429" in lowered
        or "error code: 429" in lowered
        or "'code': 429" in lowered
        or '"code":429' in lowered
        or '"code": 429' in lowered
    )


class CoverGenerationPolicyRejectedError(RuntimeError):
    """Raised when the provider rejects image generation input and we should fallback."""


class MissingYouTubeCredentialsError(RuntimeError):
    """Raised when the configured YouTube channel has no usable stored credentials."""


def _extract_http_error_details(error):
    response = getattr(error, "response", None)
    request = getattr(error, "request", None)
    status_code = getattr(response, "status_code", None)
    request_url = str(getattr(request, "url", "") or getattr(response, "url", "") or "")
    response_text = ""
    if response is not None:
        try:
            response_text = str(response.text or "")
        except Exception:
            response_text = ""
    return status_code, request_url, response_text


def is_modelscope_http_401_error(error):
    status_code, request_url, response_text = _extract_http_error_details(error)
    merged_text = "\n".join(part for part in [str(error or ""), response_text, request_url] if part).lower()
    return (
        status_code == 401
        or "401 client error" in merged_text
        or "status code 401" in merged_text
        or "error code: 401" in merged_text
        or "'code': 401" in merged_text
        or '"code":401' in merged_text
        or '"code": 401' in merged_text
        or "unauthorized" in merged_text
    )


def _log_modelscope_token_401(task_label, current_token, error, token_index=None, total_tokens=None, model_name=None):
    status_code, request_url, response_text = _extract_http_error_details(error)
    token_position = ""
    if token_index is not None and total_tokens is not None:
        token_position = f"，token={token_index}/{total_tokens}"
    model_text = f"，model={model_name}" if model_name else ""
    log.error(
        "❌ %s 命中 401，当前 token 疑似无效%s%s。token=%s | request_url=%s | response=%s | 原始错误：%s",
        task_label,
        model_text,
        token_position,
        current_token,
        request_url or "无",
        response_text or f"status_code={status_code}",
        error,
    )


def is_modelscope_image_review_rejection_error(error):
    status_code, request_url, response_text = _extract_http_error_details(error)
    merged_text = "\n".join(part for part in [str(error or ""), response_text] if part).lower()
    request_url = request_url.lower()
    review_keywords = (
        "敏感",
        "审核",
        "review",
        "sensitive",
        "moderation",
        "unsafe",
        "violation",
        "违规",
    )
    if any(keyword in merged_text for keyword in review_keywords):
        return "images/generations" in (merged_text + "\n" + request_url)
    return status_code == 400 and "api-inference.modelscope.cn/v1/images/generations" in request_url


def _is_nonempty_local_file(path):
    return bool(path and os.path.exists(path) and os.path.getsize(path) > 0)


def _persist_cover_fallback_image(source_path, target_path):
    if not _is_nonempty_local_file(source_path):
        return ""

    if os.path.abspath(source_path) == os.path.abspath(target_path):
        return target_path

    try:
        os.makedirs(os.path.dirname(target_path), exist_ok=True)
        with Image.open(source_path) as img:
            img.convert("RGB").save(target_path, format="JPEG", quality=95)
        if _is_nonempty_local_file(target_path):
            return target_path
    except Exception as e:
        log.warning("原始封面转存为标准 JPEG 失败，将继续直接使用原文件：%s", e)

    return source_path


def _read_positive_int_runtime_config(name, default_value):
    try:
        value = int(globals().get(name, default_value) or default_value)
    except Exception:
        value = default_value
    return max(1, value)


def _get_modelscope_image_request_timeout():
    return (
        _read_positive_int_runtime_config("MODELSCOPE_IMAGE_CONNECT_TIMEOUT", 300),
        _read_positive_int_runtime_config("MODELSCOPE_IMAGE_READ_TIMEOUT", 300),
    )


def _get_modelscope_image_poll_timeout():
    return (
        _read_positive_int_runtime_config("MODELSCOPE_IMAGE_POLL_CONNECT_TIMEOUT", 300),
        _read_positive_int_runtime_config("MODELSCOPE_IMAGE_POLL_READ_TIMEOUT", 300),
    )


def _sleep_before_next_modelscope_token():
    delay_seconds = _read_positive_int_runtime_config("MODELSCOPE_TOKEN_SWITCH_DELAY_SECONDS", 30)
    log.info("⏳ 不同 token 之间等待 %d 秒，随后继续切换下一个 token...", delay_seconds)
    time.sleep(delay_seconds)


def _get_modelscope_text_model_sequence():
    return [
        "Qwen/Qwen3.5-397B-A17B",
        "deepseek-ai/DeepSeek-V4-Pro",
    ]


def _create_modelscope_openai_client(current_token):
    from openai import OpenAI

    return OpenAI(
        base_url="https://api-inference.modelscope.cn/v1",
        api_key=current_token,
    )


def _extract_modelscope_chat_content(response):
    choices = getattr(response, "choices", None) or []
    first_choice = choices[0] if choices else None
    if not first_choice:
        return ""

    message = getattr(first_choice, "message", None)
    content = getattr(message, "content", None)
    if isinstance(content, list):
        merged_parts = []
        for item in content:
            if isinstance(item, dict):
                merged_parts.append(str(item.get("text") or item.get("content") or ""))
            else:
                merged_parts.append(str(getattr(item, "text", "") or getattr(item, "content", "") or ""))
        content = "".join(merged_parts)
    return str(content or "").strip()


def _strip_markdown_code_fences(text):
    cleaned_text = str(text or "").strip()
    if cleaned_text.startswith("```json"):
        cleaned_text = cleaned_text[7:]
    if cleaned_text.startswith("```"):
        cleaned_text = cleaned_text[3:]
    if cleaned_text.endswith("```"):
        cleaned_text = cleaned_text[:-3]
    return cleaned_text.strip()


def _run_qwen_task_with_token_rotation(
    task_label,
    token_pool,
    attempt,
    runner,
    max_quota_rounds=2,
    model_name="Qwen/Qwen3.5-397B-A17B",
    invalid_token_pool=None,
):
    active_tokens = _get_modelscope_active_tokens(token_pool)
    if not active_tokens:
        raise ValueError(f"{task_label} 未提供可用的 ModelScope Token。")

    collected_errors = []
    last_quota_error = None

    for quota_round in range(1, max_quota_rounds + 1):
        active_tokens = _get_modelscope_active_tokens(token_pool)
        if not active_tokens:
            break
        quota_hit_this_round = False
        round_tokens = list(active_tokens)
        total_tokens = len(round_tokens)

        for token_index, current_token in enumerate(round_tokens, start=1):
            try:
                return runner(current_token), collected_errors
            except Exception as e:
                if is_modelscope_http_401_error(e):
                    _remove_modelscope_token_from_pool(token_pool, current_token)
                    _remove_modelscope_token_from_pool(invalid_token_pool, current_token)
                    has_next_token = token_index < total_tokens
                    collected_errors.append(f"401 token={current_token} error={e}")
                    _log_modelscope_token_401(
                        task_label=task_label,
                        current_token=current_token,
                        error=e,
                        token_index=token_index,
                        total_tokens=total_tokens,
                        model_name=model_name,
                    )
                    if has_next_token:
                        _sleep_before_next_modelscope_token()
                    continue

                if is_modelscope_http_429_error(e):
                    _remove_modelscope_token_from_pool(token_pool, current_token)
                    has_next_token = token_index < total_tokens
                    quota_hit_this_round = True
                    last_quota_error = e
                    log.warning(
                        "⚠️ %s 第 %d 次失败：当前 token 触发 %s 配额限制，切换下一个 token。轮次=%d/%d，token=%d/%d | 原始错误：%s",
                        task_label,
                        attempt,
                        model_name,
                        quota_round,
                        max_quota_rounds,
                        token_index,
                        total_tokens,
                        e,
                    )
                    if has_next_token:
                        _sleep_before_next_modelscope_token()
                    continue

                collected_errors.append(str(e))
                has_next_token = token_index < len(round_tokens)
                log.warning(
                    "⚠️ %s 第 %d 次失败：%s；准备切换下一个 token。",
                    task_label,
                    attempt,
                    e,
                )
                if has_next_token:
                    _sleep_before_next_modelscope_token()

        if not quota_hit_this_round:
            return None, collected_errors
        if not _get_modelscope_active_tokens(token_pool):
            break
        if quota_round < max_quota_rounds:
            _sleep_before_next_modelscope_token()

    raise RuntimeError(
        f"{task_label} 在连续 {max_quota_rounds} 轮切换全部 token 后，仍然触发 "
        f"{model_name} 配额限制，停止运行。最后错误：{last_quota_error}"
    ) from last_quota_error


def _run_text_task_with_model_fallback(task_label, token_pool, attempt, runner, model_sequence=None):
    base_tokens = _get_modelscope_active_tokens(token_pool)
    if not base_tokens:
        raise ValueError(f"{task_label} 未提供可用的 ModelScope Token。")

    resolved_model_sequence = list(model_sequence or _get_modelscope_text_model_sequence())
    collected_errors = []

    for model_index, model_name in enumerate(resolved_model_sequence, start=1):
        model_token_pool = clone_modelscope_token_pool(base_tokens)
        try:
            result, model_errors = _run_qwen_task_with_token_rotation(
                task_label=task_label,
                token_pool=model_token_pool,
                attempt=attempt,
                runner=lambda current_token, current_model=model_name: runner(current_token, current_model),
                model_name=model_name,
                invalid_token_pool=token_pool,
            )
        except RuntimeError as e:
            collected_errors.append(f"{model_name}: {e}")
            if model_index < len(resolved_model_sequence):
                next_model_name = resolved_model_sequence[model_index]
                log.warning(
                    "⚠️ %s 在当前全部可用 token 上触发 %s 配额限制，开始自动切换到 %s 再完整重试一轮。",
                    task_label,
                    model_name,
                    next_model_name,
                )
                continue
            raise

        if model_errors:
            collected_errors.extend([f"{model_name}: {msg}" for msg in model_errors])
        if result is not None:
            return result, collected_errors

        if model_index < len(resolved_model_sequence):
            next_model_name = resolved_model_sequence[model_index]
            log.warning(
                "⚠️ %s 在当前全部可用 token 上都生成失败，开始自动切换到 %s 再完整重试一轮。",
                task_label,
                next_model_name,
            )

    return None, collected_errors


def _build_youtube_cover_draw_prompt(book_name, book_desc, current_token, attempt, text_model):
    client = _create_modelscope_openai_client(current_token)

    system_prompt = """角色设定：你是一位顶级 YouTube 封面设计师和 AI 绘图提示词专家。你的任务是根据我提供的书名和简介，输出一段可直接用于高质量文生图模型的英文提示词。

设计原则：
1. 主体必须直接体现书的内容和情绪，适合 YouTube thumbnail 的高点击构图。
2. 书名对应的中文大字必须作为画面的核心视觉元素，要求醒目、可读、对比强烈。
3. 允许补充一个极短的中文副标题增强点击欲。
4. 输出必须强调高对比、高饱和、戏剧光影、电影感和 16:9 横版构图。

最后约束：
1. 只输出一段英文 prompt，不要输出解释、分析、列表或前缀。
2. 必须包含 --ar 16:9。
3. 画面风格要偏 YouTube thumbnail，而不是普通海报。"""

    user_prompt = f"书名：[{book_name}]\n简介：[{book_desc}]"

    response = client.chat.completions.create(
        model=text_model,
        messages=[
            {'role': 'system', 'content': system_prompt},
            {'role': 'user', 'content': user_prompt}
        ]
    )

    draw_prompt = _extract_modelscope_chat_content(response)
    if not draw_prompt:
        raise ValueError("封面提示词接口未返回有效文本内容。")

    log.info("🎨 第 %d 次绘画请求 | 文字模型=%s\n%s", attempt, text_model, draw_prompt)
    return draw_prompt


def _request_modelscope_cover_image_url(image_model, current_token, draw_prompt, img_size):
    base_url = 'https://api-inference.modelscope.cn/'
    common_headers = {
        "Authorization": f"Bearer {current_token}",
        "Content-Type": "application/json",
    }
    request_timeout = _get_modelscope_image_request_timeout()
    poll_timeout = _get_modelscope_image_poll_timeout()

    log.info("🌅 正在将渲染任务下派给云端高能图层服务器 (X-ModelScope-Async-Mode)... 模型=%s", image_model)
    req_res = requests.post(
        f"{base_url}v1/images/generations",
        headers={**common_headers, "X-ModelScope-Async-Mode": "true"},
        data=json.dumps({
            "model": image_model,
            "size": img_size,
            "prompt": draw_prompt
        }, ensure_ascii=False).encode('utf-8'),
        timeout=request_timeout,
    )
    req_res.raise_for_status()
    task_id = req_res.json().get("task_id")
    if not task_id:
        raise ValueError("云端未返回 task_id。")

    log.info("📡 接收到远端任务队列牌号: %s，系统正原地静默巡检直到图块完工...", task_id)

    polls = 0
    poll_interval = 5
    max_polls = 50
    while polls < max_polls:
        polls += 1
        poll_res = requests.get(
            f"{base_url}v1/tasks/{task_id}",
            headers={**common_headers, "X-ModelScope-Task-Type": "image_generation"},
            timeout=poll_timeout,
        )
        poll_res.raise_for_status()
        data = poll_res.json()

        status = data.get("task_status")
        if status == "SUCCEED":
            output_images = data.get("output_images") or []
            if not output_images:
                raise ValueError(f"{image_model} 已成功完成，但返回结果中缺少 output_images。")
            img_url = output_images[0]
            log.info("🖼️ 远端结算完毕，获取到高速下载热链: %s", img_url)
            return img_url
        if status == "FAILED":
            raise ValueError(f"{image_model} 远端画图任务返回 FAILED。")

        time.sleep(poll_interval)

    raise ValueError(f"由于排队压力，远端在 {max_polls * poll_interval} 秒内仍未完成绘图。")


def _try_generate_cover_with_image_model(output_path, draw_prompt, img_size, image_model, token_candidates, invalid_token_pool=None):
    active_tokens = _get_modelscope_active_tokens(token_candidates)
    if not active_tokens:
        return {
            "success": False,
            "errors": ["当前已没有可用的 ModelScope Token。"],
            "failure_count": 0,
            "all_failures_are_429": True,
        }
    failure_messages = []
    failure_count = 0
    http_429_count = 0

    round_tokens = list(active_tokens)
    for token_index, current_token in enumerate(round_tokens, start=1):
        try:
            img_url = _request_modelscope_cover_image_url(image_model, current_token, draw_prompt, img_size)
            if download_file(img_url, output_path):
                log.info(
                    "🎉 %s 已成功生成 YouTube %s 超清海报图并刻录在案: %s",
                    image_model,
                    img_size,
                    os.path.basename(output_path),
                )
                return {
                    "success": True,
                    "errors": [],
                    "failure_count": 0,
                    "all_failures_are_429": False,
                }

            raise ValueError("URL 下载到本地图盘时文件被截断了。")
        except Exception as e:
            if is_modelscope_image_review_rejection_error(e):
                raise CoverGenerationPolicyRejectedError(
                    f"{image_model} 生图请求疑似触发提供商审核拒绝，不再继续重试：{e}"
                ) from e

            failure_count += 1
            failure_messages.append(str(e))
            if is_modelscope_http_401_error(e):
                _remove_modelscope_token_from_pool(token_candidates, current_token)
                _remove_modelscope_token_from_pool(invalid_token_pool, current_token)
                has_next_token = token_index < len(round_tokens)
                _log_modelscope_token_401(
                    task_label=f"{image_model} 生图",
                    current_token=current_token,
                    error=e,
                    token_index=token_index,
                    total_tokens=len(round_tokens),
                    model_name=image_model,
                )
                if has_next_token:
                    _sleep_before_next_modelscope_token()
                continue

            if is_modelscope_http_429_error(e):
                _remove_modelscope_token_from_pool(token_candidates, current_token)
                active_tokens = _get_modelscope_active_tokens(token_candidates)
                has_next_token = bool(active_tokens)
                http_429_count += 1
                log.warning(
                    "⚠️ %s 第 %d 个 token 生图失败：命中 429/限流，准备切换下一个 token。原始错误：%s",
                    image_model,
                    token_index,
                    e,
                )
                if has_next_token:
                    _sleep_before_next_modelscope_token()
                continue

            has_next_token = token_index < len(round_tokens)
            log.warning(
                "⚠️ %s 第 %d 个 token 生图失败：%s；准备切换下一个 token。",
                image_model,
                token_index,
                e,
            )
            if has_next_token:
                _sleep_before_next_modelscope_token()

    return {
        "success": False,
        "errors": failure_messages,
        "failure_count": failure_count,
        "all_failures_are_429": failure_count > 0 and http_429_count == failure_count,
    }


# =====================================================================
# API 优先级调度 - 根据 API_PRIORITY_ORDER 配置按优先级调用不同的 API 服务
# 可用值：modelscope（ModelScope 数据库 Token）、sensenova（Sensenova / Podcast AI）
# 优先级用逗号分隔，越靠前的优先级越高
# =====================================================================


def _parse_api_priority_order():
    """解析 API_PRIORITY_ORDER 配置项，返回按优先级排列的 API 名称列表"""
    raw = str(globals().get("API_PRIORITY_ORDER", "modelscope,sensenova") or "modelscope,sensenova").strip()
    api_list = [part.strip().lower() for part in raw.split(",") if part.strip()]
    # 去重但保留顺序
    seen = set()
    result = []
    for api in api_list:
        if api not in seen and api in frozenset({"modelscope", "sensenova"}):
            seen.add(api)
            result.append(api)
    if not result:
        result = ["modelscope", "sensenova"]
    return result


def _dispatch_cover_text(book_name, book_desc, text_token_pool, prompt_generation_attempt):
    """按 API_PRIORITY_ORDER 优先级依次尝试生成封面绘图提示词。

    返回 (draw_prompt, errors) 元组。draw_prompt 为空表示全部失败。
    """
    priority_list = _parse_api_priority_order()
    all_errors = []

    for api_name in priority_list:
        if api_name == "modelscope":
            draw_prompt, model_errors = _run_text_task_with_model_fallback(
                task_label="封面提示词生成",
                token_pool=text_token_pool,
                attempt=prompt_generation_attempt,
                runner=lambda current_token, current_model: _build_youtube_cover_draw_prompt(
                    book_name,
                    book_desc,
                    current_token,
                    prompt_generation_attempt,
                    current_model,
                ),
                model_sequence=_get_modelscope_text_model_sequence(),
            )
            if model_errors:
                all_errors.extend([f"modelscope: {msg}" for msg in model_errors])
            if draw_prompt:
                log.info("✅ [API 优先级] ModelScope 文本生成封面绘图提示词成功。")
                return draw_prompt, all_errors
            log.warning("⚠️ [API 优先级] ModelScope 文本生成失败，检查下一优先级 %s ...", priority_list[priority_list.index(api_name) + 1] if priority_list.index(api_name) + 1 < len(priority_list) else "无")

        elif api_name == "sensenova":
            log.info("🔄 [API 优先级] 切换到 Sensenova (Podcast AI) 生成封面绘图提示词...")
            sensenova_prompt = _call_sensenova_for_draw_prompt(book_name, book_desc)
            if sensenova_prompt:
                log.info("✅ [API 优先级] Sensenova 文本生成封面绘图提示词成功。")
                return sensenova_prompt, all_errors
            error_msg = "sensenova: Sensenova 文本生成全部重试失败"
            all_errors.append(error_msg)
            log.warning("⚠️ [API 优先级] Sensenova 文本生成失败。")

    return "", all_errors


def _dispatch_cover_image(output_path, draw_prompt, resolution, image_token_pool):
    """按 API_PRIORITY_ORDER 优先级依次尝试生成封面图片。

    返回 True 表示生成成功，False 表示全部失败。
    使用 errors 列表记录所有错误信息。
    """
    priority_list = _parse_api_priority_order()
    all_image_failures_are_429 = True
    total_image_failures = 0
    all_errors = []

    for api_name in priority_list:
        if api_name == "modelscope":
            res_to_size = {"720p": "1280x720", "1080p": "1920x1080", "1440p": "2560x1440", "4k": "3840x2160"}
            img_size = res_to_size.get(str(resolution).lower(), "1920x1080")
            image_model_sequence = [
                ("qwen/Qwen-Image-2512", "主生图模型"),
                ("Tongyi-MAI/Z-Image-Turbo", "回退生图模型"),
            ]
            any_model_success = False
            for model_index, (image_model, model_label) in enumerate(image_model_sequence, start=1):
                model_result = _try_generate_cover_with_image_model(
                    output_path=output_path,
                    draw_prompt=draw_prompt,
                    img_size=img_size,
                    image_model=image_model,
                    token_candidates=clone_modelscope_token_pool(image_token_pool),
                    invalid_token_pool=image_token_pool,
                )
                if model_result["success"]:
                    return True, []

                if model_result["errors"]:
                    all_errors.extend([f"modelscope/{image_model}: {msg}" for msg in model_result["errors"]])
                total_image_failures += int(model_result["failure_count"] or 0)
                if not model_result["all_failures_are_429"]:
                    all_image_failures_are_429 = False

                if model_index < len(image_model_sequence):
                    log.warning(
                        "⚠️ [API 优先级] %s 在全部 token 上失败，切换到 %s",
                        image_model,
                        image_model_sequence[model_index][0],
                    )

            if any_model_success:
                return True, []

            if total_image_failures > 0 and all_image_failures_are_429:
                log.warning(
                    "⚠️ [API 优先级] ModelScope 图片所有 token 均触发 429，检查下一优先级 %s ...",
                    priority_list[priority_list.index(api_name) + 1] if priority_list.index(api_name) + 1 < len(priority_list) else "无",
                )
            else:
                log.warning(
                    "⚠️ [API 优先级] ModelScope 图片生成失败（非全 429），检查下一优先级 %s ...",
                    priority_list[priority_list.index(api_name) + 1] if priority_list.index(api_name) + 1 < len(priority_list) else "无",
                )

        elif api_name == "sensenova":
            log.info("🔄 [API 优先级] 切换到 Sensenova (Podcast AI) 生成封面图片...")
            sensenova_ok = _sensenova_generate_cover_fallback(
                output_path=output_path,
                draw_prompt=draw_prompt,
                resolution=resolution,
            )
            if sensenova_ok:
                log.info("✅ [API 优先级] Sensenova 封面图片生成成功。")
                return True, []
            error_msg = "sensenova: Sensenova 图片生成全部重试失败"
            all_errors.append(error_msg)
            log.warning("⚠️ [API 优先级] Sensenova 图片生成失败。")

    return False, all_errors


# =====================================================================
# 2K 分辨率常量：11 种宽高比的 [width, height] 映射表
# ModelScope 429 限流时回退到 Sensenova (Podcast AI) 生图使用此尺寸
# =====================================================================
_2K_IMAGE_SIZES = {
    "2:3": (1664, 2496),
    "3:2": (2496, 1664),
    "3:4": (1760, 2368),
    "4:3": (2368, 1760),
    "4:5": (1824, 2272),
    "5:4": (2272, 1824),
    "1:1": (2048, 2048),
    "16:9": (2752, 1536),
    "9:16": (1536, 2752),
    "21:9": (3072, 1376),
    "9:21": (1344, 3136),
}


def _map_resolution_to_2k_size(resolution="1080p"):
    """将标准分辨率映射到最接近的 2K 尺寸（宽x高）"""
    res_to_ratio = {"720p": "4:3", "1080p": "16:9", "1440p": "16:9", "4k": "16:9"}
    ratio = res_to_ratio.get(str(resolution).lower(), "16:9")
    return _2K_IMAGE_SIZES.get(ratio, (2752, 1536))


def _sensenova_generate_cover_fallback(output_path, draw_prompt, resolution="1080p"):
    """当 ModelScope 所有 token 生图触发 429 限流时，使用 Sensenova (Podcast AI) 作为最终回退方案。

    使用 2K 分辨率尺寸（2752x1536 等）以确保封面图质量，
    下载后自动压缩为 1920x1080 JPEG（quality 85）避免 FFmpeg 封装失败。
    """
    from openai import OpenAI

    width, height = _map_resolution_to_2k_size(resolution)
    size_str = f"{width}x{height}"

    client = OpenAI(
        base_url=str(globals().get("SENSENOVA_BASE_URL", "https://token.sensenova.cn/v1") or "").strip(),
        api_key=str(globals().get("SENSENOVA_API_KEY", "") or "").strip(),
    )
    model_name = str(
        globals().get("YOUTUBE_PODCAST_IMAGE_MODEL_PRIMARY", "sensenova-u1-fast") or "sensenova-u1-fast"
    ).strip()
    retries = max(1, int(globals().get("YOUTUBE_PODCAST_IMAGE_MODEL_RETRIES", 3) or 3))

    # FFmpeg 兼容的目标尺寸（1920x1080 JPEG quality 85，约 200-500KB）
    target_size_map = {"720p": (1280, 720), "1080p": (1920, 1080), "1440p": (2560, 1440), "4k": (3840, 2160)}
    target_res = target_size_map.get(str(resolution).lower(), (1920, 1080))

    attempts_log = []
    for attempt_index in range(retries):
        try:
            log.info(
                "🔄 [Sensenova Fallback] 使用 %s 生成 2K 封面图 (%s)...",
                model_name,
                size_str,
            )
            response = client.images.generate(
                model=model_name,
                prompt=draw_prompt,
                size=size_str,
                n=1,
            )
            image_url = str(response.data[0].url or "").strip()
            if not image_url:
                raise ValueError("Sensenova 图片接口未返回可下载的 URL。")

            # 先下载到临时文件
            tmp_path = output_path + ".sensenova_tmp"
            if not download_file(image_url, tmp_path):
                raise ValueError("Sensenova 图片下载失败。")

            # 用 PIL 压缩为标准 1920x1080 JPEG，避免大图导致 FFmpeg OOM
            try:
                from PIL import Image as PILImage

                with PILImage.open(tmp_path) as img:
                    # 居中裁剪到目标比例，再缩放到目标尺寸
                    tw, th = target_res
                    src_w, src_h = img.size
                    src_ratio = src_w / src_h
                    target_ratio = tw / th

                    if src_ratio > target_ratio:
                        # 原图太宽，裁剪左右
                        new_w = int(src_h * target_ratio)
                        offset = (src_w - new_w) // 2
                        crop = img.crop((offset, 0, offset + new_w, src_h))
                    else:
                        # 原图太高，裁剪上下
                        new_h = int(src_w / target_ratio)
                        offset = (src_h - new_h) // 2
                        crop = img.crop((0, offset, src_w, offset + new_h))

                    resized = crop.resize(target_res, PILImage.LANCZOS)
                    resized.convert("RGB").save(output_path, format="JPEG", quality=85)
                    log.info(
                        "✅ Sensenova 原始图片已压缩为 %dx%d JPEG（quality=85）",
                        tw, th,
                    )
            except Exception as pil_err:
                log.warning("⚠️ Sensenova 图片 PIL 压缩失败，回退使用原始文件: %s", pil_err)
                if os.path.exists(tmp_path):
                    os.replace(tmp_path, output_path)
            finally:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)

            if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
                log.info(
                    "🎉 [Sensenova Fallback] 封面图生成成功：%s (原始尺寸: %s, 约 %.1f MB)",
                    os.path.basename(output_path),
                    size_str,
                    os.path.getsize(output_path) / 1024 / 1024,
                )
                return True

            raise ValueError("Sensenova 生成的文件为空。")
        except Exception as e:
            err_text = _podcast_error_text(e)
            attempts_log.append(f"attempt {attempt_index + 1}: {err_text}")
            log.warning(
                "⚠️ [Sensenova Fallback] 第 %d/%d 次失败：%s",
                attempt_index + 1,
                retries,
                err_text,
            )
            if attempt_index < retries - 1:
                sleep_sec = _podcast_ai_retry_sleep_seconds(attempt_index)
                time.sleep(sleep_sec)

    log.error(
        "❌ [Sensenova Fallback] 全部 %d 次重试均失败。错误：%s",
        retries,
        " ; ".join(attempts_log),
    )
    return False


def _call_sensenova_for_draw_prompt(book_name, book_desc):
    """当 ModelScope 所有文本 token 触发 429 限流时，使用 Sensenova (Podcast AI) 生成封面绘图提示词。

    返回 draw_prompt 字符串，失败时返回空字符串。
    """
    from openai import OpenAI

    client = OpenAI(
        base_url=str(globals().get("SENSENOVA_BASE_URL", "https://token.sensenova.cn/v1") or "").strip(),
        api_key=str(globals().get("SENSENOVA_API_KEY", "") or "").strip(),
    )
    model_name = str(
        globals().get("YOUTUBE_PODCAST_TEXT_MODEL_PRIMARY", "qwen-plus") or "qwen-plus"
    ).strip()
    retries = max(1, int(globals().get("YOUTUBE_PODCAST_TEXT_MODEL_RETRIES", 3) or 3))

    system_prompt = """角色设定：你是一位顶级 YouTube 封面设计师和 AI 绘图提示词专家。你的任务是根据我提供的书名和简介，输出一段可直接用于高质量文生图模型的英文提示词。

设计原则：
1. 主体必须直接体现书的内容和情绪，适合 YouTube thumbnail 的高点击构图。
2. 书名对应的中文大字必须作为画面的核心视觉元素，要求醒目、可读、对比强烈。
3. 允许补充一个极短的中文副标题增强点击欲。
4. 输出必须强调高对比、高饱和、戏剧光影、电影感和 16:9 横版构图。

最后约束：
1. 只输出纯英文提示词本身，必须去掉行首的 "Prompt:"、"prompt:" 等前缀。
2. 不要输出任何多余的汉字解释、不需要英文引导词、不需要 Markdown 块标记。
3. 提示词长度请控制在 60-120 个英文单词之间。"""

    user_prompt = f"书名：[{book_name}]\n简介：[{book_desc}]"

    for attempt_index in range(retries):
        try:
            log.info(
                "🔄 [Sensenova Fallback Text] 使用 %s 生成封面绘图提示词 (第 %d/%d 次)...",
                model_name,
                attempt_index + 1,
                retries,
            )
            response = client.chat.completions.create(
                model=model_name,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
            )
            result = _podcast_extract_chat_text(response)
            if result:
                log.info("✅ [Sensenova Fallback Text] 封面绘图提示词生成成功。")
                return result
            raise ValueError("Sensenova 返回的文本内容为空。")
        except Exception as e:
            err_text = _podcast_error_text(e)
            log.warning(
                "⚠️ [Sensenova Fallback Text] 第 %d/%d 次失败：%s",
                attempt_index + 1,
                retries,
                err_text,
            )
            if attempt_index < retries - 1:
                sleep_sec = _podcast_ai_retry_sleep_seconds(attempt_index)
                time.sleep(sleep_sec)

    log.error("❌ [Sensenova Fallback Text] 全部 %d 次重试均失败。", retries)
    return ""


def auto_create_youtube_cover(book_name, book_desc, output_path, token, resolution="1080p"):
    """使用 API_PRIORITY_ORDER 配置的优先级链生成 YouTube 封面图。

    支持按优先级顺序尝试 modelscope、sensenova 等 API 服务，
    高优先级服务不可用时自动降级到次优先级。
    """
    priority_list = _parse_api_priority_order()
    needs_modelscope_text = "modelscope" in priority_list
    needs_modelscope_image = "modelscope" in priority_list

    # 按需验证 Token 可用性，避免不必要的报错
    if needs_modelscope_text or needs_modelscope_image:
        text_token_pool = _get_modelscope_usage_token_pool(token, "text")
        image_token_pool = _get_modelscope_usage_token_pool(token, "image")

    res_to_size = {"720p": "1280x720", "1080p": "1920x1080", "1440p": "2560x1440", "4k": "3840x2160"}
    img_size = res_to_size.get(str(resolution).lower(), "1920x1080")

    log.info(
        "【🖼️ AI绘图】[%s] 分析有声书意境提取并生成高宽容度爆款 YouTube 封面 (%s)... API 优先级: %s",
        book_name, img_size, " → ".join(priority_list),
    )

    attempt = 0
    prompt_generation_attempt = 0
    cached_draw_prompt = ""

    while True:
        attempt += 1
        current_cycle_errors = []
        draw_prompt = cached_draw_prompt
        if not draw_prompt:
            prompt_generation_attempt += 1

            # 使用优先级调度获取封面绘图提示词
            draw_prompt, prompt_errors = _dispatch_cover_text(
                book_name=book_name,
                book_desc=book_desc,
                text_token_pool=text_token_pool if needs_modelscope_text else None,
                prompt_generation_attempt=prompt_generation_attempt,
            )
            if prompt_errors:
                current_cycle_errors.extend(prompt_errors)

            if draw_prompt:
                cached_draw_prompt = draw_prompt
            else:
                log.warning(
                    "⚠️ 封面生成模块第 %d 次失败：所有 API 优先级均未能生成有效提示词。错误摘要：%s；系统将持续重试，直到成功为止。",
                    attempt,
                    " | ".join(current_cycle_errors[-5:]) if current_cycle_errors else "无",
                )
                time.sleep(min(30, 5 + attempt))
                continue

        else:
            log.info("🧠 第 %d 次封面重试将复用上一次成功生成的生图提示词，不再重新生成提示词。", attempt)

        # 使用优先级调度生成封面图片
        image_ok, image_errors = _dispatch_cover_image(
            output_path=output_path,
            draw_prompt=draw_prompt,
            resolution=resolution,
            image_token_pool=image_token_pool if needs_modelscope_image else None,
        )
        if image_ok:
            return True

        if image_errors:
            current_cycle_errors.extend(image_errors)

        log.warning(
            "⚠️ 封面生成模块第 %d 次失败：所有 API 优先级均未能生成封面图片。错误摘要：%s；系统将持续复用当前提示词重试，直到成功为止。",
            attempt,
            " | ".join(current_cycle_errors[-6:]) if current_cycle_errors else "无",
        )
        time.sleep(min(30, 5 + attempt))


def auto_create_youtube_seo(book_name, book_desc, output_path, token):
    text_token_pool = _get_modelscope_usage_token_pool(token, "text")
    if not text_token_pool:
        raise ValueError("未提供 ModelScope 文字 Token，无法生成 SEO 文案。")

    log.info("【📝 AI文案大师】[%s] 分析书籍内容以撰写 YouTube SEO 最优化简介...", book_name)

    attempt = 0
    text_model_sequence = _get_modelscope_text_model_sequence()
    while True:
        attempt += 1
        def _generate_seo_dict(current_token, text_model):
            client = _create_modelscope_openai_client(current_token)

            system_prompt = """角色设定：
你现在是一位千万粉丝级别的 YouTube 运营专员与 SEO 大师。
你的任务是根据提供的【书名】和【内容简介】，为有声书视频精心打造一套高点击率（CTR）视频标题、引人入胜的描述、以及利于算法推荐的 #标签。

输出格式约束（必须严格遵守的铁律）：
你必须且只能返回一个合法的 JSON 格式对象字符串，绝对禁止输出任何多余的汉字解释、前言或者 Markdown 代码块标识！不要加上 ```json 这三个字！
JSON 必须严格有且只有以下三个 key：
{
  "title": "你设计的高吸引力长标题",
  "Description": "用Emoji点缀的带悬念和痛点的高转换率介绍词，长度大约 200 字。",
  "label": "#有声书 #个人成长 #认知刷新 等至少20个长短尾热门标签组"
}"""

            user_prompt = f"书名：[{book_name}]\n简介：[{book_desc}]"

            response = client.chat.completions.create(
                model=text_model,
                messages=[
                    {'role': 'system', 'content': system_prompt},
                    {'role': 'user', 'content': user_prompt}
                ]
            )

            llm_reply = _strip_markdown_code_fences(_extract_modelscope_chat_content(response))

            return json.loads(llm_reply)

        seo_dict, generation_errors = _run_text_task_with_model_fallback(
            task_label="SEO 文案生成",
            token_pool=text_token_pool,
            attempt=attempt,
            runner=_generate_seo_dict,
            model_sequence=text_model_sequence,
        )

        if seo_dict:
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(seo_dict, f, ensure_ascii=False, indent=2)

            log.info("🎉 YouTube SEO 结构化脑暴文案 (JSON) 已于第 %d 次生成并提取保存为: %s", attempt, os.path.basename(output_path))
            return True, seo_dict

        log.warning(
            "⚠️ SEO 文案生成第 %d 次失败：当前可用 token 全部未能生成可用结果。错误摘要：%s；系统将持续重试，直到成功为止。",
            attempt,
            " | ".join(generation_errors[-5:]) if generation_errors else "无",
        )
        time.sleep(min(30, 5 + attempt))


def generate_youtube_timestamps(chapters_data, chapter_audio_paths=None):
    """
    优先根据实际章节音频时长生成时间轴；若没有音频文件则回退到 chapters_data.long。
    """
    log.info("【⏳ 时间轴计算】正在组装 YouTube 视频分段指针...")
    timestamps = []
    current_time_seconds = 0

    sorted_chapters = sorted(chapters_data, key=lambda x: x.get("id", 0))
    use_audio_durations = bool(chapter_audio_paths) and len(chapter_audio_paths) == len(sorted_chapters)
    if chapter_audio_paths and not use_audio_durations:
        log.warning("时间轴音频数量与章节数量不一致，回退到 long 字段。")
    elif use_audio_durations:
        log.info("时间轴优先使用实际章节音频时长，避免 long 字段漂移。")

    for idx, ch in enumerate(sorted_chapters):
        h = current_time_seconds // 3600
        m = (current_time_seconds % 3600) // 60
        s = current_time_seconds % 60
        if h > 0:
            time_str = f"{h:02d}:{m:02d}:{s:02d}"
        else:
            time_str = f"{m:02d}:{s:02d}"

        title = ch.get("title", f"章节 {ch.get('id', '')}").strip()
        timestamps.append(f"{time_str} {title}")

        duration_sec = None
        if use_audio_durations:
            duration_sec = probe_audio_duration_seconds(chapter_audio_paths[idx])
        if duration_sec is None:
            duration_sec = parse_duration_to_seconds(ch.get("long", "00:00"))

        current_time_seconds += max(0, int(duration_sec or 0))

    final_text = "\n".join(timestamps)
    log.info("🎉 成功排盘 %d 章时间轴，成片总预估时长：%02d:%02d:%02d", len(sorted_chapters), current_time_seconds//3600, (current_time_seconds%3600)//60, current_time_seconds%60)
    return final_text


import subprocess
import os

def generate_video(audio_path, image_path, output_path, resolution="1080p"):
    if not os.path.exists(audio_path):
        log.error("无法生成视频：音频文件不存在 %s", audio_path)
        return False
    if not os.path.exists(image_path):
        log.error("无法生成视频：封面文件不存在 %s", image_path)
        return False

    log.info("开始通过 FFmpeg 封装 MP4 视频...")

    # =====================================================================
    # 统一封面预处理：所有来源的图片在传给 FFmpeg 前都压缩为标准尺寸 JPEG
    # 避免 2K/5MB 大图导致 Colab /dev/shm 不足、FFmpeg OOM 或解码失败
    # =====================================================================
    target_size_map = {"720p": (1280, 720), "1080p": (1920, 1080), "1440p": (2560, 1440), "4k": (3840, 2160)}
    target_res = target_size_map.get(str(resolution).lower(), (1920, 1080))
    tw, th = target_res

    processed_image = image_path
    needs_cleanup = False
    try:
        from PIL import Image as PILImage
        with PILImage.open(image_path) as img:
            src_w, src_h = img.size
            # 只有当图片明显大于目标尺寸或不是 JPEG 时才压缩
            if src_w > tw * 1.1 or src_h > th * 1.1 or img.format != "JPEG":
                # 居中裁剪到目标宽高比
                src_ratio = src_w / src_h
                target_ratio = tw / th
                if src_ratio > target_ratio:
                    new_w = int(src_h * target_ratio)
                    offset = (src_w - new_w) // 2
                    img = img.crop((offset, 0, offset + new_w, src_h))
                elif src_ratio < target_ratio:
                    new_h = int(src_w / target_ratio)
                    offset = (src_h - new_h) // 2
                    img = img.crop((0, offset, src_w, offset + new_h))
                # 缩放到目标分辨率
                img = img.resize(target_res, PILImage.LANCZOS)
                # 保存为临时压缩 JPEG
                processed_image = output_path + ".cover_cache.jpg"
                img.convert("RGB").save(processed_image, format="JPEG", quality=85)
                needs_cleanup = True
                log.info(
                    "封面预处理：%dx%d → %dx%d JPEG (quality=85)，原始约 %.1f MB",
                    src_w, src_h, tw, th,
                    os.path.getsize(image_path) / 1024 / 1024,
                )
    except Exception as e:
        log.warning("封面 PIL 预处理失败，使用原始文件：%s", e)
        processed_image = image_path

    res_to_scale = {"720p": "1280:720", "1080p": "1920:1080", "1440p": "2560:1440", "4k": "3840:2160"}
    scale_vf = res_to_scale.get(str(resolution).lower(), "1920:1080")

    base_cmd = [
        "ffmpeg", "-y",
        "-loop", "1",
        "-framerate", "1",
        "-i", processed_image,
        "-i", audio_path,
        "-vf", f"scale={scale_vf}:force_original_aspect_ratio=decrease,pad={scale_vf}:(ow-iw)/2:(oh-ih)/2,format=yuv420p",
        "-c:v", "libx264",
        "-preset", "ultrafast",
        "-tune", "stillimage",
        "-shortest",
    ]

    attempts = [
        ("copy-audio", ["-c:a", "copy"]),
        ("aac-fallback", ["-c:a", "aac", "-b:a", "192k", "-movflags", "+faststart"]),
    ]

    last_error = ""
    for idx, (mode, audio_args) in enumerate(attempts, start=1):
        cmd = base_cmd + audio_args + [output_path]
        try:
            result = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=3600,
            )
        except subprocess.TimeoutExpired:
            last_error = f"FFmpeg 在 {mode} 模式下执行超时"
            log.error(last_error)
            if os.path.exists(output_path):
                os.remove(output_path)
            continue
        except Exception as e:
            last_error = f"调用 FFmpeg 封装时发生异常: {e}"
            log.error(last_error)
            if os.path.exists(output_path):
                os.remove(output_path)
            continue

        if result.returncode == 0 and os.path.exists(output_path) and os.path.getsize(output_path) > 0:
            log.info(
                "视频封装完成: %s (模式=%s, 大小: %.2f MB)",
                os.path.basename(output_path),
                mode,
                os.path.getsize(output_path) / 1024 / 1024,
            )
            # 清理临时压缩封面缓存
            if needs_cleanup and os.path.exists(processed_image):
                os.remove(processed_image)
            return True

        last_error = (result.stderr or "").strip()[-1500:]
        if os.path.exists(output_path):
            os.remove(output_path)

        if idx < len(attempts):
            log.warning("视频封装在 %s 模式失败，切换到下一种兼容方案。", mode)
        else:
            log.error("视频封装失败，FFmpeg 报错:\n%s", last_error)

    # 清理临时压缩封面缓存
    if needs_cleanup and os.path.exists(processed_image):
        os.remove(processed_image)
    return False


from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request as GoogleAuthRequest
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError
from PIL import Image
from datetime import datetime as dt_datetime, timedelta as dt_timedelta, timezone as dt_timezone
from zoneinfo import ZoneInfo
import json
import time
import os

try:
    YOUTUBE_SCHEDULE_LOCAL_TIMEZONE = ZoneInfo("Asia/Shanghai")
except Exception:
    YOUTUBE_SCHEDULE_LOCAL_TIMEZONE = dt_timezone(dt_timedelta(hours=8))
YOUTUBE_DAILY_PUBLISH_LIMIT = max(1, int(globals().get("YOUTUBE_DAILY_PUBLISH_LIMIT", 3) or 3))
YOUTUBE_TITLE_MATCH_CACHE = {}
YOUTUBE_LOCALIZATION_CONVERTER_CACHE = {}
YOUTUBE_LOCALIZATION_INSTALL_ATTEMPTED = set()


def get_youtube_default_language():
    value = str(globals().get("YOUTUBE_DEFAULT_LANGUAGE", "zh-CN") or "zh-CN").strip()
    return value or "zh-CN"


def youtube_traditional_localization_enabled():
    return bool(globals().get("ENABLE_YOUTUBE_TRADITIONAL_LOCALIZATION", True))


def get_youtube_localization_locales():
    raw_value = str(globals().get("YOUTUBE_LOCALIZATION_LOCALES", "") or "").strip()
    if not raw_value:
        raw_value = get_youtube_traditional_locale()

    default_language = get_youtube_default_language()
    locales = []
    for chunk in raw_value.replace("\r", "\n").split("\n"):
        for part in chunk.split(","):
            locale = str(part or "").strip()
            if not locale or locale == default_language or locale in locales:
                continue
            locales.append(locale)
    return locales


def get_youtube_traditional_locale():
    value = str(globals().get("YOUTUBE_TRADITIONAL_LOCALE", "zh-TW") or "zh-TW").strip()
    return value or "zh-TW"


def get_youtube_traditional_opencc_config():
    value = str(globals().get("YOUTUBE_TRADITIONAL_OPENCC_CONFIG", "s2t") or "s2t").strip()
    return value or "s2t"


def youtube_traditional_opencc_auto_install_enabled():
    return bool(globals().get("ENABLE_AUTO_INSTALL_OPENCC", True))


def _get_youtube_localization_converter(config_name=""):
    config_name = str(config_name or get_youtube_traditional_opencc_config() or "").strip()
    if not config_name:
        return None
    if config_name in YOUTUBE_LOCALIZATION_CONVERTER_CACHE:
        cached = YOUTUBE_LOCALIZATION_CONVERTER_CACHE.get(config_name)
        return cached or None

    try:
        from opencc import OpenCC
    except ImportError:
        if not youtube_traditional_opencc_auto_install_enabled():
            log.warning(
                "OpenCC is unavailable and ENABLE_AUTO_INSTALL_OPENCC is disabled. zh-TW localization will be skipped."
            )
            YOUTUBE_LOCALIZATION_CONVERTER_CACHE[config_name] = False
            return None
        if config_name not in YOUTUBE_LOCALIZATION_INSTALL_ATTEMPTED:
            YOUTUBE_LOCALIZATION_INSTALL_ATTEMPTED.add(config_name)
            try:
                import sys as _sys

                log.warning(
                    "Missing opencc for zh-TW localization. Attempting automatic install: opencc-python-reimplemented"
                )
                install_result = subprocess.run(
                    [_sys.executable, "-m", "pip", "install", "-q", "opencc-python-reimplemented"],
                    check=False,
                    capture_output=True,
                    text=True,
                )
                if install_result.returncode == 0:
                    from opencc import OpenCC

                    log.info("Installed opencc-python-reimplemented automatically for zh-TW localization.")
                else:
                    detail = (install_result.stderr or install_result.stdout or "").strip()[-500:]
                    log.warning(
                        "Automatic OpenCC install failed. zh-TW localization will be skipped for this run: %s",
                        detail or "no pip output",
                    )
                    YOUTUBE_LOCALIZATION_CONVERTER_CACHE[config_name] = False
                    return None
            except Exception as install_error:
                log.warning(
                    "Unable to auto-install OpenCC. zh-TW localization will be skipped for this run: %s",
                    install_error,
                )
                YOUTUBE_LOCALIZATION_CONVERTER_CACHE[config_name] = False
                return None
        else:
            log.warning(
                "OpenCC is unavailable and auto-install already failed earlier in this run. zh-TW localization will be skipped."
            )
            YOUTUBE_LOCALIZATION_CONVERTER_CACHE[config_name] = False
            return None

    converter = _build_opencc_converter_with_fallback(config_name)
    YOUTUBE_LOCALIZATION_CONVERTER_CACHE[config_name] = converter
    return converter


def _get_youtube_locale_conversion_config(locale):
    normalized_locale = str(locale or "").strip()
    if not normalized_locale:
        return ""
    if normalized_locale == "zh-HK":
        return "s2hk"
    if normalized_locale in {"zh-TW", "zh-Hant"}:
        return get_youtube_traditional_opencc_config()
    return ""


def _build_opencc_converter_with_fallback(config_name):
    try:
        from opencc import OpenCC

        return OpenCC(config_name)
    except Exception as exc:
        fallback_config = get_youtube_traditional_opencc_config()
        if config_name != fallback_config:
            try:
                from opencc import OpenCC

                log.warning(
                    "OpenCC config %s is unavailable. Falling back to %s for YouTube localization: %s",
                    config_name,
                    fallback_config,
                    exc,
                )
                return OpenCC(fallback_config)
            except Exception:
                pass
        raise


def _build_youtube_localization_entry_for_locale(locale, normalized_title, normalized_description):
    conversion_config = _get_youtube_locale_conversion_config(locale)
    if not conversion_config:
        return {
            "title": normalized_title,
            "description": normalized_description,
        }

    converter = _get_youtube_localization_converter(conversion_config)
    if converter is None:
        return None
    return {
        "title": converter.convert(normalized_title),
        "description": converter.convert(normalized_description),
    }


def build_youtube_traditional_localizations(title="", description=""):
    default_language = get_youtube_default_language()
    if not youtube_traditional_localization_enabled():
        return default_language, {}

    normalized_title = str(title or "")[:100]
    normalized_description = str(description or "")[:5000]
    if not normalized_title and not normalized_description:
        return default_language, {}

    target_locales = get_youtube_localization_locales()
    if not target_locales:
        return default_language, {}

    generated = {}
    for target_locale in target_locales:
        entry = _build_youtube_localization_entry_for_locale(
            target_locale,
            normalized_title,
            normalized_description,
        )
        if entry is None:
            continue
        generated[target_locale] = entry
    return default_language, generated


def merge_youtube_localizations(existing_localizations=None, title="", description="", force_overwrite=False):
    merged = dict(existing_localizations or {})
    default_language, generated = build_youtube_traditional_localizations(title=title, description=description)
    if not generated:
        return default_language, merged, False

    changed = False
    for target_locale, localized_entry in generated.items():
        if merged.get(target_locale) and not force_overwrite:
            continue
        if merged.get(target_locale) != localized_entry:
            merged[target_locale] = localized_entry
            changed = True
    return default_language, merged, changed


def _build_youtube_mutable_video_snippet(snippet, default_language=""):
    body_snippet = {
        "title": str((snippet or {}).get("title") or "")[:100],
        "description": str((snippet or {}).get("description") or "")[:5000],
        "defaultLanguage": str((snippet or {}).get("defaultLanguage") or default_language or get_youtube_default_language()).strip(),
    }
    tags = (snippet or {}).get("tags")
    if tags:
        body_snippet["tags"] = list(tags)
    category_id = str((snippet or {}).get("categoryId") or "").strip()
    if category_id:
        body_snippet["categoryId"] = category_id
    return body_snippet

def authenticate_youtube_from_supabase(channel_name):
    """从数据库获取指定频道的 YouTube Token，并在需要时自动刷新。"""
    table_sql = get_public_table_identifier("youtube_credentials")
    log.info("🔐 正在连接数据库读取 YouTube '%s' 频道无人值守通行证...", channel_name)
    try:
        row = execute_postgres_fetchone(
            sql.SQL(
                """
                SELECT token_json
                FROM {}
                WHERE channel_name = %s
                LIMIT 1
                """
            ).format(table_sql),
            (channel_name,),
        )
        if not row:
            message = f"无法在数据库找到频道 {channel_name} 的授权凭证。请先在初始化单元中写入。"
            log.error("❌ %s", message)
            raise MissingYouTubeCredentialsError(message)

        token_info = row.get("token_json")
        if not token_info:
            message = f"频道 {channel_name} 的授权凭证数据为空。请先在初始化单元中写入有效凭证。"
            log.error("❌ %s", message)
            raise MissingYouTubeCredentialsError(message)

        token_dict = json.loads(token_info) if isinstance(token_info, str) else token_info
        credentials = Credentials.from_authorized_user_info(
            token_dict,
            scopes=["https://www.googleapis.com/auth/youtube"],
        )

        if credentials.expired:
            if credentials.refresh_token:
                log.info("🔄 YouTube 凭证已过期，尝试自动刷新令牌...")
                credentials.refresh(GoogleAuthRequest())
                refreshed_token = json.loads(credentials.to_json())
                try:
                    execute_postgres(
                        sql.SQL(
                            """
                            INSERT INTO {} (channel_name, token_json, updated_at)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (channel_name)
                            DO UPDATE SET
                              token_json = EXCLUDED.token_json,
                              updated_at = EXCLUDED.updated_at
                            """
                        ).format(table_sql),
                        (channel_name, Jsonb(refreshed_token), dt_module.datetime.now().isoformat()),
                    )
                    log.info("✅ 新令牌已自动回写数据库。")
                except Exception as refresh_save_error:
                    log.warning("⚠️ 令牌刷新成功，但回写数据库失败: %s", refresh_save_error)
            else:
                log.error("❌ YouTube 凭证已过期，且缺少 refresh_token，无法自动刷新。")
                return None

        youtube = build("youtube", "v3", credentials=credentials, cache_discovery=False)
        log.info("✅ YouTube '%s' 频道连线并授权成功。", channel_name)
        return youtube
    except Exception as e:
        log.error("❌ 初始化 YouTube 客户端失败，请检查数据库连接和表数据: %s", e)
        return None

def compress_thumbnail_to_safe_limit(img_path, max_bytes=2 * 1024 * 1024):
    """将海报压缩到 YouTube 更容易接受的体积范围。"""
    if not img_path or not os.path.exists(img_path):
        return img_path

    size = os.path.getsize(img_path)
    if size <= max_bytes:
        return img_path

    log.warning(
        "⚠️ 警报响应！远端画师渲染了一款重型大画幅神图试图破门！(原生体积: %.2f MB) 系统自动介入，将其瘦身减压以免在 YouTube 端被击落拒收...",
        size / (1024 * 1024),
    )

    dir_name = os.path.dirname(img_path)
    base_name = os.path.basename(img_path)
    safe_path = os.path.join(dir_name, "safe_2mb_" + base_name)
    plans = [
        {"size": None, "quality": 85, "label": "原始尺寸高质量压缩"},
        {"size": (1920, 1080), "quality": 80, "label": "收缩至 1920x1080"},
        {"size": (1280, 720), "quality": 75, "label": "退守至 1280x720"},
        {"size": (1280, 720), "quality": 65, "label": "进一步降低 JPEG 质量"},
    ]

    try:
        with Image.open(img_path) as source_img:
            base_img = source_img.convert("RGB")
            final_size = size
            for plan in plans:
                candidate = base_img.copy()
                if plan["size"]:
                    candidate.thumbnail(plan["size"], Image.Resampling.LANCZOS)
                candidate.save(safe_path, format="JPEG", quality=plan["quality"], optimize=True)
                final_size = os.path.getsize(safe_path)
                log.info("🧪 海报压缩方案：%s -> %.2f MB", plan["label"], final_size / (1024 * 1024))
                if final_size <= max_bytes:
                    break

        if final_size > max_bytes:
            log.warning("⚠️ 已完成多轮压缩，但海报仍略高于安全线，仍尝试使用压缩版上传。")
        else:
            log.info("🎉 魔鬼减压计划完成！出厂新核：%.2f MB。符合云端全准入安检！", final_size / (1024 * 1024))
        return safe_path
    except Exception as e:
        log.error("❌ 拦截削修中生病坠毁: %s。只能把原大毒饼图强塞云端进行赌运传输...", e)
        return img_path

def normalize_youtube_category_id(category_id):
    """支持留空或占位值，表示上传时不设置分类。"""
    if category_id is None:
        return ""

    normalized = str(category_id).strip()
    if normalized.lower() in {"", "none", "null"}:
        return ""
    return normalized

def normalize_youtube_tags(tags, max_total_chars=500, max_count=30):
    """兼容空格/逗号/# 标签格式，并控制 YouTube 可接受的总体长度。"""
    if not tags:
        return []

    raw_items = []
    for chunk in str(tags).replace("\n", " ").split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        if "#" in chunk and " " in chunk:
            raw_items.extend(part for part in chunk.split() if part.strip())
        else:
            raw_items.append(chunk)

    normalized = []
    seen = set()
    total_chars = 0
    for item in raw_items:
        cleaned = item.strip().strip("#").strip()
        if not cleaned:
            continue
        key = cleaned.lower()
        if key in seen:
            continue

        extra_chars = len(cleaned) + (1 if normalized else 0)
        if len(normalized) >= max_count or total_chars + extra_chars > max_total_chars:
            break

        normalized.append(cleaned)
        seen.add(key)
        total_chars += extra_chars

    return normalized


def _parse_youtube_datetime(value):
    text = str(value or "").strip()
    if not text:
        return None

    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = dt_datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=dt_timezone.utc)
        return parsed.astimezone(dt_timezone.utc)
    except Exception:
        return None


def _format_youtube_datetime_z(value):
    parsed = _parse_youtube_datetime(value) if not isinstance(value, dt_datetime) else value.astimezone(dt_timezone.utc)
    if not parsed:
        return ""
    return parsed.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _get_youtube_uploads_playlist_id_with_client(youtube):
    response = youtube.channels().list(part="contentDetails", mine=True, maxResults=1).execute()
    items = response.get("items", [])
    if not items:
        raise RuntimeError("无法读取当前 YouTube 频道信息，未找到 uploads playlist。")

    uploads_playlist_id = (
        ((items[0].get("contentDetails") or {}).get("relatedPlaylists") or {}).get("uploads") or ""
    ).strip()
    if not uploads_playlist_id:
        raise RuntimeError("当前 YouTube 频道未返回 uploads playlist ID。")
    return uploads_playlist_id


def _list_upload_video_ids_with_client(youtube, uploads_playlist_id, max_videos=100):
    video_ids = []
    page_token = None
    while True:
        response = youtube.playlistItems().list(
            part="contentDetails",
            playlistId=uploads_playlist_id,
            maxResults=50,
            pageToken=page_token,
        ).execute()
        for item in response.get("items", []):
            video_id = str(((item.get("contentDetails") or {}).get("videoId") or "")).strip()
            if video_id:
                video_ids.append(video_id)
                if len(video_ids) >= max(1, int(max_videos or 100)):
                    return video_ids[: max(1, int(max_videos or 100))]
        page_token = response.get("nextPageToken")
        if not page_token:
            break
    return video_ids


def _chunk_items(items, chunk_size):
    for idx in range(0, len(items), chunk_size):
        yield items[idx:idx + chunk_size]


def _normalize_youtube_title_key(title):
    text = " ".join(str(title or "").split()).strip()
    return text.casefold()


def _fetch_video_status_rows_with_client(youtube, video_ids):
    rows = []
    for chunk in _chunk_items(video_ids, 50):
        response = youtube.videos().list(
            part="snippet,status",
            id=",".join(chunk),
        ).execute()
        rows.extend(response.get("items", []))
    return rows


def _fetch_video_rows_with_localizations_with_client(youtube, video_ids):
    rows = []
    for chunk in _chunk_items(video_ids, 50):
        response = youtube.videos().list(
            part="snippet,localizations",
            id=",".join(chunk),
        ).execute()
        rows.extend(response.get("items", []))
    return rows


def _fetch_single_video_row_with_localizations_with_client(youtube, video_id):
    normalized_video_id = str(video_id or "").strip()
    if not normalized_video_id:
        return {}

    rows = _fetch_video_rows_with_localizations_with_client(youtube, [normalized_video_id])
    return dict(rows[0]) if rows else {}


def _get_effective_published_at_utc(video_row, now_utc):
    status_publish_at = _parse_youtube_datetime((video_row.get("status") or {}).get("publishAt"))
    if status_publish_at is not None:
        if status_publish_at <= now_utc:
            return status_publish_at
        return None

    return _parse_youtube_datetime((video_row.get("snippet") or {}).get("publishedAt"))


def _get_future_scheduled_publish_at_utc(video_row, now_utc):
    status_publish_at = _parse_youtube_datetime((video_row.get("status") or {}).get("publishAt"))
    if status_publish_at is not None and status_publish_at > now_utc:
        return status_publish_at
    return None


def _build_existing_video_match_from_row(video_row):
    if not isinstance(video_row, dict):
        return {}

    video_id = str(video_row.get("id") or "").strip()
    title = str(((video_row.get("snippet") or {}).get("title") or "")).strip()
    if not video_id or not title:
        return {}

    uploaded_at = _format_youtube_datetime_z((video_row.get("snippet") or {}).get("publishedAt"))
    publish_at = _format_youtube_datetime_z((video_row.get("status") or {}).get("publishAt"))
    return {
        "video_id": video_id,
        "youtube_url": f"https://youtu.be/{video_id}",
        "uploaded_at": uploaded_at,
        "publish_at": publish_at,
        "schedule_reason": "existing_title_match",
        "title": title,
    }


def _build_channel_video_title_index_with_client(youtube):
    uploads_playlist_id = _get_youtube_uploads_playlist_id_with_client(youtube)
    video_ids = _list_upload_video_ids_with_client(youtube, uploads_playlist_id)
    rows = _fetch_video_status_rows_with_client(youtube, video_ids)

    title_index = {}
    for row in rows:
        match = _build_existing_video_match_from_row(row)
        if not match:
            continue
        title_key = _normalize_youtube_title_key(match.get("title"))
        if not title_key:
            continue

        previous = title_index.get(title_key)
        current_uploaded = _parse_youtube_datetime(match.get("uploaded_at")) or dt_datetime.min.replace(tzinfo=dt_timezone.utc)
        previous_uploaded = _parse_youtube_datetime(previous.get("uploaded_at")) if previous else None
        previous_uploaded = previous_uploaded or dt_datetime.min.replace(tzinfo=dt_timezone.utc)
        if previous is None or current_uploaded >= previous_uploaded:
            title_index[title_key] = match
    return title_index


def _get_channel_video_title_index(channel_name, force_refresh=False):
    normalized_channel = str(channel_name or "").strip()
    if not normalized_channel:
        return {}

    if force_refresh or normalized_channel not in YOUTUBE_TITLE_MATCH_CACHE:
        youtube = authenticate_youtube_from_supabase(normalized_channel)
        if not youtube:
            return {}
        YOUTUBE_TITLE_MATCH_CACHE[normalized_channel] = _build_channel_video_title_index_with_client(youtube)

    return YOUTUBE_TITLE_MATCH_CACHE.get(normalized_channel, {})


def find_existing_channel_video_by_exact_title(channel_name, title, force_refresh=False):
    normalized_title = str(title or "").strip()[:100]
    title_key = _normalize_youtube_title_key(normalized_title)
    if not title_key:
        return {}

    title_index = _get_channel_video_title_index(channel_name, force_refresh=force_refresh)
    match = title_index.get(title_key, {})
    return dict(match) if isinstance(match, dict) else {}


def remember_existing_channel_video_title_match(channel_name, title, match):
    normalized_channel = str(channel_name or "").strip()
    normalized_title = str(title or "").strip()[:100]
    title_key = _normalize_youtube_title_key(normalized_title)
    if not normalized_channel or not title_key or not isinstance(match, dict):
        return

    cache_bucket = YOUTUBE_TITLE_MATCH_CACHE.setdefault(normalized_channel, {})
    cache_bucket[title_key] = dict(match)


def _collect_channel_publish_schedule_facts_with_client(youtube, now_utc):
    uploads_playlist_id = _get_youtube_uploads_playlist_id_with_client(youtube)
    video_ids = _list_upload_video_ids_with_client(youtube, uploads_playlist_id)
    if not video_ids:
        return {
            "uploads_playlist_id": uploads_playlist_id,
            "published_count_by_local_date": {},
            "future_count_by_local_date": {},
            "future_publish_times_by_local_date": {},
            "latest_future_publish_at": None,
            "video_count": 0,
        }

    rows = _fetch_video_status_rows_with_client(youtube, video_ids)
    published_count_by_local_date = {}
    future_count_by_local_date = {}
    future_publish_times_by_local_date = {}
    latest_future_publish_at = None

    for row in rows:
        published_at = _get_effective_published_at_utc(row, now_utc)
        if published_at is not None:
            local_day = published_at.astimezone(YOUTUBE_SCHEDULE_LOCAL_TIMEZONE).date().isoformat()
            published_count_by_local_date[local_day] = published_count_by_local_date.get(local_day, 0) + 1

        future_publish_at = _get_future_scheduled_publish_at_utc(row, now_utc)
        if future_publish_at is not None and (
            latest_future_publish_at is None or future_publish_at > latest_future_publish_at
        ):
            latest_future_publish_at = future_publish_at
        if future_publish_at is not None:
            local_publish_at = future_publish_at.astimezone(YOUTUBE_SCHEDULE_LOCAL_TIMEZONE).replace(microsecond=0)
            local_day = local_publish_at.date().isoformat()
            future_count_by_local_date[local_day] = future_count_by_local_date.get(local_day, 0) + 1
            future_publish_times_by_local_date.setdefault(local_day, []).append(local_publish_at)

    for local_day, items in future_publish_times_by_local_date.items():
        future_publish_times_by_local_date[local_day] = sorted(items)

    return {
        "uploads_playlist_id": uploads_playlist_id,
        "published_count_by_local_date": published_count_by_local_date,
        "future_count_by_local_date": future_count_by_local_date,
        "future_publish_times_by_local_date": future_publish_times_by_local_date,
        "latest_future_publish_at": latest_future_publish_at,
        "video_count": len(rows),
    }


def _get_youtube_daily_publish_limit():
    try:
        limit = int(globals().get("YOUTUBE_DAILY_PUBLISH_LIMIT", 3) or 3)
    except Exception:
        limit = 3
    return max(1, limit)


def _build_youtube_daily_publish_slots(target_date, base_publish_at_local, daily_limit):
    base_time = base_publish_at_local.timetz().replace(microsecond=0)
    day_start = dt_datetime.combine(target_date, base_time, tzinfo=YOUTUBE_SCHEDULE_LOCAL_TIMEZONE).replace(microsecond=0)
    day_end = day_start.replace(hour=23, minute=55, second=0, microsecond=0)
    if day_end <= day_start:
        day_end = day_start + dt_timedelta(minutes=10 * max(0, daily_limit - 1))

    if daily_limit <= 1:
        return [day_start]

    interval_seconds = max(600, int((day_end - day_start).total_seconds() // max(1, daily_limit - 1)))
    slots = []
    for slot_index in range(daily_limit):
        candidate = day_start + dt_timedelta(seconds=interval_seconds * slot_index)
        if candidate > day_end:
            candidate = day_end
        candidate = candidate.replace(microsecond=0)
        if slots and candidate <= slots[-1]:
            candidate = (slots[-1] + dt_timedelta(minutes=10)).replace(microsecond=0)
        slots.append(candidate)
    return slots


def resolve_youtube_publish_schedule_with_client(youtube, privacy_status="unlisted", schedule_after_hours=0):
    normalized_privacy = str(privacy_status or "unlisted").strip().lower()
    if normalized_privacy != "schedule":
        return {
            "publish_at": "",
            "schedule_reason": "",
            "local_publish_at": "",
            "base_publish_at": "",
            "latest_future_publish_at": "",
        }

    hours = max(1, int(schedule_after_hours or 0))
    now_utc = dt_datetime.now(dt_timezone.utc)
    base_publish_at_utc = (now_utc + dt_timedelta(hours=hours)).replace(microsecond=0)
    base_publish_at_local = base_publish_at_utc.astimezone(YOUTUBE_SCHEDULE_LOCAL_TIMEZONE).replace(microsecond=0)

    facts = _collect_channel_publish_schedule_facts_with_client(youtube, now_utc)
    latest_future_publish_at = facts.get("latest_future_publish_at")
    published_count_by_local_date = facts.get("published_count_by_local_date", {})
    future_count_by_local_date = facts.get("future_count_by_local_date", {})
    future_publish_times_by_local_date = facts.get("future_publish_times_by_local_date", {})
    daily_limit = _get_youtube_daily_publish_limit()

    schedule_reason = "base_schedule"
    final_publish_at_local = base_publish_at_local
    final_publish_at_utc = base_publish_at_utc

    candidate_day = base_publish_at_local.date()
    base_day = candidate_day
    found_slot = False
    for day_offset in range(370):
        current_day = candidate_day + dt_timedelta(days=day_offset)
        local_day_key = current_day.isoformat()
        reserved_count = int(published_count_by_local_date.get(local_day_key, 0) or 0) + int(
            future_count_by_local_date.get(local_day_key, 0) or 0
        )
        if reserved_count >= daily_limit:
            continue

        occupied_times = list(future_publish_times_by_local_date.get(local_day_key, []) or [])
        slots = _build_youtube_daily_publish_slots(current_day, base_publish_at_local, daily_limit)
        earliest_allowed = base_publish_at_local if current_day == base_day else slots[0]
        for slot in slots:
            if slot < earliest_allowed:
                continue
            if any(abs((slot - occupied).total_seconds()) < 60 for occupied in occupied_times):
                continue
            final_publish_at_local = slot
            final_publish_at_utc = slot.astimezone(dt_timezone.utc).replace(microsecond=0)
            schedule_reason = f"daily_slot_{reserved_count + 1}_of_{daily_limit}"
            found_slot = True
            break

        if not found_slot and reserved_count < daily_limit:
            fallback_anchor = max([earliest_allowed] + occupied_times) if occupied_times else earliest_allowed
            fallback_slot = (fallback_anchor + dt_timedelta(minutes=10)).replace(microsecond=0)
            if fallback_slot.date() == current_day:
                final_publish_at_local = fallback_slot
                final_publish_at_utc = fallback_slot.astimezone(dt_timezone.utc).replace(microsecond=0)
                schedule_reason = f"daily_fallback_{reserved_count + 1}_of_{daily_limit}"
                found_slot = True

        if found_slot:
            break

    publish_at = _format_youtube_datetime_z(final_publish_at_utc)
    base_publish_at = _format_youtube_datetime_z(base_publish_at_utc)
    latest_future_text = _format_youtube_datetime_z(latest_future_publish_at) if latest_future_publish_at else ""

    log.info(
        "📅 YouTube 排期决策：reason=%s | 本地发布时间=%s | UTC发布时间=%s | 基础UTC=%s | 最晚未来定时=%s | 已扫描视频=%d",
        schedule_reason,
        final_publish_at_local.isoformat(),
        publish_at,
        base_publish_at,
        latest_future_text or "无",
        int(facts.get("video_count", 0) or 0),
    )

    return {
        "publish_at": publish_at,
        "schedule_reason": schedule_reason,
        "local_publish_at": final_publish_at_local.isoformat(),
        "base_publish_at": base_publish_at,
        "latest_future_publish_at": latest_future_text,
    }

def build_youtube_status(privacy_status="unlisted", schedule_after_hours=0, publish_at=""):
    normalized = str(privacy_status or "unlisted").strip().lower()
    if normalized not in {"private", "unlisted", "public", "schedule"}:
        log.warning("未知的 YouTube 隐私设置 '%s'，已回退为 unlisted。", privacy_status)
        normalized = "unlisted"

    if normalized == "schedule":
        if publish_at:
            normalized_publish_at = _format_youtube_datetime_z(publish_at)
            log.info("📅 YouTube 预约公开已启用：使用显式 publishAt (%s)", normalized_publish_at)
            return {
                "privacyStatus": "private",
                "publishAt": normalized_publish_at,
            }

        hours = max(1, int(schedule_after_hours or 0))
        calculated_publish_at = (
            dt_datetime.now(dt_timezone.utc) + dt_timedelta(hours=hours)
        ).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        log.info("📅 YouTube 预约公开已启用：%d 小时后自动公开 (%s)", hours, calculated_publish_at)
        return {
            "privacyStatus": "private",
            "publishAt": calculated_publish_at,
        }

    return {
        "privacyStatus": normalized,
    }


def _build_video_upload_request_body(title, description, tags, privacy_status="unlisted", category_id="", schedule_after_hours=0, publish_at=""):
    tags_list = normalize_youtube_tags(tags)
    normalized_category_id = normalize_youtube_category_id(category_id)
    default_language, _generated_localizations = build_youtube_traditional_localizations(title=title, description=description)
    snippet = {
        "title": title[:100],
        "description": description[:5000],
        "defaultLanguage": default_language,
    }

    if tags_list:
        snippet["tags"] = tags_list
    if normalized_category_id:
        snippet["categoryId"] = normalized_category_id
        log.info("YouTube 分类已设置为: %s", normalized_category_id)
    else:
        log.info("YOUTUBE_CATEGORY_ID 留空，上传时不设置 categoryId。")

    return {
        "snippet": snippet,
        "status": build_youtube_status(privacy_status, schedule_after_hours, publish_at=publish_at),
    }


def _upload_to_youtube_with_client(
    youtube,
    video_path,
    title,
    description,
    tags,
    cover_path,
    privacy_status="unlisted",
    category_id="",
    schedule_after_hours=0,
    publish_at="",
    schedule_reason="",
):
    body = _build_video_upload_request_body(
        title=title,
        description=description,
        tags=tags,
        privacy_status=privacy_status,
        category_id=category_id,
        schedule_after_hours=schedule_after_hours,
        publish_at=publish_at,
    )

    log.info("🚀 开启跨国深空打孔传送视频大本尊: %s", os.path.basename(video_path))
    media = MediaFileUpload(video_path, chunksize=1024 * 1024 * 20, resumable=True)
    request = youtube.videos().insert(part="snippet,status", body=body, media_body=media)

    response = None
    retry_count = 0
    max_retries = 5
    while response is None:
        try:
            status, response = request.next_chunk()
            if status:
                log.info("   ⏳ [发送塔台播报进度]：%d%%", int(status.progress() * 100))
        except HttpError as e:
            status_code = getattr(getattr(e, "resp", None), "status", None)
            if status_code in {500, 502, 503, 504} and retry_count < max_retries:
                retry_count += 1
                wait = 2 ** retry_count
                log.warning("⚠️ 上传分片遭遇 HTTP %s，准备第 %d 次重试，等待 %d 秒...", status_code, retry_count, wait)
                time.sleep(wait)
                continue
            raise

    video_id = response["id"]
    uploaded_at = dt_datetime.now(dt_timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    youtube_url = f"https://youtu.be/{video_id}"
    log.info("🎉 本身巨盒已被 Youtube 安全收纳进柜里！影视 ID 为: %s", video_id)

    if cover_path and os.path.exists(cover_path):
        safe_cover = compress_thumbnail_to_safe_limit(cover_path)
        log.info("🖼 拦截成功！对换新生成的特种压缩皮肤套向官网推去覆盖...")
        max_thumb_retries = 3
        for attempt in range(1, max_thumb_retries + 1):
            try:
                thumb_req = youtube.thumbnails().set(
                    videoId=video_id,
                    media_body=MediaFileUpload(safe_cover)
                )
                thumb_req.execute()
                log.info("🎉 Youtube 收下了我们的大画幅爆款神眼罩！前置门面搭建收工完毕！")
                break
            except HttpError as e:
                if attempt < max_thumb_retries:
                    log.warning("⚠️ 海报被 YouTube 大气网拦下(第 %d 遭打)，下落 5 秒冷凝...", attempt)
                    time.sleep(5)
                else:
                    log.error("❌ 尽管做尽处理，但这块门面历经 %d 轮抛投后依然被封杀: %s", max_thumb_retries, e)

    localization_sync = _sync_video_localizations_with_client(
        youtube,
        video_id,
        title=title,
        description=description,
        force_overwrite=False,
    )
    if localization_sync.get("applied_locales"):
        log.info(
            "Video localizations applied for %s: %s",
            video_id,
            ", ".join(localization_sync.get("applied_locales", [])),
        )
    if localization_sync.get("failed_locales"):
        log.warning(
            "Video localization sync partially failed for %s; continuing upload success path. failed=%s",
            video_id,
            json.dumps(localization_sync.get("failed_locales", {}), ensure_ascii=False),
        )

    return {
        "video_id": video_id,
        "youtube_url": youtube_url,
        "uploaded_at": uploaded_at,
        "title": title[:100],
        "publish_at": _format_youtube_datetime_z(publish_at) if publish_at else "",
        "schedule_reason": str(schedule_reason or ""),
        "localizations_applied": localization_sync.get("applied_locales", []),
        "localizations_failed": localization_sync.get("failed_locales", {}),
    }


def upload_to_youtube_detailed(
    video_path,
    title,
    description,
    tags,
    cover_path,
    channel_name,
    privacy_status="unlisted",
    category_id="",
    schedule_after_hours=0,
):
    if not channel_name:
        log.error("未指定信标频道代码，自动丢弃发行工作。")
        return False

    youtube = authenticate_youtube_from_supabase(channel_name)
    if not youtube:
        return False

    try:
        existing_match = find_existing_channel_video_by_exact_title(channel_name, title)
        if existing_match:
            log.info("检测到频道内已存在同标题视频，直接复用并跳过上传：%s", str(title or "").strip()[:100])
            remember_existing_channel_video_title_match(channel_name, title, existing_match)
            return existing_match

        resolved_schedule = resolve_youtube_publish_schedule_with_client(
            youtube,
            privacy_status=privacy_status,
            schedule_after_hours=schedule_after_hours,
        )
        upload_result = _upload_to_youtube_with_client(
            youtube=youtube,
            video_path=video_path,
            title=title,
            description=description,
            tags=tags,
            cover_path=cover_path,
            privacy_status=privacy_status,
            category_id=category_id,
            schedule_after_hours=schedule_after_hours,
            publish_at=resolved_schedule.get("publish_at", ""),
            schedule_reason=resolved_schedule.get("schedule_reason", ""),
        )
        if upload_result:
            remember_existing_channel_video_title_match(channel_name, title, upload_result)
        return upload_result
    except Exception as e:
        log.error("❌ 主力信封管线在传输时遭受强击崩溃: %s", e)
        return False


def upload_to_youtube(
    video_path,
    title,
    description,
    tags,
    cover_path,
    channel_name,
    privacy_status="unlisted",
    category_id="",
    schedule_after_hours=0,
):
    result = upload_to_youtube_detailed(
        video_path=video_path,
        title=title,
        description=description,
        tags=tags,
        cover_path=cover_path,
        channel_name=channel_name,
        privacy_status=privacy_status,
        category_id=category_id,
        schedule_after_hours=schedule_after_hours,
    )
    return result.get("youtube_url") if isinstance(result, dict) else False


def normalize_playlist_privacy_status(privacy_status="public"):
    normalized = str(privacy_status or "public").strip().lower()
    if normalized not in {"private", "unlisted", "public"}:
        log.warning("未知的播放列表隐私设置 '%s'，已回退为 public。", privacy_status)
        normalized = "public"
    return normalized


def is_playlist_not_found_http_error(error):
    if not isinstance(error, HttpError):
        return False

    status_code = getattr(getattr(error, "resp", None), "status", None)
    raw_text = str(error)
    if "playlistNotFound" in raw_text:
        return True

    try:
        content = getattr(error, "content", b"")
        if isinstance(content, bytes):
            payload = json.loads(content.decode("utf-8", errors="ignore"))
        elif isinstance(content, str):
            payload = json.loads(content)
        else:
            payload = {}
        reasons = [
            str(item.get("reason") or "").strip()
            for item in ((payload.get("error") or {}).get("errors") or [])
            if isinstance(item, dict)
        ]
        if "playlistNotFound" in reasons:
            return True
    except Exception:
        pass

    return status_code == 404 and "playlistId" in raw_text


def _create_or_update_playlist_with_client(youtube, title, description="", privacy_status="public", playlist_id=""):
    normalized_privacy = normalize_playlist_privacy_status(privacy_status)
    default_language, _generated_localizations = build_youtube_traditional_localizations(title=title, description=description)
    body = {
        "snippet": {
            "title": str(title or "")[:150],
            "description": str(description or "")[:5000],
            "defaultLanguage": default_language,
        },
        "status": {
            "privacyStatus": normalized_privacy,
        },
    }

    if playlist_id:
        body["id"] = playlist_id
        response = youtube.playlists().update(part="snippet,status", body=body).execute()
    else:
        response = youtube.playlists().insert(part="snippet,status", body=body).execute()

    final_playlist_id = response["id"]
    localization_sync = _sync_playlist_localizations_with_client(
        youtube,
        final_playlist_id,
        title=body["snippet"]["title"],
        description=body["snippet"]["description"],
        force_overwrite=False,
    )
    if localization_sync.get("failed_locales"):
        log.warning(
            "Playlist localization sync partially failed for %s; continuing playlist success path. failed=%s",
            final_playlist_id,
            json.dumps(localization_sync.get("failed_locales", {}), ensure_ascii=False),
        )
    return {
        "playlist_id": final_playlist_id,
        "playlist_url": f"https://www.youtube.com/playlist?list={final_playlist_id}",
        "title": body["snippet"]["title"],
        "description": body["snippet"]["description"],
        "privacy_status": normalized_privacy,
        "localizations_applied": localization_sync.get("applied_locales", []),
        "localizations_failed": localization_sync.get("failed_locales", {}),
    }


def _list_playlist_items_with_client(youtube, playlist_id):
    items = []
    page_token = None
    playlist_not_found_retry_count = 0
    max_playlist_not_found_retries = 6
    while True:
        try:
            response = youtube.playlistItems().list(
                part="snippet,contentDetails",
                playlistId=playlist_id,
                maxResults=50,
                pageToken=page_token,
            ).execute()
        except HttpError as e:
            if is_playlist_not_found_http_error(e) and playlist_not_found_retry_count < max_playlist_not_found_retries:
                playlist_not_found_retry_count += 1
                wait_seconds = min(12, 2 + playlist_not_found_retry_count)
                log.warning(
                    "播放列表 %s 暂时还不可读，等待 %d 秒后重试读取（%d/%d）...",
                    playlist_id,
                    wait_seconds,
                    playlist_not_found_retry_count,
                    max_playlist_not_found_retries,
                )
                time.sleep(wait_seconds)
                page_token = None
                items = []
                continue
            raise

        for item in response.get("items", []):
            resource = ((item.get("snippet") or {}).get("resourceId") or {})
            video_id = resource.get("videoId") or (item.get("contentDetails") or {}).get("videoId") or ""
            items.append(
                {
                    "playlist_item_id": item.get("id", ""),
                    "video_id": video_id,
                    "position": int((item.get("snippet") or {}).get("position", 0) or 0),
                }
            )
        page_token = response.get("nextPageToken")
        if not page_token:
            break
    return items


def _list_owned_playlists_with_client(youtube):
    playlists = []
    page_token = None
    while True:
        response = youtube.playlists().list(
            part="snippet,status",
            mine=True,
            maxResults=50,
            pageToken=page_token,
        ).execute()
        for item in response.get("items", []):
            playlists.append(
                {
                    "playlist_id": str(item.get("id") or "").strip(),
                    "playlist_url": f"https://www.youtube.com/playlist?list={str(item.get('id') or '').strip()}",
                    "title": str((item.get("snippet") or {}).get("title") or "").strip(),
                    "description": str((item.get("snippet") or {}).get("description") or ""),
                    "privacy_status": normalize_playlist_privacy_status(
                        (item.get("status") or {}).get("privacyStatus") or "public"
                    ),
                }
            )
        page_token = response.get("nextPageToken")
        if not page_token:
            break
    return playlists


def _list_owned_playlist_rows_with_localizations_with_client(youtube):
    rows = []
    page_token = None
    while True:
        response = youtube.playlists().list(
            part="snippet,status,localizations",
            mine=True,
            maxResults=50,
            pageToken=page_token,
        ).execute()
        rows.extend(response.get("items", []))
        page_token = response.get("nextPageToken")
        if not page_token:
            break
    return rows


def _load_playlist_localizations_with_client(youtube, playlist_id):
    normalized_playlist_id = str(playlist_id or "").strip()
    if not normalized_playlist_id:
        return {}

    response = youtube.playlists().list(
        part="localizations",
        id=normalized_playlist_id,
        maxResults=1,
    ).execute()
    items = response.get("items", [])
    if not items:
        return {}
    return dict(items[0].get("localizations") or {})


def _fetch_single_playlist_row_with_localizations_with_client(youtube, playlist_id):
    normalized_playlist_id = str(playlist_id or "").strip()
    if not normalized_playlist_id:
        return {}

    playlist_not_found_retry_count = 0
    max_playlist_not_found_retries = 6
    while True:
        try:
            response = youtube.playlists().list(
                part="snippet,localizations",
                id=normalized_playlist_id,
                maxResults=1,
            ).execute()
        except HttpError as e:
            if is_playlist_not_found_http_error(e) and playlist_not_found_retry_count < max_playlist_not_found_retries:
                playlist_not_found_retry_count += 1
                wait_seconds = min(12, 2 + playlist_not_found_retry_count)
                log.warning(
                    "播放列表 %s 暂时还不可读，等待 %d 秒后重试读取（%d/%d）...",
                    normalized_playlist_id,
                    wait_seconds,
                    playlist_not_found_retry_count,
                    max_playlist_not_found_retries,
                )
                time.sleep(wait_seconds)
                continue
            raise

        items = response.get("items", [])
        if items:
            return dict(items[0])
        if playlist_not_found_retry_count < max_playlist_not_found_retries:
            playlist_not_found_retry_count += 1
            wait_seconds = min(12, 2 + playlist_not_found_retry_count)
            log.warning(
                "播放列表 %s 暂时还不可读，等待 %d 秒后重试读取（%d/%d）...",
                normalized_playlist_id,
                wait_seconds,
                playlist_not_found_retry_count,
                max_playlist_not_found_retries,
            )
            time.sleep(wait_seconds)
            continue
        return {}


def _sync_video_localizations_with_client(youtube, video_id, title="", description="", force_overwrite=False):
    normalized_video_id = str(video_id or "").strip()
    if not normalized_video_id:
        return {"applied_locales": [], "skipped_locales": [], "failed_locales": {}}

    video_row = _fetch_single_video_row_with_localizations_with_client(youtube, normalized_video_id)
    if not video_row:
        log.warning("Unable to fetch uploaded video row for localization sync: video_id=%s", normalized_video_id)
        return {"applied_locales": [], "skipped_locales": [], "failed_locales": {}}

    snippet = dict(video_row.get("snippet") or {})
    effective_title = str(title or snippet.get("title") or "")[:100]
    effective_description = str(description or snippet.get("description") or "")[:5000]
    default_language, generated = build_youtube_traditional_localizations(
        title=effective_title,
        description=effective_description,
    )
    if not generated:
        return {"applied_locales": [], "skipped_locales": [], "failed_locales": {}}

    base_snippet = _build_youtube_mutable_video_snippet(snippet, default_language=default_language)
    base_snippet["title"] = effective_title
    base_snippet["description"] = effective_description
    current_localizations = dict(video_row.get("localizations") or {})
    applied_locales = []
    skipped_locales = []
    failed_locales = {}

    for target_locale, localized_entry in generated.items():
        if current_localizations.get(target_locale) and not force_overwrite:
            skipped_locales.append(target_locale)
            continue
        if current_localizations.get(target_locale) == localized_entry:
            skipped_locales.append(target_locale)
            continue

        body = {
            "id": normalized_video_id,
            "snippet": dict(base_snippet),
            "localizations": dict(current_localizations),
        }
        body["localizations"][target_locale] = localized_entry
        try:
            youtube.videos().update(part="snippet,localizations", body=body).execute()
            current_localizations[target_locale] = localized_entry
            applied_locales.append(target_locale)
        except HttpError as e:
            failed_locales[target_locale] = str(e)
            log.warning(
                "Skipping rejected video localization locale=%s video_id=%s title=%s error=%s",
                target_locale,
                normalized_video_id,
                effective_title,
                e,
            )
        except Exception as e:
            failed_locales[target_locale] = str(e)
            log.warning(
                "Video localization sync failed locale=%s video_id=%s title=%s error=%s",
                target_locale,
                normalized_video_id,
                effective_title,
                e,
            )

    return {
        "applied_locales": applied_locales,
        "skipped_locales": skipped_locales,
        "failed_locales": failed_locales,
    }


def _sync_playlist_localizations_with_client(youtube, playlist_id, title="", description="", force_overwrite=False):
    normalized_playlist_id = str(playlist_id or "").strip()
    if not normalized_playlist_id:
        return {"applied_locales": [], "skipped_locales": [], "failed_locales": {}}

    playlist_row = _fetch_single_playlist_row_with_localizations_with_client(youtube, normalized_playlist_id)
    if not playlist_row:
        log.warning("Unable to fetch playlist row for localization sync after retries: playlist_id=%s", normalized_playlist_id)
        return {"applied_locales": [], "skipped_locales": [], "failed_locales": {}}

    snippet = dict(playlist_row.get("snippet") or {})
    effective_title = str(title or snippet.get("title") or "")[:150]
    effective_description = str(description or snippet.get("description") or "")[:5000]
    default_language, generated = build_youtube_traditional_localizations(
        title=effective_title,
        description=effective_description,
    )
    if not generated:
        return {"applied_locales": [], "skipped_locales": [], "failed_locales": {}}

    base_snippet = {
        "title": effective_title,
        "description": effective_description,
        "defaultLanguage": str(
            snippet.get("defaultLanguage") or default_language or get_youtube_default_language()
        ).strip(),
    }
    current_localizations = dict(playlist_row.get("localizations") or {})
    applied_locales = []
    skipped_locales = []
    failed_locales = {}

    for target_locale, localized_entry in generated.items():
        if current_localizations.get(target_locale) and not force_overwrite:
            skipped_locales.append(target_locale)
            continue
        if current_localizations.get(target_locale) == localized_entry:
            skipped_locales.append(target_locale)
            continue

        body = {
            "id": normalized_playlist_id,
            "snippet": dict(base_snippet),
            "localizations": dict(current_localizations),
        }
        body["localizations"][target_locale] = localized_entry
        try:
            youtube.playlists().update(part="snippet,localizations", body=body).execute()
            current_localizations[target_locale] = localized_entry
            applied_locales.append(target_locale)
        except HttpError as e:
            failed_locales[target_locale] = str(e)
            log.warning(
                "Skipping rejected playlist localization locale=%s playlist_id=%s title=%s error=%s",
                target_locale,
                normalized_playlist_id,
                effective_title,
                e,
            )
        except Exception as e:
            failed_locales[target_locale] = str(e)
            log.warning(
                "Playlist localization sync failed locale=%s playlist_id=%s title=%s error=%s",
                target_locale,
                normalized_playlist_id,
                effective_title,
                e,
            )

    return {
        "applied_locales": applied_locales,
        "skipped_locales": skipped_locales,
        "failed_locales": failed_locales,
    }


def _build_playlist_localizations_update_body_from_row(playlist_row, force_overwrite=False):
    if not isinstance(playlist_row, dict):
        return {}

    playlist_id = str(playlist_row.get("id") or "").strip()
    snippet = dict(playlist_row.get("snippet") or {})
    title = str(snippet.get("title") or "")[:150]
    description = str(snippet.get("description") or "")[:5000]
    if not playlist_id or (not title and not description):
        return {}

    default_language, merged_localizations, changed = merge_youtube_localizations(
        existing_localizations=playlist_row.get("localizations") or {},
        title=title,
        description=description,
        force_overwrite=force_overwrite,
    )
    if not changed:
        return {}

    return {
        "id": playlist_id,
        "snippet": {
            "title": title,
            "description": description,
            "defaultLanguage": str(snippet.get("defaultLanguage") or default_language or get_youtube_default_language()).strip(),
        },
        "localizations": merged_localizations,
    }


def _build_video_localizations_update_body_from_row(video_row, force_overwrite=False):
    if not isinstance(video_row, dict):
        return {}

    video_id = str(video_row.get("id") or "").strip()
    snippet = dict(video_row.get("snippet") or {})
    title = str(snippet.get("title") or "")[:100]
    description = str(snippet.get("description") or "")[:5000]
    if not video_id or (not title and not description):
        return {}

    default_language, merged_localizations, changed = merge_youtube_localizations(
        existing_localizations=video_row.get("localizations") or {},
        title=title,
        description=description,
        force_overwrite=force_overwrite,
    )
    if not changed:
        return {}

    return {
        "id": video_id,
        "snippet": _build_youtube_mutable_video_snippet(snippet, default_language=default_language),
        "localizations": merged_localizations,
    }


def backfill_youtube_traditional_localizations(
    channel_name="",
    apply=False,
    max_videos=0,
    include_videos=True,
    include_playlists=True,
    force_overwrite=False,
):
    normalized_channel = str(channel_name or globals().get("YOUTUBE_CHANNEL_NAME", "") or "").strip()
    if not normalized_channel:
        raise RuntimeError("YOUTUBE_CHANNEL_NAME is required to backfill YouTube localizations.")

    youtube = authenticate_youtube_from_supabase(normalized_channel)
    if not youtube:
        raise RuntimeError(f"Unable to initialize YouTube client for channel {normalized_channel!r}.")

    summary = {
        "channel_name": normalized_channel,
        "apply": bool(apply),
        "video_updated": 0,
        "video_skipped": 0,
        "playlist_updated": 0,
        "playlist_skipped": 0,
        "target_locales": get_youtube_localization_locales(),
        "default_language": get_youtube_default_language(),
    }

    if include_videos:
        uploads_playlist_id = _get_youtube_uploads_playlist_id_with_client(youtube)
        video_limit = int(max_videos or 0)
        video_ids = _list_upload_video_ids_with_client(
            youtube,
            uploads_playlist_id,
            max_videos=video_limit if video_limit > 0 else 10 ** 9,
        )
        for video_row in _fetch_video_rows_with_localizations_with_client(youtube, video_ids):
            body = _build_video_localizations_update_body_from_row(video_row, force_overwrite=force_overwrite)
            if not body:
                summary["video_skipped"] += 1
                continue
            if apply:
                youtube.videos().update(part="snippet,localizations", body=body).execute()
                log.info(
                    "Updated Chinese locale localizations for video %s: %s",
                    body.get("id"),
                    str((body.get("snippet") or {}).get("title") or ""),
                )
            else:
                log.info(
                    "Dry-run: video %s would receive Chinese locale localizations: %s",
                    body.get("id"),
                    str((body.get("snippet") or {}).get("title") or ""),
                )
            summary["video_updated"] += 1

    if include_playlists:
        builtin_playlist_ids = _get_builtin_playlist_ids_with_client(youtube)
        for playlist_row in _list_owned_playlist_rows_with_localizations_with_client(youtube):
            playlist_id = str(playlist_row.get("id") or "").strip()
            if playlist_id in builtin_playlist_ids:
                summary["playlist_skipped"] += 1
                continue
            body = _build_playlist_localizations_update_body_from_row(
                playlist_row,
                force_overwrite=force_overwrite,
            )
            if not body:
                summary["playlist_skipped"] += 1
                continue
            if apply:
                youtube.playlists().update(part="snippet,localizations", body=body).execute()
                log.info(
                    "Updated Chinese locale localizations for playlist %s: %s",
                    body.get("id"),
                    str((body.get("snippet") or {}).get("title") or ""),
                )
            else:
                log.info(
                    "Dry-run: playlist %s would receive Chinese locale localizations: %s",
                    body.get("id"),
                    str((body.get("snippet") or {}).get("title") or ""),
                )
            summary["playlist_updated"] += 1

    log.info("YouTube Chinese locale localization backfill summary: %s", summary)
    return summary


def _find_matching_owned_playlist_with_client(youtube, title, ordered_video_ids=None, privacy_status="public"):
    normalized_title = str(title or "").strip()
    desired_video_ids = [str(video_id).strip() for video_id in (ordered_video_ids or []) if str(video_id).strip()]
    normalized_privacy = normalize_playlist_privacy_status(privacy_status)
    if not normalized_title:
        return {}

    title_matches = []
    for playlist in _list_owned_playlists_with_client(youtube):
        if str(playlist.get("title") or "").strip() != normalized_title:
            continue
        title_matches.append(playlist)

    if not title_matches:
        return {}

    exact_content_match = {}
    privacy_match = {}
    for playlist in title_matches:
        playlist_id = str(playlist.get("playlist_id") or "").strip()
        if not playlist_id:
            continue
        if desired_video_ids:
            try:
                playlist_items = _list_playlist_items_with_client(youtube, playlist_id)
            except Exception:
                playlist_items = []
            existing_video_ids = [str(item.get("video_id") or "").strip() for item in playlist_items if str(item.get("video_id") or "").strip()]
            if existing_video_ids == desired_video_ids:
                exact_content_match = playlist
                if str(playlist.get("privacy_status") or "").strip().lower() == normalized_privacy:
                    return playlist
        if not privacy_match and str(playlist.get("privacy_status") or "").strip().lower() == normalized_privacy:
            privacy_match = playlist

    if exact_content_match:
        return exact_content_match
    if privacy_match:
        return privacy_match
    return title_matches[0]


def _delete_playlist_item_with_client(youtube, playlist_item_id):
    youtube.playlistItems().delete(id=playlist_item_id).execute()


def _insert_playlist_video_with_client(youtube, playlist_id, video_id):
    response = youtube.playlistItems().insert(
        part="snippet",
        body={
            "snippet": {
                "playlistId": playlist_id,
                "resourceId": {
                    "kind": "youtube#video",
                    "videoId": video_id,
                }
            }
        },
    ).execute()
    return {
        "playlist_item_id": response.get("id", ""),
        "video_id": video_id,
    }


def _update_playlist_item_position_with_client(youtube, playlist_item_id, playlist_id, video_id, position):
    youtube.playlistItems().update(
        part="snippet",
        body={
            "id": playlist_item_id,
            "snippet": {
                "playlistId": playlist_id,
                "resourceId": {
                    "kind": "youtube#video",
                    "videoId": video_id,
                },
                "position": int(position),
            },
        },
    ).execute()


def sync_youtube_playlist(channel_name, title, description, ordered_video_ids, privacy_status="public", playlist_id=""):
    if not channel_name:
        log.error("未指定信标频道代码，无法同步 YouTube 播放列表。")
        return False

    ordered_video_ids = [str(video_id).strip() for video_id in ordered_video_ids if str(video_id).strip()]
    if not ordered_video_ids:
        log.warning("播放列表同步跳过：没有可加入的 YouTube 视频 ID。")
        return False

    youtube = authenticate_youtube_from_supabase(channel_name)
    if not youtube:
        return False

    playlist_result = {
        "playlist_id": str(playlist_id or ""),
        "playlist_url": f"https://www.youtube.com/playlist?list={playlist_id}" if playlist_id else "",
        "title": str(title or "")[:150],
        "description": str(description or "")[:5000],
        "privacy_status": normalize_playlist_privacy_status(privacy_status),
        "success": False,
        "error": "",
    }
    live_rows_by_id, missing_video_ids = _wait_for_live_video_rows_with_client(
        youtube,
        ordered_video_ids,
        max_attempts=3,
        context_label=str(title or "").strip()[:80] or "playlist-sync",
    )
    if missing_video_ids:
        playlist_result["error"] = (
            "One or more uploaded YouTube videos are no longer accessible: "
            + ",".join(missing_video_ids)
        )
        log.error(
            "Cannot sync YouTube playlist because some uploaded videos are missing. title=%s missing_video_ids=%s",
            str(title or "").strip()[:150],
            ",".join(missing_video_ids),
        )
        return playlist_result

    original_playlist_id = str(playlist_id or "").strip()
    if not original_playlist_id:
        recovered_playlist = _find_matching_owned_playlist_with_client(
            youtube,
            title=title,
            ordered_video_ids=ordered_video_ids,
            privacy_status=privacy_status,
        )
        recovered_playlist_id = str(recovered_playlist.get("playlist_id") or "").strip() if isinstance(recovered_playlist, dict) else ""
        if recovered_playlist_id:
            playlist_id = recovered_playlist_id
            playlist_result.update(recovered_playlist)
            log.info(
                "Detected an existing owned playlist with the same title and adopted it for sync: playlist_id=%s title=%s",
                recovered_playlist_id,
                str(recovered_playlist.get("title") or title or ""),
            )

    for attempt_index in range(2):
        current_video_id = ""
        current_action = ""
        try:
            playlist_result = _create_or_update_playlist_with_client(
                youtube,
                title=title,
                description=description,
                privacy_status=privacy_status,
                playlist_id=playlist_id,
            )
            playlist_id = playlist_result["playlist_id"]
            desired_set = set(ordered_video_ids)

            existing_items = _list_playlist_items_with_client(youtube, playlist_id)
            grouped_items = {}
            for item in existing_items:
                grouped_items.setdefault(item["video_id"], []).append(item)

            for video_id, items in grouped_items.items():
                items.sort(key=lambda x: x["position"])
                items_to_delete = []
                if video_id not in desired_set:
                    items_to_delete = items
                elif len(items) > 1:
                    items_to_delete = items[1:]

                for item in items_to_delete:
                    if item.get("playlist_item_id"):
                        _delete_playlist_item_with_client(youtube, item["playlist_item_id"])

            existing_items = _list_playlist_items_with_client(youtube, playlist_id)
            existing_video_ids = {item["video_id"] for item in existing_items}
            for video_id in ordered_video_ids:
                if video_id not in existing_video_ids:
                    current_video_id = video_id
                    current_action = "insert"
                    _insert_playlist_video_with_client(youtube, playlist_id, video_id)

            latest_items = _list_playlist_items_with_client(youtube, playlist_id)
            item_map = {}
            for item in latest_items:
                if item["video_id"] in desired_set and item["video_id"] not in item_map:
                    item_map[item["video_id"]] = item

            for position, video_id in enumerate(ordered_video_ids):
                item = item_map.get(video_id)
                if not item:
                    continue
                if int(item.get("position", -1)) != position:
                    current_video_id = video_id
                    current_action = "reorder"
                    _update_playlist_item_position_with_client(
                        youtube,
                        playlist_item_id=item["playlist_item_id"],
                        playlist_id=playlist_id,
                        video_id=video_id,
                        position=position,
                    )

            latest_items = _list_playlist_items_with_client(youtube, playlist_id)
            final_item_map = {}
            for item in latest_items:
                if item["video_id"] in desired_set and item["video_id"] not in final_item_map:
                    final_item_map[item["video_id"]] = item["playlist_item_id"]

            playlist_result["video_ids"] = ordered_video_ids
            playlist_result["playlist_item_map"] = final_item_map
            playlist_result["success"] = True
            return playlist_result
        except HttpError as e:
            if original_playlist_id and attempt_index == 0 and is_playlist_not_found_http_error(e):
                log.warning(
                    "检测到状态里保存的旧 playlist_id=%s 已失效，将自动放弃旧 ID 并重建播放列表。",
                    original_playlist_id,
                )
                playlist_id = ""
                playlist_result["playlist_id"] = ""
                playlist_result["playlist_url"] = ""
                continue
            log.error("❌ 同步 YouTube 播放列表失败: %s", e)
            return playlist_result
        except Exception as e:
            log.error("❌ 同步 YouTube 播放列表失败: %s", e)
            return playlist_result

    return playlist_result


import base64
import datetime as dt_module
from urllib.parse import urlparse, parse_qs

@dataclass
class BookResult:
    """单本书的处理结果。"""

    book_id: str = ""
    book_name: str = ""
    category: str = ""
    chapter_count: int = 0
    success_count: int = 0
    chapter_audio_paths: list = field(default_factory=list)
    merged_audio_path: str = ""
    mixed_audio_path: str = ""
    cover_image_path: str = ""
    video_path: str = ""
    seo_text_path: str = ""
    seo_title: str = ""
    seo_description: str = ""
    seo_tags: str = ""
    youtube_chapters: str = ""
    youtube_url: str = ""
    youtube_urls: list = field(default_factory=list)
    youtube_publish_at: str = ""
    youtube_schedule_reason: str = ""
    playlist_id: str = ""
    playlist_url: str = ""
    playlist_title: str = ""
    part_results: list = field(default_factory=list)
    part_count: int = 1
    completed_part_count: int = 0
    playlist_required: bool = False
    playlist_completed: bool = False
    estimated_total_duration_seconds: int = 0
    split_mode: bool = False
    pending_resume: bool = False
    stop_requested: bool = False
    state_path: str = ""
    audio_ready: bool = False
    video_ready: bool = False
    upload_ready: bool = False
    success: bool = False
    skipped: bool = False
    deleted_from_books: bool = False
    skipped_reason: str = ""
    error: str = ""


def prepare_book_cover_and_seo(result, book_data, book_dir, safe_name, book_name):
    ai_cover_target_path = os.path.join(book_dir, f"{safe_name}_cover.jpg")
    seo_path_ai = os.path.join(book_dir, f"{safe_name}_seo_description.json")
    ai_cover_ready = bool(
        result.cover_image_path
        and os.path.exists(result.cover_image_path)
        and os.path.getsize(result.cover_image_path) > 0
        and os.path.abspath(result.cover_image_path) == os.path.abspath(ai_cover_target_path)
    )
    seo_ready = bool(
        result.seo_text_path
        and os.path.exists(result.seo_text_path)
        and os.path.getsize(result.seo_text_path) > 0
    )
    cover_ready = _is_nonempty_local_file(result.cover_image_path)
    fallback_cover_path = result.cover_image_path if cover_ready and not ai_cover_ready else ""

    pic_url = book_data.get("picUrl", "")
    if pic_url:
        ext = pic_url.split("?")[0].rsplit(".", 1)[-1] or "jpg"
        cover_path = os.path.join(book_dir, f"cover.{ext}")
        if download_file(pic_url, cover_path):
            fallback_cover_path = cover_path
            if not ai_cover_ready:
                result.cover_image_path = cover_path
                cover_ready = True
            log.info("原始封面已准备完成：%s", os.path.basename(cover_path))

    if ENABLE_COVER_GENERATION and not ai_cover_ready and SKIP_EXISTING and os.path.exists(ai_cover_target_path) and os.path.getsize(ai_cover_target_path) > 0:
        result.cover_image_path = ai_cover_target_path
        ai_cover_ready = True
        cover_ready = True
        log.info("[%s] 复用已生成的 AI 封面。", book_name)

    if ENABLE_SEO_GENERATION and not seo_ready and SKIP_EXISTING and os.path.exists(seo_path_ai) and os.path.getsize(seo_path_ai) > 0:
        seo_dict = read_json_file(seo_path_ai, default={}) or {}
        if isinstance(seo_dict, dict):
            result.seo_text_path = seo_path_ai
            result.seo_title = seo_dict.get("title", "")
            result.seo_description = seo_dict.get("Description", "")
            result.seo_tags = seo_dict.get("label", "")
            seo_ready = True
            log.info("[%s] 复用已生成的 SEO 文案。", book_name)

    needs_modelscope_token = (ENABLE_COVER_GENERATION and not ai_cover_ready) or (ENABLE_SEO_GENERATION and not seo_ready)
    token_pool = {}
    if needs_modelscope_token:
        resolved_modelscope_token = resolve_modelscope_token(str(YOUTUBE_CHANNEL_NAME).strip())
        token_pool = build_modelscope_token_pool_bundle(resolved_modelscope_token, shuffle_once=True)
        if not any(token_pool.values()):
            raise RuntimeError("未能解析出可用的 ModelScope Token，无法继续 AI 生成。")

    if ENABLE_COVER_GENERATION and not ai_cover_ready:
        book_desc_text = str(book_data.get("keyWord", "")) + " " + str(book_data.get("bookDescription", ""))
        try:
            ok_cover = auto_create_youtube_cover(book_name, book_desc_text, ai_cover_target_path, token_pool, VIDEO_RESOLUTION)
        except CoverGenerationPolicyRejectedError as e:
            if not _is_nonempty_local_file(fallback_cover_path):
                raise RuntimeError("AI 封面命中提供商审核拒绝，且 books 数据中没有可用封面可回退，停止后续处理。") from e

            result.cover_image_path = _persist_cover_fallback_image(fallback_cover_path, ai_cover_target_path)
            cover_ready = _is_nonempty_local_file(result.cover_image_path)
            ai_cover_ready = os.path.abspath(result.cover_image_path) == os.path.abspath(ai_cover_target_path) and cover_ready
            log.warning(
                "[%s] AI 封面命中提供商审核拒绝，已停止继续重试并改用 books 数据封面：%s | %s",
                book_name,
                os.path.basename(result.cover_image_path),
                e,
            )
            ok_cover = True
        if not ok_cover:
            raise RuntimeError("AI 封面生成未成功，停止后续处理。")
        if _is_nonempty_local_file(ai_cover_target_path):
            result.cover_image_path = ai_cover_target_path
            ai_cover_ready = True
            cover_ready = True

    if ENABLE_SEO_GENERATION and not seo_ready:
        book_desc_text = str(book_data.get("keyWord", "")) + " " + str(book_data.get("bookDescription", ""))
        ok_seo, seo_dict = auto_create_youtube_seo(book_name, book_desc_text, seo_path_ai, token_pool)
        if not ok_seo or not isinstance(seo_dict, dict):
            raise RuntimeError("SEO 文案生成未成功，停止后续处理。")
        result.seo_text_path = seo_path_ai
        result.seo_title = seo_dict.get("title", "")
        result.seo_description = seo_dict.get("Description", "")
        result.seo_tags = seo_dict.get("label", "")
        seo_ready = True

    cover_ready = _is_nonempty_local_file(result.cover_image_path)
    if ENABLE_COVER_GENERATION and not cover_ready:
        raise RuntimeError("已开启 AI 封面生成，但封面既未生成成功，也没有可用的 books 封面可回退，停止后续处理。")

    if ENABLE_SEO_GENERATION and not seo_ready:
        raise RuntimeError("已开启 SEO 生成，但文案未生成成功，停止后续处理。")

    return result


def get_split_shared_assets(state):
    shared = state.get("shared_assets")
    if isinstance(shared, dict):
        return shared

    state["shared_assets"] = {}
    return state["shared_assets"]


def get_split_playlist_state(state):
    playlist = state.get("playlist")
    if isinstance(playlist, dict):
        return playlist

    state["playlist"] = {}
    return state["playlist"]


def restore_split_shared_assets_from_state(result, state, book_dir, safe_name, book_name):
    shared = get_split_shared_assets(state)
    restored_items = []

    seo_title = str(shared.get("seo_title") or "").strip()
    seo_description = str(shared.get("seo_description") or "")
    seo_tags = str(shared.get("seo_tags") or "")
    if seo_title or seo_description or seo_tags:
        seo_path = os.path.join(book_dir, f"{safe_name}_seo_description.json")
        seo_dict = {
            "title": seo_title,
            "Description": seo_description,
            "label": seo_tags,
        }
        try:
            if not (os.path.exists(seo_path) and os.path.getsize(seo_path) > 0):
                write_json_file(seo_path, seo_dict)
            result.seo_text_path = seo_path
            result.seo_title = seo_title
            result.seo_description = seo_description
            result.seo_tags = seo_tags
            restored_items.append("SEO")
        except Exception as e:
            log.warning("[%s] 从数据库状态恢复 SEO 文案失败: %s", book_name, e)

    cover_base64 = str(shared.get("cover_image_base64") or "").strip()
    cover_filename = str(shared.get("cover_filename") or f"{safe_name}_cover.jpg").strip()
    if cover_base64:
        cover_path = os.path.join(book_dir, os.path.basename(cover_filename))
        try:
            if not (os.path.exists(cover_path) and os.path.getsize(cover_path) > 0):
                os.makedirs(os.path.dirname(cover_path), exist_ok=True)
                with open(cover_path, "wb") as handle:
                    handle.write(base64.b64decode(cover_base64.encode("ascii")))
            if os.path.exists(cover_path) and os.path.getsize(cover_path) > 0:
                result.cover_image_path = cover_path
                restored_items.append("封面")
        except Exception as e:
            log.warning("[%s] 从数据库状态恢复共享封面失败: %s", book_name, e)

    if restored_items:
        log.info("[%s] 已从数据库状态恢复长音频共享%s。", book_name, "与".join(restored_items))

    return result


def persist_split_shared_assets_to_state(book_record, state, result, book_dir, safe_name, book_name):
    shared = get_split_shared_assets(state)

    shared["seo_title"] = str(result.seo_title or "")
    shared["seo_description"] = str(result.seo_description or "")
    shared["seo_tags"] = str(result.seo_tags or "")
    shared["cover_filename"] = ""

    if result.seo_title or result.seo_description or result.seo_tags:
        shared["seo_json_filename"] = f"{safe_name}_seo_description.json"

    if result.cover_image_path and os.path.exists(result.cover_image_path) and os.path.getsize(result.cover_image_path) > 0:
        try:
            shared["cover_filename"] = os.path.basename(result.cover_image_path)
            with open(result.cover_image_path, "rb") as handle:
                shared["cover_image_base64"] = base64.b64encode(handle.read()).decode("ascii")
        except Exception as e:
            log.warning("[%s] 写入数据库共享封面前读取本地文件失败: %s", book_name, e)

    shared["shared_title_without_prefix"] = str(result.seo_title or book_name or "")
    shared["shared_description"] = str(result.seo_description or "")
    shared["shared_tags"] = str(result.seo_tags or "")
    shared["shared_cover_path"] = str(result.cover_image_path or "")
    shared["synced_at"] = dt_module.datetime.now().isoformat()

    state_ref = save_split_processing_state(book_record, state)
    result.state_path = state_ref
    return state_ref


def build_standard_processing_state(book_record):
    existing = load_split_processing_state(book_record) or {}
    existing_mode = str(existing.get("mode") or "").strip().lower() if isinstance(existing, dict) else ""
    existing_shared_assets = existing.get("shared_assets") if isinstance(existing.get("shared_assets"), dict) else {}
    now = dt_module.datetime.now().isoformat()

    return {
        "state_version": 5,
        "mode": "standard_upload",
        "book_id": str(book_record.get("book_id", "")),
        "book_name": book_record.get("book_name", ""),
        "category": book_record.get("category", ""),
        "part_count": 1,
        "parts": [],
        "shared_assets": existing_shared_assets,
        "last_stage": existing.get("last_stage", "standard_assets_pending") if existing_mode == "standard_upload" else "standard_assets_pending",
        "last_error": existing.get("last_error", "") if existing_mode == "standard_upload" else "",
        "pending_resume": True,
        "created_at": existing.get("created_at") if existing_mode == "standard_upload" else now,
    }


def prepare_standard_book_cover_and_seo_with_state(result, book_record, book_data, book_dir, safe_name, book_name):
    state = build_standard_processing_state(book_record)
    restore_split_shared_assets_from_state(result, state, book_dir, safe_name, book_name)
    prepare_book_cover_and_seo(result, book_data, book_dir, safe_name, book_name)
    state["last_stage"] = "standard_shared_assets_ready"
    state["last_error"] = ""
    state_ref = persist_split_shared_assets_to_state(book_record, state, result, book_dir, safe_name, book_name)
    result.state_path = state_ref
    return state_ref, state


def build_ordered_split_video_records(state, split_plan):
    records = []
    for part_plan in split_plan.get("parts", []):
        part_state = get_split_part_state(state, part_plan["part_index"]) or {}
        video_id = str(part_state.get("video_id") or "").strip()
        youtube_url = str(part_state.get("youtube_url") or "").strip()
        if not video_id and youtube_url:
            video_id = _extract_youtube_video_id(youtube_url)
        if not video_id:
            continue

        records.append(
            {
                "part_index": part_plan["part_index"],
                "video_id": video_id,
                "youtube_url": youtube_url or f"https://youtu.be/{video_id}",
                "youtube_title": str(part_state.get("youtube_title") or ""),
                "uploaded_at": str(part_state.get("uploaded_at") or ""),
            }
        )

    def sort_key(item):
        uploaded_at = item.get("uploaded_at", "")
        if uploaded_at:
            return (0, uploaded_at, int(item.get("part_index", 0)))
        return (1, "", int(item.get("part_index", 0)))

    return sorted(records, key=sort_key)


def build_split_playlist_description(result, ordered_video_records):
    base_desc = str(result.seo_description or "").strip()
    link_lines = []
    for record in ordered_video_records:
        title = str(record.get("youtube_title") or "").strip()
        url = str(record.get("youtube_url") or "").strip()
        if not url:
            continue
        link_lines.append(f"{title}: {url}" if title else url)

    if link_lines and base_desc:
        return base_desc + "\n\n分片链接:\n" + "\n".join(link_lines)
    if link_lines:
        return "分片链接:\n" + "\n".join(link_lines)
    return base_desc


def sync_split_playlist(result, state, split_plan, book_record, book_name):
    playlist_state = get_split_playlist_state(state)
    ordered_video_records = build_ordered_split_video_records(state, split_plan)
    expected_count = len(split_plan.get("parts", []))

    if len(ordered_video_records) != expected_count:
        raise RuntimeError("分片视频尚未全部上传成功，暂不能创建播放列表")

    ordered_video_ids = [item["video_id"] for item in ordered_video_records]
    shared = get_split_shared_assets(state)
    playlist_title = str(shared.get("shared_title_without_prefix") or result.seo_title or book_name or "").strip()
    playlist_description = build_split_playlist_description(result, ordered_video_records)

    playlist_state["title"] = playlist_title
    playlist_state["description"] = playlist_description
    playlist_state["privacy_status"] = "public"
    playlist_state["status"] = "syncing"
    playlist_state["video_ids"] = ordered_video_ids
    playlist_state["last_error"] = ""
    state["last_stage"] = "playlist_syncing"
    save_split_processing_state(book_record, state)

    sync_result = sync_youtube_playlist(
        channel_name=str(YOUTUBE_CHANNEL_NAME).strip(),
        playlist_id=str(playlist_state.get("playlist_id") or ""),
        title=playlist_title,
        description=playlist_description,
        ordered_video_ids=ordered_video_ids,
        privacy_status="public",
    )
    if isinstance(sync_result, dict) and sync_result.get("playlist_id"):
        playlist_state["playlist_id"] = sync_result.get("playlist_id", "")
        playlist_state["playlist_url"] = sync_result.get("playlist_url", "")
        playlist_state["title"] = sync_result.get("title", playlist_title)
        playlist_state["description"] = sync_result.get("description", playlist_description)
        playlist_state["privacy_status"] = sync_result.get("privacy_status", "public")
        save_split_processing_state(book_record, state)

    if not sync_result or not sync_result.get("success", False):
        sync_error = ""
        if isinstance(sync_result, dict):
            sync_error = str(sync_result.get("error") or "").strip()
        raise RuntimeError(sync_error or "YouTube 播放列表同步失败")

    playlist_state["playlist_id"] = sync_result.get("playlist_id", "")
    playlist_state["playlist_url"] = sync_result.get("playlist_url", "")
    playlist_state["title"] = sync_result.get("title", playlist_title)
    playlist_state["description"] = sync_result.get("description", playlist_description)
    playlist_state["privacy_status"] = sync_result.get("privacy_status", "public")
    playlist_state["video_ids"] = ordered_video_ids
    playlist_state["status"] = "completed"
    playlist_state["last_error"] = ""
    playlist_state["last_synced_at"] = dt_module.datetime.now().isoformat()
    state["last_stage"] = "playlist_completed"
    state["last_error"] = ""

    playlist_item_map = sync_result.get("playlist_item_map", {}) if isinstance(sync_result, dict) else {}
    for part in state.get("parts", []):
        video_id = str(part.get("video_id") or "").strip()
        if video_id and video_id in playlist_item_map:
            part["playlist_item_id"] = playlist_item_map[video_id]

    save_split_processing_state(book_record, state)
    result.playlist_id = playlist_state["playlist_id"]
    result.playlist_url = playlist_state.get("playlist_url", "")
    result.playlist_title = playlist_state.get("title", "")
    result.playlist_required = True
    result.playlist_completed = True
    result.pending_resume = False
    result.error = ""
    return result


def build_youtube_payload(
    result,
    book_name,
    category,
    youtube_chapters="",
    title_prefix="",
    part_hint="",
    include_youtube_chapters=True,
    include_part_hint=True,
):
    final_title = result.seo_title or book_name
    final_tags = result.seo_tags or category
    final_desc = result.seo_description or ""

    if part_hint and include_part_hint:
        final_desc = f"{part_hint}\n\n{final_desc}".strip()

    if youtube_chapters and include_youtube_chapters:
        final_desc += "\n\n精彩章节时间轴:\n" + youtube_chapters

    if APPEND_TAGS_TO_DESC and final_tags:
        final_desc += "\n\n" + final_tags

    if APPEND_TAGS_TO_TITLE and final_tags:
        some_tags = " ".join([t for t in final_tags.split() if t.startswith("#")][:2])
        if some_tags and len(final_title) + len(some_tags) < 95:
            final_title += " " + some_tags

    if title_prefix:
        final_title = f"{title_prefix}{final_title}"

    return final_title[:100], final_desc[:5000], final_tags


def download_chapter_items(chapter_items, chapters_dir):
    if not chapter_items:
        return []

    os.makedirs(chapters_dir, exist_ok=True)
    stuck_log_interval = max(10, int(globals().get("AUDIO_DOWNLOAD_STUCK_LOG_INTERVAL_SECONDS", 30) or 30))

    def dl_one(item):
        mp3_url = item["chapter"].get("mp3Url", "")
        title = item.get("title") or f"chapter_{item['source_index']:04d}"
        if not mp3_url:
            return {
                "source_index": item["source_index"],
                "title": title,
                "path": None,
                "attempts": 0,
                "elapsed_seconds": 0.0,
                "error": "章节缺少 mp3Url",
            }

        path = os.path.join(chapters_dir, f"{item['source_index']:04d}_{sanitize_filename(title)}.mp3")
        result = download_audio_file(mp3_url, path, timeout_seconds=300)
        time.sleep(REQUEST_DELAY)
        return {
            "source_index": item["source_index"],
            "title": title,
            "path": path if result["ok"] else None,
            "attempts": result["attempts"],
            "elapsed_seconds": result["elapsed_seconds"],
            "error": result["error"],
        }

    paths_map = {}
    failures = {}
    total = len(chapter_items)
    with concurrent.futures.ThreadPoolExecutor(max_workers=DOWNLOAD_WORKERS) as exe:
        futures = {
            exe.submit(dl_one, item): {
                "source_index": item["source_index"],
                "title": item.get("title") or f"chapter_{item['source_index']:04d}",
                "submitted_at": time.time(),
            }
            for item in chapter_items
        }
        pending = set(futures.keys())
        with tqdm(total=total, desc="并发下载分片章节", unit="章") as progress:
            while pending:
                done, pending = concurrent.futures.wait(
                    pending,
                    timeout=stuck_log_interval,
                    return_when=concurrent.futures.FIRST_COMPLETED,
                )

                if done:
                    for future in done:
                        result = future.result()
                        idx = result["source_index"]
                        paths_map[idx] = result["path"]
                        if not result["path"]:
                            failures[idx] = result
                        progress.update(1)
                    continue

                pending_samples = []
                now = time.time()
                for future in sorted(pending, key=lambda item: futures[item]["source_index"])[:5]:
                    meta = futures[future]
                    pending_samples.append(
                        f"{meta['source_index']:04d}_{sanitize_filename(meta['title'])}({int(now - meta['submitted_at'])}s)"
                    )

                log.warning(
                    "并发下载仍在等待 %d/%d 个章节完成，可能有线程正在长时间重试或网络静默。当前等待中: %s",
                    len(pending),
                    total,
                    " | ".join(pending_samples) if pending_samples else "无",
                )

    ordered_indexes = [item["source_index"] for item in chapter_items]
    chapter_paths = [paths_map[idx] for idx in ordered_indexes if paths_map.get(idx)]
    if len(chapter_paths) != len(ordered_indexes):
        missing_details = []
        for idx in ordered_indexes:
            if paths_map.get(idx):
                continue
            failed = failures.get(idx)
            if failed:
                missing_details.append(
                    f"{idx:04d}_{sanitize_filename(failed['title'])}"
                    f"(重试{failed['attempts']}次, 耗时{int(failed['elapsed_seconds'])}s, {failed['error']})"
                )
            else:
                missing_details.append(f"{idx:04d}_未知章节(未返回结果)")
        raise RuntimeError(f"章节下载不完整，失败章节: {'; '.join(missing_details)}")

    return chapter_paths


def build_final_audio_from_chapter_paths(chapter_paths, working_dir, merged_path, mixed_path, book_name):
    if ENABLE_BGM_MIX and MUSIC_DIR.strip() and os.path.exists(MUSIC_DIR.strip()):
        mixed_dir = os.path.join(working_dir, "mixed_chapters")
        os.makedirs(mixed_dir, exist_ok=True)
        mixed_chapters = []

        for i, ch_path in enumerate(chapter_paths, start=1):
            mixed_basename = os.path.splitext(os.path.basename(ch_path))[0] + "_mixed.mp3"
            ch_mixed = os.path.join(mixed_dir, mixed_basename)
            if os.path.exists(ch_mixed) and os.path.getsize(ch_mixed) > 0:
                mixed_chapters.append(ch_mixed)
                continue

            log.info("[%s] 混音章节 %d/%d -> %s", book_name, i, len(chapter_paths), os.path.basename(ch_path))
            ok_mix = mix_with_bgm(
                ch_path,
                ch_mixed,
                MUSIC_DIR.strip(),
                volume_offset_db=VOLUME_OFFSET_DB,
                highpass_freq=HIGHPASS_FREQ,
                fade_duration_ms=FADE_DURATION_MS,
                min_volume_db=MIN_VOLUME_DB,
                dyn_vol=ENABLE_DYNAMIC_VOLUME,
                spec_shape=ENABLE_SPECTRAL_SHAPING,
                stereo_offset=STEREO_OFFSET,
            )
            if not ok_mix:
                raise RuntimeError(f"BGM 混音失败: {os.path.basename(ch_path)}")
            mixed_chapters.append(ch_mixed)

        if not merge_audio_ffmpeg(mixed_chapters, mixed_path):
            raise RuntimeError("长音频分片混音合并失败")

        for temp_path in mixed_chapters:
            try:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
            except Exception as cleanup_error:
                log.warning("清理临时混音文件失败: %s", cleanup_error)

        return {
            "audio_path": mixed_path,
            "mixed_audio_path": mixed_path,
        }

    if not merge_audio_ffmpeg(chapter_paths, merged_path):
        raise RuntimeError("章节音频合并失败")

    return {
        "audio_path": merged_path,
        "mixed_audio_path": "",
    }


def process_standard_book(result, book_record, book_data, chapters_sorted, book_dir, safe_name, book_name, category):
    merged_path = os.path.join(book_dir, f"{safe_name}.mp3")
    mixed_path = os.path.join(book_dir, f"{safe_name}_mixed.mp3")
    final_path = mixed_path if ENABLE_BGM_MIX else merged_path

    reuse_existing_audio = SKIP_EXISTING and os.path.exists(final_path) and os.path.getsize(final_path) > 0
    if reuse_existing_audio:
        log.info("[%s] 复用现成音频: %s", book_name, os.path.basename(final_path))
        result.merged_audio_path = final_path
        if ENABLE_BGM_MIX:
            result.mixed_audio_path = final_path
        result.audio_ready = True

    if not chapters_sorted:
        if reuse_existing_audio:
            log.warning("[%s] chapters_data 为空，跳过章节下载，仅复用已有音频。", book_name)
        else:
            result.error = "chapters_data 为空或无效，且不存在可复用的成品音频"
        return result

    result.chapter_count = len(chapters_sorted)
    result.youtube_chapters = generate_youtube_timestamps(chapters_sorted)
    result.estimated_total_duration_seconds = sum(estimate_chapter_duration_seconds(ch) for ch in chapters_sorted)

    if not reuse_existing_audio:
        chapter_items = [
            {
                "source_index": idx,
                "chapter": chapter,
                "title": chapter.get("title", f"chapter_{idx:04d}"),
            }
            for idx, chapter in enumerate(chapters_sorted, start=1)
        ]
        chapter_paths = download_chapter_items(chapter_items, os.path.join(book_dir, "chapters"))
        result.success_count = len(chapter_paths)

        if result.success_count == 0:
            result.error = "所有章节下载失败"
            return result

        if ENABLE_DEEPFILTER:
            denoised_dir = os.path.join(book_dir, "denoised_chapters")
            denoised_targets = [os.path.join(denoised_dir, os.path.basename(path)) for path in chapter_paths]
            try:
                chapter_paths = denoise_audio_paths_parallel(
                    chapter_paths,
                    output_paths=denoised_targets,
                    max_workers=DEEPFILTER_WORKERS,
                )
            except Exception as e:
                result.error = f"DeepFilter 降噪失败: {e}"
                return result

        result.chapter_audio_paths = chapter_paths
        result.youtube_chapters = generate_youtube_timestamps(chapters_sorted, chapter_paths)

        try:
            audio_info = build_final_audio_from_chapter_paths(
                chapter_paths,
                book_dir,
                merged_path,
                mixed_path,
                book_name,
            )
        except Exception as e:
            result.error = str(e)
            return result

        result.merged_audio_path = audio_info["audio_path"]
        result.mixed_audio_path = audio_info["mixed_audio_path"]
        result.audio_ready = True
    else:
        result.success_count = result.chapter_count

    prepare_standard_book_cover_and_seo_with_state(
        result,
        book_record,
        book_data,
        book_dir,
        safe_name,
        book_name,
    )

    if ENABLE_VIDEO_GENERATION:
        video_path = os.path.join(book_dir, f"{safe_name}_final.mp4")
        if SKIP_EXISTING and os.path.exists(video_path) and os.path.getsize(video_path) > 0:
            result.video_path = video_path
            result.video_ready = True
            log.info("[%s] 复用已封装的 MP4 成品。", book_name)
        elif result.merged_audio_path and result.cover_image_path:
            try:
                ok_vid = generate_video(result.merged_audio_path, result.cover_image_path, video_path, VIDEO_RESOLUTION)
                if ok_vid:
                    result.video_path = video_path
                    result.video_ready = True
                else:
                    log.warning("[%s] MP4 封装失败，本次仅保留音频成品。", book_name)
            except Exception as e:
                log.error("[%s] MP4 封装发生异常: %s", book_name, e)
        else:
            log.warning("[%s] 缺少音频或封面，跳过 MP4 封装。", book_name)

    if ENABLE_YOUTUBE_UPLOAD and YOUTUBE_CHANNEL_NAME.strip():
        if result.video_path and os.path.exists(result.video_path):
            try:
                upload_receipt_path = os.path.join(book_dir, "youtube_upload_receipt.json")
                final_title, final_desc, final_tags = build_youtube_payload(
                    result,
                    book_name,
                    category,
                    youtube_chapters=result.youtube_chapters,
                )
                upload_result = {}
                if not FORCE_REPROCESS:
                    upload_result = load_youtube_upload_receipt(
                        upload_receipt_path,
                        video_path=result.video_path,
                        channel_name=str(YOUTUBE_CHANNEL_NAME).strip(),
                    )
                if upload_result:
                    log.info("[%s] 复用本地 YouTube 上传回执，跳过重复上传。", book_name)
                else:
                    upload_result = upload_to_youtube_detailed(
                        video_path=result.video_path,
                        title=final_title,
                        description=final_desc,
                        tags=final_tags,
                        cover_path=result.cover_image_path,
                        channel_name=str(YOUTUBE_CHANNEL_NAME).strip(),
                        privacy_status=str(YOUTUBE_PRIVACY_STATUS).strip(),
                        category_id=str(YOUTUBE_CATEGORY_ID).strip(),
                        schedule_after_hours=YOUTUBE_SCHEDULE_AFTER_HOURS,
                    )
                    if upload_result:
                        persist_youtube_upload_receipt(
                            upload_receipt_path,
                            video_path=result.video_path,
                            upload_result=upload_result,
                            channel_name=str(YOUTUBE_CHANNEL_NAME).strip(),
                            title=final_title,
                            privacy_status=str(YOUTUBE_PRIVACY_STATUS).strip(),
                            category_id=str(YOUTUBE_CATEGORY_ID).strip(),
                            schedule_after_hours=YOUTUBE_SCHEDULE_AFTER_HOURS,
                        )
                if upload_result:
                    result.youtube_url = upload_result.get("youtube_url", "")
                    result.youtube_urls = [result.youtube_url] if result.youtube_url else []
                    result.youtube_publish_at = upload_result.get("publish_at", "")
                    result.youtube_schedule_reason = upload_result.get("schedule_reason", "")
                    result.upload_ready = bool(result.youtube_url)
            except Exception as e:
                log.error("[%s] YouTube 上传异常: %s", book_name, e)
        else:
            log.warning("[%s] 缺少可上传的 MP4，跳过 YouTube 上传。", book_name)

    return result


def build_part_result_record(part_plan, part_state):
    return {
        "part_index": part_plan["part_index"],
        "chapter_start_index": part_plan["chapter_start_index"],
        "chapter_end_index": part_plan["chapter_end_index"],
        "chapter_count": len(part_plan.get("items", [])),
        "estimated_duration_seconds": part_plan.get("estimated_duration_seconds", 0),
        "actual_duration_seconds": part_state.get("actual_duration_seconds", 0),
        "audio_path": part_state.get("audio_path", ""),
        "video_path": part_state.get("video_path", ""),
        "video_id": part_state.get("video_id", ""),
        "uploaded_at": part_state.get("uploaded_at", ""),
        "publish_at": part_state.get("publish_at", ""),
        "schedule_reason": part_state.get("schedule_reason", ""),
        "youtube_url": part_state.get("youtube_url", ""),
        "youtube_title": part_state.get("youtube_title", ""),
        "playlist_item_id": part_state.get("playlist_item_id", ""),
        "status": part_state.get("status", "pending"),
        "error": part_state.get("error", ""),
    }


def sync_result_from_split_state(result, state, split_plan):
    result.part_count = len(split_plan.get("parts", [])) or 1
    result.part_results = []
    result.youtube_urls = []
    result.youtube_publish_at = ""
    result.youtube_schedule_reason = ""
    result.success_count = 0
    result.chapter_audio_paths = []
    result.youtube_chapters = ""
    playlist_state = get_split_playlist_state(state)
    result.playlist_id = str(playlist_state.get("playlist_id") or "")
    result.playlist_url = str(playlist_state.get("playlist_url") or "")
    result.playlist_title = str(playlist_state.get("title") or "")
    progress = evaluate_split_completion_state(state)
    playlist_required = progress["playlist_required"]
    playlist_completed = progress["playlist_completed"]

    latest_audio_path = ""
    latest_video_path = ""
    latest_youtube_chapters = ""
    latest_publish_at = ""
    latest_schedule_reason = ""
    all_timestamps = []
    completed_part_count = 0

    for part_plan in split_plan.get("parts", []):
        part_state = get_split_part_state(state, part_plan["part_index"]) or {}
        _reconcile_split_part_state(part_state)
        result.part_results.append(build_part_result_record(part_plan, part_state))

        if _split_part_is_completed(part_state):
            completed_part_count += 1
            result.success_count += len(part_plan.get("items", []))

        if part_state.get("youtube_url"):
            result.youtube_urls.append(part_state["youtube_url"])

        if part_state.get("audio_path"):
            latest_audio_path = part_state["audio_path"]
        if part_state.get("video_path"):
            latest_video_path = part_state["video_path"]
        if part_state.get("publish_at"):
            latest_publish_at = str(part_state.get("publish_at") or "")
        if part_state.get("schedule_reason"):
            latest_schedule_reason = str(part_state.get("schedule_reason") or "")

        if part_state.get("youtube_title"):
            all_timestamps.append(f"{part_state['youtube_title']}: {part_state.get('youtube_url', '')}".strip())

        if part_state.get("youtube_chapters"):
            latest_youtube_chapters = part_state["youtube_chapters"]

    result.merged_audio_path = latest_audio_path
    result.video_path = latest_video_path
    result.completed_part_count = progress["completed_part_count"]
    result.playlist_required = playlist_required
    result.playlist_completed = playlist_completed
    result.pending_resume = not progress["fully_completed"]
    result.youtube_url = "\n".join(result.youtube_urls)
    result.youtube_publish_at = latest_publish_at
    result.youtube_schedule_reason = latest_schedule_reason
    result.youtube_chapters = latest_youtube_chapters or "\n".join([item for item in all_timestamps if item])
    return result


def cleanup_completed_split_state_for_book(book_record, result, book_name):
    try:
        if delete_split_processing_state(book_record, only_if_completed=False):
            result.state_path = ""
            log.info("[%s] Split upload state deleted.", book_name)
    except Exception as e:
        log.warning("[%s] Failed to delete split upload state: %s", book_name, e)
    return result


def process_split_part(result, state, state_ref, split_plan, part_plan, book_record, book_dir, safe_name, book_name, category):
    part_index = part_plan["part_index"]
    part_count = len(split_plan.get("parts", []))
    part_state = get_split_part_state(state, part_index)
    part_dir = os.path.join(book_dir, "_split_parts", f"part_{part_index:02d}")
    upload_receipt_path = os.path.join(part_dir, "youtube_upload_receipt.json")
    expected_video_path = os.path.join(part_dir, f"{safe_name}_part_{part_index:02d}_final.mp4")
    os.makedirs(part_dir, exist_ok=True)

    if part_state is None:
        raise RuntimeError(f"未找到分片状态定义: part {part_index}")

    if ENABLE_VIDEO_GENERATION and _is_nonempty_local_file(expected_video_path):
        part_state["video_path"] = part_state.get("video_path") or expected_video_path

    if ENABLE_YOUTUBE_UPLOAD and str(YOUTUBE_CHANNEL_NAME or "").strip():
        reused_upload_result = {}
        if not FORCE_REPROCESS:
            reused_upload_result = load_youtube_upload_receipt(
                upload_receipt_path,
                video_path=part_state.get("video_path") or expected_video_path,
                channel_name=str(YOUTUBE_CHANNEL_NAME).strip(),
            )
        if reused_upload_result:
            part_state["video_id"] = str(part_state.get("video_id") or reused_upload_result.get("video_id") or "")
            part_state["youtube_url"] = str(part_state.get("youtube_url") or reused_upload_result.get("youtube_url") or "")
            part_state["uploaded_at"] = str(part_state.get("uploaded_at") or reused_upload_result.get("uploaded_at") or "")
            part_state["publish_at"] = str(part_state.get("publish_at") or reused_upload_result.get("publish_at") or "")
            part_state["schedule_reason"] = str(part_state.get("schedule_reason") or reused_upload_result.get("schedule_reason") or "")
            part_state["youtube_title"] = str(part_state.get("youtube_title") or reused_upload_result.get("title") or "")

    if _reconcile_split_part_state(part_state):
        state["last_error"] = ""
        state_ref = save_split_processing_state(book_record, state)
        result.state_path = state_ref

    if part_state.get("status") == "completed":
        log.info("[%s] 分片 %d/%d 已完成，跳过重做。", book_name, part_index, part_count)
        return build_part_result_record(part_plan, part_state)

    precomputed_upload_title = ""
    precomputed_upload_desc = ""
    precomputed_upload_tags = ""
    if ENABLE_YOUTUBE_UPLOAD and YOUTUBE_CHANNEL_NAME.strip():
        precomputed_upload_title, precomputed_upload_desc, precomputed_upload_tags = build_youtube_payload(
            result,
            book_name,
            category,
            youtube_chapters="",
            title_prefix=f"{part_index}-" if part_count > 1 else "",
            part_hint="",
            include_youtube_chapters=False,
            include_part_hint=False,
        )
        if not FORCE_REPROCESS:
            existing_channel_match = find_existing_channel_video_by_exact_title(
                str(YOUTUBE_CHANNEL_NAME).strip(),
                precomputed_upload_title,
            )
            if existing_channel_match:
                part_state["youtube_title"] = str(existing_channel_match.get("title") or precomputed_upload_title or "")
                part_state["youtube_url"] = str(existing_channel_match.get("youtube_url") or "")
                part_state["video_id"] = str(existing_channel_match.get("video_id") or "")
                part_state["uploaded_at"] = str(existing_channel_match.get("uploaded_at") or "")
                part_state["publish_at"] = str(existing_channel_match.get("publish_at") or "")
                part_state["schedule_reason"] = str(existing_channel_match.get("schedule_reason") or "existing_title_match")
                part_state["last_stage"] = "existing_title_match"
                state["last_stage"] = f"part_{part_index}_existing_title_match"
                state["last_error"] = ""
                state_ref = save_split_processing_state(book_record, state)
                result.state_path = state_ref
                result.youtube_publish_at = part_state["publish_at"]
                result.youtube_schedule_reason = part_state["schedule_reason"]
                log.info("[%s] 分片 %d/%d 命中频道内同标题视频，直接复用并跳过重复处理。", book_name, part_index, part_count)
                return build_part_result_record(part_plan, part_state)

    chapter_items = part_plan.get("items", [])
    chapters_only = [item["chapter"] for item in chapter_items]
    chapters_dir = os.path.join(part_dir, "chapters")
    denoised_dir = os.path.join(part_dir, "denoised")

    part_state["status"] = "in_progress"
    part_state["started_at"] = part_state.get("started_at") or dt_module.datetime.now().isoformat()
    part_state["last_stage"] = "download"
    part_state["error"] = ""
    state["last_stage"] = f"part_{part_index}_download"
    state["last_error"] = ""
    state["pending_resume"] = True
    state_ref = save_split_processing_state(book_record, state)
    result.state_path = state_ref

    try:
        chapter_paths = download_chapter_items(chapter_items, chapters_dir)

        if ENABLE_DEEPFILTER:
            part_state["last_stage"] = "denoise"
            state["last_stage"] = f"part_{part_index}_denoise"
            state_ref = save_split_processing_state(book_record, state)
            result.state_path = state_ref

            denoised_targets = [os.path.join(denoised_dir, os.path.basename(path)) for path in chapter_paths]
            chapter_paths = denoise_audio_paths_parallel(
                chapter_paths,
                output_paths=denoised_targets,
                max_workers=DEEPFILTER_WORKERS,
            )

        youtube_chapters = generate_youtube_timestamps(chapters_only, chapter_paths)
        merged_path = os.path.join(part_dir, f"{safe_name}_part_{part_index:02d}.mp3")
        mixed_path = os.path.join(part_dir, f"{safe_name}_part_{part_index:02d}_mixed.mp3")

        part_state["last_stage"] = "merge_audio"
        state["last_stage"] = f"part_{part_index}_merge_audio"
        state_ref = save_split_processing_state(book_record, state)
        result.state_path = state_ref

        audio_info = build_final_audio_from_chapter_paths(
            chapter_paths,
            part_dir,
            merged_path,
            mixed_path,
            f"{book_name} [part {part_index}]",
        )
        audio_path = audio_info["audio_path"]
        actual_duration_seconds = probe_audio_duration_seconds(audio_path) or part_plan.get("estimated_duration_seconds", 0)

        part_state["audio_path"] = audio_path
        part_state["youtube_chapters"] = youtube_chapters
        part_state["actual_duration_seconds"] = actual_duration_seconds

        video_path = ""
        if ENABLE_VIDEO_GENERATION:
            part_state["last_stage"] = "generate_video"
            state["last_stage"] = f"part_{part_index}_generate_video"
            state_ref = save_split_processing_state(book_record, state)
            result.state_path = state_ref

            video_path = expected_video_path
            if SKIP_EXISTING and os.path.exists(video_path) and os.path.getsize(video_path) > 0:
                log.info("[%s] 分片 %d/%d 复用现有 MP4。", book_name, part_index, part_count)
            else:
                if not result.cover_image_path:
                    raise RuntimeError("缺少封面，无法为分片封装视频")
                ok_vid = generate_video(audio_path, result.cover_image_path, video_path, VIDEO_RESOLUTION)
                if not ok_vid:
                    raise RuntimeError("分片 MP4 封装失败")
            part_state["video_path"] = video_path

        if ENABLE_YOUTUBE_UPLOAD and YOUTUBE_CHANNEL_NAME.strip():
            part_state["last_stage"] = "upload_youtube"
            state["last_stage"] = f"part_{part_index}_upload_youtube"
            state_ref = save_split_processing_state(book_record, state)
            result.state_path = state_ref

            if not part_state.get("video_path") or not os.path.exists(part_state["video_path"]):
                raise RuntimeError("缺少可上传的视频分片")

            final_title = precomputed_upload_title
            final_desc = precomputed_upload_desc
            final_tags = precomputed_upload_tags
            if not final_title:
                final_title, final_desc, final_tags = build_youtube_payload(
                    result,
                    book_name,
                    category,
                    youtube_chapters="",
                    title_prefix=f"{part_index}-" if part_count > 1 else "",
                    part_hint="",
                    include_youtube_chapters=False,
                    include_part_hint=False,
                )
            upload_result = {}
            if not FORCE_REPROCESS:
                upload_result = load_youtube_upload_receipt(
                    upload_receipt_path,
                    video_path=part_state["video_path"],
                    channel_name=str(YOUTUBE_CHANNEL_NAME).strip(),
                )
            if upload_result:
                log.info("[%s] Split part %d/%d reuses a saved YouTube upload receipt.", book_name, part_index, part_count)
            else:
                upload_result = upload_to_youtube_detailed(
                    video_path=part_state["video_path"],
                    title=final_title,
                    description=final_desc,
                    tags=final_tags,
                    cover_path=result.cover_image_path,
                    channel_name=str(YOUTUBE_CHANNEL_NAME).strip(),
                    privacy_status=str(YOUTUBE_PRIVACY_STATUS).strip(),
                    category_id=str(YOUTUBE_CATEGORY_ID).strip(),
                    schedule_after_hours=YOUTUBE_SCHEDULE_AFTER_HOURS,
                )
            if not upload_result:
                raise RuntimeError("YouTube upload did not complete")

            persist_youtube_upload_receipt(
                upload_receipt_path,
                video_path=part_state["video_path"],
                upload_result=upload_result,
                channel_name=str(YOUTUBE_CHANNEL_NAME).strip(),
                title=final_title,
                privacy_status=str(YOUTUBE_PRIVACY_STATUS).strip(),
                category_id=str(YOUTUBE_CATEGORY_ID).strip(),
                schedule_after_hours=YOUTUBE_SCHEDULE_AFTER_HOURS,
            )

            part_state["youtube_title"] = final_title
            part_state["youtube_url"] = upload_result.get("youtube_url", "")
            part_state["video_id"] = upload_result.get("video_id", "")
            part_state["uploaded_at"] = upload_result.get("uploaded_at", "")
            part_state["publish_at"] = upload_result.get("publish_at", "")
            part_state["schedule_reason"] = upload_result.get("schedule_reason", "")
            result.youtube_publish_at = part_state["publish_at"]
            result.youtube_schedule_reason = part_state["schedule_reason"]
            part_state["last_stage"] = "upload_persisted"
            state["last_stage"] = f"part_{part_index}_upload_persisted"
            state["last_error"] = ""
            state_ref = save_split_processing_state(book_record, state)
            result.state_path = state_ref

        part_state["status"] = "completed"
        part_state["completed_at"] = dt_module.datetime.now().isoformat()
        part_state["last_stage"] = "completed"
        part_state["error"] = ""
        state["last_stage"] = f"part_{part_index}_completed"
        state["last_error"] = ""
        state_ref = save_split_processing_state(book_record, state)
        result.state_path = state_ref

        return build_part_result_record(part_plan, part_state)
    except Exception as e:
        part_state["status"] = "failed"
        part_state["error"] = str(e)
        state["last_stage"] = f"part_{part_index}_failed"
        state["last_error"] = str(e)
        state_ref = save_split_processing_state(book_record, state)
        result.state_path = state_ref
        raise


def process_split_book(result, book_record, book_data, chapters_sorted, book_dir, safe_name, book_name, category, run_started_at=None):
    split_plan = build_split_part_plans(chapters_sorted)
    result.split_mode = True
    result.part_count = len(split_plan.get("parts", [])) or 1
    result.chapter_count = len(chapters_sorted)
    result.estimated_total_duration_seconds = split_plan.get("estimated_total_seconds", 0)
    result.success_count = 0

    state_ref, state = initialize_split_processing_state(book_record, book_dir, chapters_sorted, split_plan)
    result.state_path = state_ref

    log.info(
        "[%s] 预估总时长 %s，触发长音频分片模式，计划拆成 %d 个视频上传。",
        book_name,
        format_seconds_hhmmss(result.estimated_total_duration_seconds),
        result.part_count,
    )

    restore_split_shared_assets_from_state(result, state, book_dir, safe_name, book_name)
    prepare_book_cover_and_seo(result, book_data, book_dir, safe_name, book_name)
    state_ref = persist_split_shared_assets_to_state(book_record, state, result, book_dir, safe_name, book_name)
    result.state_path = state_ref

    reconcile_summary = reconcile_split_part_upload_states(result, state, split_plan, book_name, category)
    if reconcile_summary.get("changed"):
        recovered_parts = [str(item[0]) for item in reconcile_summary.get("recovered", [])]
        reset_parts = [str(item[0]) for item in reconcile_summary.get("reset", [])]
        if reset_parts:
            playlist_state = get_split_playlist_state(state)
            playlist_state["status"] = "pending"
            playlist_state["last_error"] = "Waiting for split parts to be re-uploaded after stale video recovery."
            playlist_state["video_ids"] = []
        state["last_stage"] = "resume_reconciled"
        if reset_parts:
            state["last_error"] = "Reset stale uploaded YouTube references for split parts: " + ",".join(reset_parts)
        else:
            state["last_error"] = ""
        state_ref = save_split_processing_state(book_record, state)
        result.state_path = state_ref
        log.info(
            "[%s] Resume reconciliation finished. recovered_parts=%s reset_parts=%s state=%s",
            book_name,
            ",".join(recovered_parts) if recovered_parts else "<none>",
            ",".join(reset_parts) if reset_parts else "<none>",
            state_ref,
        )

    playlist_required = bool(result.part_count > 1 and ENABLE_YOUTUBE_UPLOAD and str(YOUTUBE_CHANNEL_NAME).strip())
    playlist_state = get_split_playlist_state(state)
    playlist_completed = bool(playlist_state.get("playlist_id")) and str(playlist_state.get("status") or "").strip().lower() == "completed"

    if state.get("status") == "completed" and (not playlist_required or playlist_completed):
        sync_result_from_split_state(result, state, split_plan)
        result.pending_resume = False
        return result
    elif state.get("status") == "completed" and playlist_required and not playlist_completed:
        log.info("[%s] 检测到分片上传已完成但播放列表尚未补齐，将继续恢复 playlist。", book_name)

    for part_plan in split_plan.get("parts", []):
        part_state = get_split_part_state(state, part_plan["part_index"]) or {}
        if part_state.get("status") == "completed":
            continue

        if run_started_at is not None:
            should_stop, remaining_seconds = should_stop_before_next_book(run_started_at)
            if should_stop:
                state["pending_resume"] = True
                state["last_stage"] = f"waiting_before_part_{part_plan['part_index']}"
                state["last_error"] = (
                    f"触发 Colab 时长保护，暂停在分片 {part_plan['part_index']}/{result.part_count} 前，"
                    f"预计剩余 {max(0, int(remaining_seconds or 0))} 秒。"
                )
                state_ref = save_split_processing_state(book_record, state)
                result.state_path = state_ref
                result.pending_resume = True
                result.stop_requested = True
                result.error = state["last_error"]
                break

        try:
            process_split_part(
                result,
                state,
                state_ref,
                split_plan,
                part_plan,
                book_record,
                book_dir,
                safe_name,
                book_name,
                category,
            )
        except Exception as e:
            result.error = str(e)
            break

    state = reload_split_processing_state(book_record, fallback_state=state, book_name=book_name)
    sync_result_from_split_state(result, state, split_plan)

    if result.completed_part_count >= result.part_count:
        if playlist_required:
            try:
                playlist_state = get_split_playlist_state(state)
                playlist_state["status"] = "syncing"
                state["pending_resume"] = True
                state["last_stage"] = "playlist_pending"
                state_ref = save_split_processing_state(book_record, state)
                result.state_path = state_ref

                sync_split_playlist(result, state, split_plan, book_record, book_name)
                state = reload_split_processing_state(book_record, fallback_state=state, book_name=book_name)
                sync_result_from_split_state(result, state, split_plan)
                if not bool(getattr(result, "playlist_completed", False)):
                    playlist_state = get_split_playlist_state(state)
                    incomplete_error = (
                        "Playlist sync returned without completion: "
                        f"playlist_id={str(playlist_state.get('playlist_id') or '')} "
                        f"playlist_status={str(playlist_state.get('status') or '')} "
                        f"playlist_url={str(playlist_state.get('playlist_url') or '')} "
                        f"ordered_video_ids={[item.get('video_id') for item in build_ordered_split_video_records(state, split_plan)]}"
                    )
                    playlist_state["status"] = "failed"
                    playlist_state["last_error"] = incomplete_error
                    state["pending_resume"] = True
                    state["last_stage"] = "playlist_failed"
                    state["last_error"] = incomplete_error
                    state_ref = save_split_processing_state(book_record, state)
                    result.state_path = state_ref
                    result.pending_resume = True
                    result.error = incomplete_error
                    return result
            except Exception as e:
                playlist_state = get_split_playlist_state(state)
                playlist_state["status"] = "failed"
                playlist_state["last_error"] = str(e)
                state["pending_resume"] = True
                state["last_stage"] = "playlist_failed"
                state["last_error"] = str(e)
                state_ref = save_split_processing_state(book_record, state)
                result.state_path = state_ref
                result.pending_resume = True
                result.error = str(e)
                return result

        state["pending_resume"] = False
        state["last_error"] = ""
        state["last_stage"] = "all_parts_completed"
        state_ref = save_split_processing_state(book_record, state)
        result.state_path = state_ref
        state = reload_split_processing_state(book_record, fallback_state=state, book_name=book_name)
        sync_result_from_split_state(result, state, split_plan)
        result.pending_resume = not (
            result.completed_part_count >= result.part_count
            and (not playlist_required or bool(getattr(result, "playlist_completed", False)))
        )
        if not result.pending_resume:
            result.error = ""
        elif not result.error:
            playlist_state = get_split_playlist_state(state)
            result.error = (
                "Split book reached final checkpoint but is still incomplete: "
                f"playlist_id={str(playlist_state.get('playlist_id') or '')} "
                f"playlist_status={str(playlist_state.get('status') or '')} "
                f"playlist_url={str(playlist_state.get('playlist_url') or '')} "
                f"completed_part_count={result.completed_part_count}/{result.part_count}"
            )
    elif not result.error:
        result.error = "长音频分片处理中断，已记录进度，等待下次续跑"

    return result


def skip_and_delete_short_book(book_record, result, book_name):
    duration_text = format_seconds_hhmmss(getattr(result, "estimated_total_duration_seconds", 0))
    short_reason = (
        f"预估总时长 {duration_text} 小于 {format_seconds_hhmmss(MIN_BOOK_DURATION_SECONDS)}，"
        "已跳过处理并从 books 表删除。"
    )
    try:
        _delete_book_from_database(book_record["book_id"])
        try:
            if delete_split_processing_state(book_record, only_if_completed=False):
                result.state_path = ""
        except Exception as state_error:
            log.warning("[%s] books 记录已删除，但清理 book_processing_states 失败: %s", book_name, state_error)
    except Exception as e:
        result.error = (
            f"预估总时长 {duration_text} 小于 {format_seconds_hhmmss(MIN_BOOK_DURATION_SECONDS)}，"
            f"但删除 books 记录失败: {e}"
        )
        return result

    result.skipped = True
    result.deleted_from_books = True
    result.skipped_reason = short_reason
    result.error = short_reason
    log.info("[%s] %s", book_name, short_reason)
    return result


def process_book(book_record: dict, run_started_at=None) -> BookResult:
    """
    单书处理入口：
    1. 普通长度书籍沿用原有流程。
    2. 预估总时长超过 LONG_AUDIO_SPLIT_TRIGGER_HOURS 时切换到分片模式。
    3. 分片模式只处理当前分片所需章节，并把状态写入数据库的 BOOK_STATE_TABLE。
    """

    book_id = str(book_record["book_id"])
    book_name = book_record.get("book_name") or f"book_{book_id}"
    category = book_record.get("category", "未分类")

    safe_name = sanitize_filename(book_name)
    safe_cat = sanitize_filename(category)
    book_dir = os.path.join(OUTPUT_ROOT, safe_cat, f"{safe_name}_{book_id}")
    os.makedirs(book_dir, exist_ok=True)

    result = BookResult(book_id=book_id, book_name=book_name, category=category)

    def finish():
        return finalize_book_result(result, book_dir, book_record=book_record)

    raw = book_record.get("book_data", {})
    try:
        book_data = json.loads(raw) if isinstance(raw, str) else raw
    except Exception as e:
        result.error = f"book_data JSON 解析失败: {e}"
        return finish()

    if not isinstance(book_data, dict):
        result.error = "book_data 不是有效字典"
        return finish()

    chapters = book_data.get("chapters_data", []) or []
    chapters_sorted = sorted(chapters, key=lambda c: c.get("id", 0))
    result.chapter_count = len(chapters_sorted)
    explicit_total_duration_seconds = get_explicit_total_book_duration_seconds(chapters_sorted)

    if not chapters_sorted:
        final_path = os.path.join(book_dir, f"{safe_name}_mixed.mp3" if ENABLE_BGM_MIX else f"{safe_name}.mp3")
        if SKIP_EXISTING and os.path.exists(final_path) and os.path.getsize(final_path) > 0:
            result.merged_audio_path = final_path
            result.audio_ready = True
            prepare_standard_book_cover_and_seo_with_state(
                result,
                book_record,
                book_data,
                book_dir,
                safe_name,
                book_name,
            )
        else:
            result.error = "chapters_data 为空或无效，且不存在可复用的成品音频"
        return finish()

    split_plan = build_split_part_plans(chapters_sorted)
    result.estimated_total_duration_seconds = split_plan.get("estimated_total_seconds", 0)
    if explicit_total_duration_seconds is not None:
        result.estimated_total_duration_seconds = explicit_total_duration_seconds

    if explicit_total_duration_seconds is not None and 0 < int(explicit_total_duration_seconds or 0) < MIN_BOOK_DURATION_SECONDS:
        skip_and_delete_short_book(book_record, result, book_name)
        return finish()

    if split_plan.get("split_mode"):
        process_split_book(
            result,
            book_record,
            book_data,
            chapters_sorted,
            book_dir,
            safe_name,
            book_name,
            category,
            run_started_at=run_started_at,
        )
    else:
        process_standard_book(
            result,
            book_record,
            book_data,
            chapters_sorted,
            book_dir,
            safe_name,
            book_name,
            category,
        )

    return finish()


def _fetch_books_page_from_database(offset, page_size):
    table_sql = get_public_table_identifier("books")
    statement = sql.SQL(
        """
        SELECT book_id, book_name, category, book_data, status, tags
        FROM {}
        """
    ).format(table_sql)
    params = []
    if TARGET_CATEGORY.strip():
        statement += sql.SQL(" WHERE category = %s")
        params.append(TARGET_CATEGORY.strip())
    statement += sql.SQL(" ORDER BY book_id LIMIT %s OFFSET %s")
    params.extend([page_size, offset])
    return execute_postgres_fetchall(statement, tuple(params))


def _update_book_status_in_database(book_id, status_value):
    table_sql = get_public_table_identifier("books")
    execute_postgres(
        sql.SQL("UPDATE {} SET status = %s WHERE book_id = %s").format(table_sql),
        (status_value, str(book_id)),
    )


def _update_book_tags_in_database(book_id, tags_value):
    table_sql = get_public_table_identifier("books")
    execute_postgres(
        sql.SQL("UPDATE {} SET tags = %s WHERE book_id = %s").format(table_sql),
        (tags_value, str(book_id)),
    )


def _delete_book_from_database(book_id):
    table_sql = get_public_table_identifier("books")
    execute_postgres(
        sql.SQL("DELETE FROM {} WHERE book_id = %s").format(table_sql),
        (str(book_id),),
    )


def finalize_successful_book_for_project(book_record, result, book_name, flag):
    new_status = build_supabase_text_update(book_record.get("status"), [flag] if flag else [], prefer="string")

    try:
        _update_book_status_in_database(book_record["book_id"], new_status)
        book_record["status"] = new_status
        log.info("Completed and marked status='%s'", new_status)
    except Exception as e:
        log.error("[%s] Failed to update books.status: %s", book_name, e)
        result.success = False
        if getattr(result, "split_mode", False):
            result.pending_resume = True
            result.error = f"Split upload finished, but updating books.status failed: {e}"
        else:
            result.error = f"Output finished, but updating books.status failed: {e}"
        return False

    if getattr(result, "split_mode", False) or str(getattr(result, "state_path", "") or "").strip():
        try:
            if delete_split_processing_state(book_record, only_if_completed=False):
                result.state_path = ""
                if getattr(result, "split_mode", False):
                    log.info("[%s] Split upload finalized and book_processing_states deleted.", book_name)
                else:
                    log.info("[%s] Standard upload finalized and book_processing_states deleted.", book_name)
        except Exception as e:
            log.error(
                "[%s] books.status updated, but deleting book_processing_states failed; startup cleanup will retry: %s",
                book_name,
                e,
            )

    return True


def run_pipeline(runtime_config: dict | None = None):
    apply_runtime_config(runtime_config)
    validate_runtime_config()

    execute_postgres_fetchval("SELECT 1 AS ok")
    log.info("PostgreSQL connected")

    applied_cloud_overrides = apply_cloud_runtime_overrides()
    if applied_cloud_overrides:
        log.info("Applied cloud runtime overrides: %s", ", ".join(sorted(applied_cloud_overrides.keys())))

    os.makedirs(OUTPUT_ROOT, exist_ok=True)
    sync_music_library_if_enabled()

    cat_label = TARGET_CATEGORY.strip() or "all"
    log.info("Fetching books... category=%s", cat_label)

    all_books = []
    all_books_by_id = {}
    page_size = 100
    offset = 0

    while True:
        rows = _fetch_books_page_from_database(offset, page_size)
        if not rows:
            break

        for row in rows:
            book_id = str(row.get("book_id") or "").strip()
            if book_id:
                all_books_by_id[book_id] = row

            tags_list = set(normalize_text_items(row.get("tags")))
            if "bad" not in tags_list:
                all_books.append(row)

        if len(rows) < page_size:
            break
        offset += page_size

    flag = PROJECT_FLAG.strip()
    interrupted_states = list_interrupted_book_states(all_books_by_id) if all_books_by_id else {}

    if not FORCE_REPROCESS and flag:
        filtered_books = []
        for book in all_books:
            existing_flags = set(normalize_text_items(book.get("status")))
            if flag not in existing_flags:
                filtered_books.append(book)
        all_books = filtered_books

    log.info("Books remaining after status filter: %d", len(all_books))

    if all_books:
        interrupted_books = [book for book in all_books if str(book.get("book_id")) in interrupted_states]
        fresh_books = [book for book in all_books if str(book.get("book_id")) not in interrupted_states]
        random.shuffle(fresh_books)

        if PRIORITIZE_INTERRUPTED_BOOKS and interrupted_books:
            interrupted_books.sort(
                key=lambda item: interrupted_states[str(item.get("book_id"))].get("updated_at", ""),
                reverse=True,
            )
            all_books = interrupted_books + fresh_books
            log.info("Prioritizing %d interrupted books with saved processing state.", len(interrupted_books))
        else:
            all_books = fresh_books + interrupted_books
            random.shuffle(all_books)
            log.info("Shuffled processing order for %d books.", len(all_books))

    try:
        success_target_count = max(0, int(MAX_PROCESS_COUNT or 0))
    except Exception:
        success_target_count = 0

    if success_target_count > 0:
        log.info("This run will stop after %d successful uploads.", success_target_count)

    if not all_books:
        runtime_console_print("No books to process.", level="INFO")
        return {
            "success": True,
            "results": [],
            "summary_path": "",
            "stop_reason": "",
            "successful_upload_count": 0,
        }

    all_results = []
    run_started_at = time.time()
    stop_reason = ""
    successful_upload_count = 0

    def counts_towards_max_process(result):
        if not getattr(result, "success", False):
            return False
        if ENABLE_YOUTUBE_UPLOAD:
            return bool(getattr(result, "upload_ready", False))
        return True

    for i, book in enumerate(all_books, start=1):
        if success_target_count > 0 and successful_upload_count >= success_target_count:
            stop_reason = f"Reached upload target for this run: {success_target_count}"
            log.info(stop_reason)
            break

        should_stop, remaining_seconds = should_stop_before_next_book(run_started_at)
        if should_stop:
            stop_reason = f"Colab runtime guard triggered with about {max(0, int(remaining_seconds))} seconds remaining"
            log.warning(stop_reason)
            break

        if i > 1:
            clear_runtime_output_if_needed()

        name = book.get("book_name", "unknown")
        cat = book.get("category", "uncategorized")
        runtime_console_print(f"\n{'=' * 50}", level="INFO")
        log.info("[%d/%d] Book: %s | %s", i, len(all_books), name, cat)

        should_break_after_summary = False
        try:
            result = process_book(book, run_started_at=run_started_at)
        except MissingYouTubeCredentialsError as e:
            stop_reason = f"YouTube credential initialization failed: {e}"
            log.error("[%s] %s", name, stop_reason)
            result = BookResult(book_id=str(book.get("book_id", "")), book_name=name, category=cat, error=str(e))
            should_break_after_summary = True
        except Exception as e:
            log.error("[%s] Uncaught exception while processing book: %s", name, e)
            result = BookResult(book_id=str(book.get("book_id", "")), book_name=name, category=cat, error=f"Uncaught exception: {e}")

        all_results.append(result)

        if result.success:
            finalize_successful_book_for_project(book, result, name, flag)

        if result.success:
            if counts_towards_max_process(result):
                successful_upload_count += 1
                if success_target_count > 0:
                    log.info("Upload counter progress: %d/%d", successful_upload_count, success_target_count)

            log.info(
                "chapters=%d merged=%s mixed=%s",
                result.success_count,
                os.path.basename(result.merged_audio_path) if result.merged_audio_path else "none",
                os.path.basename(result.mixed_audio_path) if result.mixed_audio_path else "none",
            )
        elif result.skipped:
            log.info("Skipped: %s", getattr(result, "skipped_reason", "") or result.error)
        elif result.pending_resume:
            log.warning("Resume state saved: %s", result.error)
            if result.stop_requested:
                stop_reason = result.error
                should_break_after_summary = True
        else:
            log.error("Failure: %s", result.error)

            if "chapters_data" in result.error:
                existing_tags = normalize_text_items(book.get("tags"))
                if "bad" not in existing_tags:
                    new_tags = build_supabase_text_update(book.get("tags"), ["bad"], prefer="array")
                    try:
                        _update_book_tags_in_database(book["book_id"], new_tags)
                        book["tags"] = new_tags
                        log.info("Marked book tags with 'bad'.")
                    except Exception as e:
                        log.error("Failed to update tags: %s", e)

        try:
            save_run_summary(
                OUTPUT_ROOT,
                all_results,
                archive=False,
                extra={
                    "run_started_at": dt_module.datetime.fromtimestamp(run_started_at).isoformat(),
                    "elapsed_seconds": round(time.time() - run_started_at, 1),
                    "stop_reason": stop_reason,
                },
            )
        except Exception as e:
            log.warning("Failed to write incremental run summary: %s", e)

        if should_break_after_summary:
            break

    success = sum(1 for r in all_results if r.success)
    partial = sum(1 for r in all_results if getattr(r, "pending_resume", False))
    skipped = sum(1 for r in all_results if getattr(r, "skipped", False))
    failed = len(all_results) - success - partial - skipped
    runtime_console_print("\n" + "=" * 42, level="INFO")
    runtime_console_print("  Run Complete", level="INFO")
    runtime_console_print(
        f"  Total: {len(all_results)}  Success: {success}  Resume: {partial}  Skipped: {skipped}  Failed: {failed}",
        level="INFO",
    )
    if success_target_count > 0:
        runtime_console_print(f"  Upload Counter: {successful_upload_count}/{success_target_count}", level="INFO")
    runtime_console_print(f"  Output Dir: {OUTPUT_ROOT}", level="INFO")
    summary_path = save_run_summary(
        OUTPUT_ROOT,
        all_results,
        archive=True,
        extra={
            "run_started_at": dt_module.datetime.fromtimestamp(run_started_at).isoformat(),
            "elapsed_seconds": round(time.time() - run_started_at, 1),
            "stop_reason": stop_reason,
        },
    )
    runtime_console_print(f"  Summary: {summary_path}", level="INFO")
    runtime_console_print("=" * 42, level="INFO")

    return {
        "success": failed == 0 and partial == 0,
        "results": all_results,
        "summary_path": summary_path,
        "stop_reason": stop_reason,
        "successful_upload_count": successful_upload_count,
    }


# ============================================================================
# Podcast runtime integration test variant
# ============================================================================
from collections import defaultdict as _podcast_defaultdict
from google.auth.transport.requests import AuthorizedSession
from PIL import ImageDraw, ImageFont, ImageOps
from openai import OpenAI


_PODCAST_RUNTIME_DEFAULTS = {
    "ENABLE_YOUTUBE_PODCAST_RUNTIME": True,
    "ENABLE_YOUTUBE_PODCAST_UNIFIED_SHOW": True,
    "ENABLE_YOUTUBE_PODCAST_SPLIT_PLAYLIST": True,
    "YOUTUBE_PODCAST_SHOW_TITLE_TEMPLATE": "{channel_name}｜长篇有声书全集",
    "YOUTUBE_PODCAST_IMAGE_SIZE": 2048,
    "YOUTUBE_PODCAST_IMAGE_MAX_BYTES": 2097152,
    "YOUTUBE_PODCAST_SHOW_PLAYLIST_SETTING_KEY": "podcast_longform_show_playlist_id",
    "SENSENOVA_BASE_URL": "https://token.sensenova.cn/v1",
    "SENSENOVA_API_KEY": "sk-8Tr86c17YvA5jBEoem2uYYAQGXGzmpDU",
    "YOUTUBE_PODCAST_TEXT_MODEL_PRIMARY": "deepseek-v4-flash",
    "YOUTUBE_PODCAST_TEXT_MODEL_FALLBACK": "sensenova-6.7-flash-lite",
    "YOUTUBE_PODCAST_IMAGE_MODEL_PRIMARY": "sensenova-u1-fast",
    "YOUTUBE_PODCAST_TEXT_MODEL_RETRIES": 2,
    "YOUTUBE_PODCAST_IMAGE_MODEL_RETRIES": 3,
    "YOUTUBE_PODCAST_AI_RETRY_BASE_SECONDS": 30.0,
    "YOUTUBE_PODCAST_YT_RETRIES": 5,
    "YOUTUBE_PODCAST_YT_RETRY_BASE_SECONDS": 3.0,
    "YOUTUBE_PODCAST_FONT_CACHE_DIRNAME": "_podcast_font_cache",
}
DEFAULT_RUNTIME_CONFIG.update(_PODCAST_RUNTIME_DEFAULTS)
apply_runtime_config()


_PODCAST_PLAYLIST_IMAGES_ENDPOINT = "https://www.googleapis.com/youtube/v3/playlistImages"
_PODCAST_SHOW_IMAGE_FILENAME = "podcast_longform_show_cover.jpg"
_PODCAST_PLAYLIST_ASSET_DIR = "_podcast_playlist_assets"
_PODCAST_SHOW_ASSET_DIR = "_podcast_show_assets"


def _podcast_runtime_enabled():
    return bool(globals().get("ENABLE_YOUTUBE_PODCAST_RUNTIME", False))


def _podcast_unified_show_enabled():
    return bool(globals().get("ENABLE_YOUTUBE_PODCAST_UNIFIED_SHOW", False))


def _podcast_split_playlist_enabled():
    return bool(globals().get("ENABLE_YOUTUBE_PODCAST_SPLIT_PLAYLIST", False))


def _podcast_show_setting_key():
    return str(
        globals().get("YOUTUBE_PODCAST_SHOW_PLAYLIST_SETTING_KEY", "podcast_longform_show_playlist_id") or ""
    ).strip() or "podcast_longform_show_playlist_id"


def _podcast_show_title(channel_name):
    template = str(
        globals().get("YOUTUBE_PODCAST_SHOW_TITLE_TEMPLATE", "{channel_name}｜长篇有声书全集")
        or "{channel_name}｜长篇有声书全集"
    )
    normalized = str(channel_name or "").strip()
    try:
        return template.format(channel_name=normalized)
    except Exception:
        return f"{normalized}｜长篇有声书全集"


def _podcast_image_size():
    try:
        return max(512, int(globals().get("YOUTUBE_PODCAST_IMAGE_SIZE", 2048) or 2048))
    except Exception:
        return 2048


def _podcast_image_max_bytes():
    try:
        return max(512000, int(globals().get("YOUTUBE_PODCAST_IMAGE_MAX_BYTES", 2097152) or 2097152))
    except Exception:
        return 2097152


def _podcast_progress(message):
    log.info("[podcast] %s", str(message or "").strip())


def _podcast_now_iso():
    return dt_module.datetime.now().isoformat()


def _podcast_short(text, limit=72):
    normalized = re.sub(r"\s+", " ", str(text or "").strip())
    if len(normalized) <= limit:
        return normalized
    return normalized[: max(8, limit - 1)].rstrip() + "…"


def _sanitize_filename_component(value):
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", str(value or "").strip())
    cleaned = cleaned.strip("._")
    return cleaned or "item"


def _podcast_load_channel_setting(channel_name, setting_key):
    normalized_channel = str(channel_name or "").strip()
    normalized_key = str(setting_key or "").strip()
    if not normalized_channel or not normalized_key:
        return ""

    table_sql = get_public_table_identifier(get_cloud_runtime_settings_table_name())
    row = execute_postgres_fetchone(
        sql.SQL(
            """
            SELECT setting_value
            FROM {}
            WHERE channel_name = %s AND setting_key = %s
            ORDER BY updated_at DESC
            LIMIT 1
            """
        ).format(table_sql),
        (normalized_channel, normalized_key),
        optional=True,
    )
    return str((row or {}).get("setting_value") or "").strip()


def _podcast_save_channel_setting(channel_name, setting_key, setting_value):
    normalized_channel = str(channel_name or "").strip()
    normalized_key = str(setting_key or "").strip()
    if not normalized_channel or not normalized_key:
        return False

    now = _podcast_now_iso()
    table_sql = get_public_table_identifier(get_cloud_runtime_settings_table_name())
    execute_postgres(
        sql.SQL(
            """
            INSERT INTO {} (
              channel_name,
              setting_key,
              setting_value,
              created_at,
              updated_at
            )
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (channel_name, setting_key)
            DO UPDATE SET
              setting_value = EXCLUDED.setting_value,
              updated_at = EXCLUDED.updated_at
            """
        ).format(table_sql),
        (
            normalized_channel,
            normalized_key,
            str(setting_value or "").strip(),
            now,
            now,
        ),
        optional=True,
    )
    return True


def _podcast_extract_best_thumbnail_url(thumbnails):
    if not isinstance(thumbnails, dict):
        return ""
    preferred = ["maxres", "standard", "high", "medium", "default"]
    for key in preferred:
        row = thumbnails.get(key) or {}
        url = str(row.get("url") or "").strip()
        if url:
            return url
    for row in thumbnails.values():
        if not isinstance(row, dict):
            continue
        url = str(row.get("url") or "").strip()
        if url:
            return url
    return ""


def _podcast_normalize_status(value):
    normalized = str(value or "").strip().lower()
    if normalized in {"enabled", "disabled"}:
        return normalized
    return ""


def _podcast_playlist_row_to_record(item):
    snippet = item.get("snippet") or {}
    status = item.get("status") or {}
    playlist_id = str(item.get("id") or "").strip()
    return {
        "playlist_id": playlist_id,
        "playlist_url": f"https://www.youtube.com/playlist?list={playlist_id}" if playlist_id else "",
        "title": str(snippet.get("title") or "").strip(),
        "description": str(snippet.get("description") or ""),
        "thumbnail_url": _podcast_extract_best_thumbnail_url(snippet.get("thumbnails") or {}),
        "privacy_status": normalize_playlist_privacy_status(status.get("privacyStatus") or "public"),
        "podcast_status": _podcast_normalize_status(status.get("podcastStatus")),
    }


def _podcast_error_text(error):
    return re.sub(r"\s+", " ", str(error or "")).strip()


def _podcast_extract_http_error_details(error):
    status = int(getattr(getattr(error, "resp", None), "status", 0) or 0)
    reason = ""
    payload_text = ""
    try:
        raw = getattr(error, "content", b"") or b""
        if isinstance(raw, (bytes, bytearray)):
            payload_text = raw.decode("utf-8", errors="ignore")
        else:
            payload_text = str(raw)
        payload = json.loads(payload_text) if payload_text else {}
        items = ((payload.get("error") or {}).get("errors") or []) if isinstance(payload, dict) else []
        if items:
            reason = str((items[0] or {}).get("reason") or "").strip()
    except Exception:
        payload_text = _podcast_error_text(error)
    if not reason:
        reason = _podcast_error_text(error)
    return status, reason, payload_text


def _podcast_is_retryable_text_error(message):
    text = str(message or "").lower()
    return any(
        token in text
        for token in [
            "timeout",
            "timed out",
            "temporarily unavailable",
            "connection reset",
            "connection aborted",
            "connection broken",
            "service unavailable",
            "bad gateway",
            "internal error",
        ]
    )


def _podcast_is_retryable_youtube_http_error(error):
    if not isinstance(error, HttpError):
        return False

    status, reason, payload_text = _podcast_extract_http_error_details(error)
    reason_lower = str(reason or "").lower()
    payload_lower = str(payload_text or "").lower()
    if status in {408, 409, 429, 500, 502, 503, 504}:
        return True

    retryable_reasons = {
        "serviceUnavailable",
        "backendError",
        "internalError",
        "rateLimitExceeded",
        "userRateLimitExceeded",
        "quotaExceeded",
        "conflict",
    }
    if reason_lower.replace("_", "") in {item.lower().replace("_", "") for item in retryable_reasons}:
        return True
    return "service_unavailable" in payload_lower or "the operation was aborted" in payload_lower


def _podcast_youtube_retry_sleep_seconds(attempt_index):
    base = float(globals().get("YOUTUBE_PODCAST_YT_RETRY_BASE_SECONDS", 3.0) or 3.0)
    return max(1.0, base * (2 ** max(0, int(attempt_index or 0))))


def _podcast_ai_retry_sleep_seconds(_attempt_index):
    base = float(globals().get("YOUTUBE_PODCAST_AI_RETRY_BASE_SECONDS", 30.0) or 30.0)
    return max(1.0, base)


def _podcast_execute_youtube_request(request, op_name="youtube request"):
    retries = max(1, int(globals().get("YOUTUBE_PODCAST_YT_RETRIES", 5) or 5))
    last_error = None
    for attempt_index in range(retries):
        try:
            return request.execute()
        except HttpError as e:
            last_error = e
            if attempt_index >= retries - 1 or not _podcast_is_retryable_youtube_http_error(e):
                raise
            sleep_seconds = _podcast_youtube_retry_sleep_seconds(attempt_index)
            status, reason, _payload = _podcast_extract_http_error_details(e)
            _podcast_progress(
                f"{op_name} hit transient YouTube error status={status} reason={reason or 'unknown'}, retrying in {sleep_seconds:.0f}s ({attempt_index + 1}/{retries})"
            )
            time.sleep(sleep_seconds)
        except Exception as e:
            last_error = e
            if attempt_index >= retries - 1 or not _podcast_is_retryable_text_error(e):
                raise
            sleep_seconds = _podcast_youtube_retry_sleep_seconds(attempt_index)
            _podcast_progress(
                f"{op_name} hit transient request error, retrying in {sleep_seconds:.0f}s ({attempt_index + 1}/{retries}): {_podcast_error_text(e)}"
            )
            time.sleep(sleep_seconds)
    if last_error is not None:
        raise last_error
    raise RuntimeError(f"{op_name} failed without response")


def _podcast_fetch_playlist_by_id(youtube, playlist_id, retries=6, wait_seconds=1.5):
    normalized = str(playlist_id or "").strip()
    if not normalized:
        return {}

    attempts = max(1, int(retries or 1))
    for attempt_index in range(attempts):
        response = _podcast_execute_youtube_request(
            youtube.playlists().list(part="snippet,status", id=normalized, maxResults=1),
            op_name=f"playlists.list:{normalized}",
        )
        items = response.get("items", [])
        if items:
            return _podcast_playlist_row_to_record(items[0])
        if attempt_index < attempts - 1:
            time.sleep(max(0.1, float(wait_seconds or 0.1)))
    return {}


def _podcast_wait_for_playlist_podcast_status(
    youtube,
    playlist_id,
    desired_status="enabled",
    retries=15,
    wait_seconds=3.0,
):
    normalized = str(playlist_id or "").strip()
    target = _podcast_normalize_status(desired_status)
    if not normalized or not target:
        return {}

    attempts = max(1, int(retries or 1))
    last_seen = {}
    for attempt_index in range(attempts):
        fetched = _podcast_fetch_playlist_by_id(youtube, normalized, retries=1, wait_seconds=0)
        if fetched:
            last_seen = fetched
        if str((last_seen or {}).get("podcast_status") or "").strip().lower() == target:
            return last_seen
        if attempt_index < attempts - 1:
            time.sleep(max(0.1, float(wait_seconds or 0.1)))
    return last_seen


def _list_owned_playlists_with_client(youtube):
    playlists = []
    page_token = None
    while True:
        response = _podcast_execute_youtube_request(
            youtube.playlists().list(
                part="snippet,status",
                mine=True,
                maxResults=50,
                pageToken=page_token,
            ),
            op_name="playlists.list:mine",
        )
        for item in response.get("items", []):
            row = _podcast_playlist_row_to_record(item)
            playlists.append(row)
        page_token = response.get("nextPageToken")
        if not page_token:
            break
    return playlists


def _create_or_update_playlist_with_client(
    youtube,
    title,
    description="",
    privacy_status="public",
    playlist_id="",
    podcast_status=None,
):
    normalized_privacy = normalize_playlist_privacy_status(privacy_status)
    default_language, _generated_localizations = build_youtube_traditional_localizations(
        title=title,
        description=description,
    )
    body = {
        "snippet": {
            "title": str(title or "")[:150],
            "description": str(description or "")[:5000],
            "defaultLanguage": default_language,
        },
        "status": {
            "privacyStatus": normalized_privacy,
        },
    }
    normalized_podcast_status = _podcast_normalize_status(podcast_status)
    if normalized_podcast_status:
        body["status"]["podcastStatus"] = normalized_podcast_status

    if playlist_id:
        body["id"] = playlist_id
        response = _podcast_execute_youtube_request(
            youtube.playlists().update(part="snippet,status", body=body),
            op_name=f"playlists.update:{playlist_id}",
        )
    else:
        response = _podcast_execute_youtube_request(
            youtube.playlists().insert(part="snippet,status", body=body),
            op_name=f"playlists.insert:{_podcast_short(title, 48)}",
        )

    final_playlist_id = str(response.get("id") or "").strip()
    localization_sync = _sync_playlist_localizations_with_client(
        youtube,
        final_playlist_id,
        title=body["snippet"]["title"],
        description=body["snippet"]["description"],
        force_overwrite=False,
    )
    if localization_sync.get("failed_locales"):
        log.warning(
            "Playlist localization sync partially failed for %s; continuing playlist success path. failed=%s",
            final_playlist_id,
            json.dumps(localization_sync.get("failed_locales", {}), ensure_ascii=False),
        )

    fetched = _podcast_fetch_playlist_by_id(youtube, final_playlist_id, retries=8, wait_seconds=1.5) if final_playlist_id else {}
    result = {
        "playlist_id": final_playlist_id,
        "playlist_url": f"https://www.youtube.com/playlist?list={final_playlist_id}" if final_playlist_id else "",
        "title": body["snippet"]["title"],
        "description": body["snippet"]["description"],
        "privacy_status": normalized_privacy,
        "localizations_applied": localization_sync.get("applied_locales", []),
        "localizations_failed": localization_sync.get("failed_locales", {}),
        "podcast_status": normalized_podcast_status,
    }
    if fetched:
        result.update(
            {
                "thumbnail_url": fetched.get("thumbnail_url", ""),
                "podcast_status": fetched.get("podcast_status", result.get("podcast_status", "")),
            }
        )
    return result


def _list_playlist_items_with_client(youtube, playlist_id):
    items = []
    page_token = None
    playlist_not_found_retry_count = 0
    max_playlist_not_found_retries = 6
    while True:
        try:
            response = _podcast_execute_youtube_request(
                youtube.playlistItems().list(
                    part="snippet,contentDetails",
                    playlistId=playlist_id,
                    maxResults=50,
                    pageToken=page_token,
                ),
                op_name=f"playlistItems.list:{playlist_id}",
            )
        except HttpError as e:
            if is_playlist_not_found_http_error(e) and playlist_not_found_retry_count < max_playlist_not_found_retries:
                playlist_not_found_retry_count += 1
                wait_seconds = min(12, 2 + playlist_not_found_retry_count)
                log.warning(
                    "播放列表 %s 暂时还不可读，等待 %d 秒后重试读取（%d/%d）...",
                    playlist_id,
                    wait_seconds,
                    playlist_not_found_retry_count,
                    max_playlist_not_found_retries,
                )
                time.sleep(wait_seconds)
                page_token = None
                continue
            raise

        for item in response.get("items", []):
            snippet = item.get("snippet") or {}
            content_details = item.get("contentDetails") or {}
            resource_id = snippet.get("resourceId") or {}
            video_id = str(resource_id.get("videoId") or content_details.get("videoId") or "").strip()
            items.append(
                {
                    "playlist_item_id": str(item.get("id") or "").strip(),
                    "video_id": video_id,
                    "position": int(snippet.get("position") or 0),
                    "title": str(snippet.get("title") or "").strip(),
                }
            )
        page_token = response.get("nextPageToken")
        if not page_token:
            break
    return items


def _delete_playlist_item_with_client(youtube, playlist_item_id):
    _podcast_execute_youtube_request(
        youtube.playlistItems().delete(id=playlist_item_id),
        op_name=f"playlistItems.delete:{playlist_item_id}",
    )


def _insert_playlist_video_with_client(youtube, playlist_id, video_id):
    response = _podcast_execute_youtube_request(
        youtube.playlistItems().insert(
            part="snippet",
            body={
                "snippet": {
                    "playlistId": playlist_id,
                    "resourceId": {
                        "kind": "youtube#video",
                        "videoId": video_id,
                    },
                }
            },
        ),
        op_name=f"playlistItems.insert:{playlist_id}:{video_id}",
    )
    return {
        "playlist_item_id": str(response.get("id") or "").strip(),
        "video_id": str(video_id or "").strip(),
    }


def _update_playlist_item_position_with_client(youtube, playlist_item_id, playlist_id, video_id, position):
    _podcast_execute_youtube_request(
        youtube.playlistItems().update(
            part="snippet",
            body={
                "id": playlist_item_id,
                "snippet": {
                    "playlistId": playlist_id,
                    "resourceId": {
                        "kind": "youtube#video",
                        "videoId": video_id,
                    },
                    "position": int(position),
                },
            },
        ),
        op_name=f"playlistItems.update:{playlist_id}:{video_id}:{position}",
    )


def _podcast_extract_youtube_credentials(youtube):
    http_obj = getattr(youtube, "_http", None)
    candidates = [
        getattr(http_obj, "credentials", None),
        getattr(getattr(http_obj, "request", None), "credentials", None),
        getattr(getattr(http_obj, "http", None), "credentials", None),
        getattr(getattr(getattr(http_obj, "http", None), "request", None), "credentials", None),
    ]
    credentials = next((item for item in candidates if item is not None), None)
    if credentials is None:
        raise RuntimeError("无法从 YouTube client 提取 OAuth credentials。")
    if (getattr(credentials, "expired", False) or not getattr(credentials, "valid", True)) and getattr(
        credentials,
        "refresh_token",
        None,
    ):
        credentials.refresh(GoogleAuthRequest())
    return credentials


def _podcast_playlist_image_row(item, fallback_playlist_id):
    snippet = item.get("snippet") or {}
    return {
        "image_id": str(item.get("id") or "").strip(),
        "playlist_id": str(snippet.get("playlistId") or fallback_playlist_id or "").strip(),
        "type": str(snippet.get("type") or "").strip().lower(),
        "width": int(snippet.get("width") or 0),
        "height": int(snippet.get("height") or 0),
    }


def _podcast_is_playlist_images_unsupported_error(message):
    return "PLAYLIST_TYPE_UNSUPPORTED" in str(message or "")


def _podcast_list_playlist_images_via_rest(youtube, playlist_id, filter_params):
    credentials = _podcast_extract_youtube_credentials(youtube)
    session = AuthorizedSession(credentials)
    images = []
    page_token = None
    retries = max(1, int(globals().get("YOUTUBE_PODCAST_YT_RETRIES", 5) or 5))
    while True:
        params = {
            "part": "snippet",
            "maxResults": 50,
            **filter_params,
        }
        if page_token:
            params["pageToken"] = page_token

        last_error = None
        payload = None
        for attempt_index in range(retries):
            response = session.get(_PODCAST_PLAYLIST_IMAGES_ENDPOINT, params=params, timeout=60)
            if response.status_code < 400:
                payload = response.json()
                break

            try:
                payload = response.json()
            except Exception:
                payload = response.text
            last_error = RuntimeError(
                f"playlistImages.list failed: status={response.status_code} params={params} payload={payload}"
            )
            if response.status_code not in {408, 409, 429, 500, 502, 503, 504} or attempt_index >= retries - 1:
                raise last_error
            sleep_seconds = _podcast_youtube_retry_sleep_seconds(attempt_index)
            _podcast_progress(
                f"playlistImages.list retrying in {sleep_seconds:.0f}s for playlist={playlist_id} status={response.status_code}"
            )
            time.sleep(sleep_seconds)
        if payload is None and last_error is not None:
            raise last_error

        for item in (payload or {}).get("items", []):
            images.append(_podcast_playlist_image_row(item, playlist_id))
        page_token = (payload or {}).get("nextPageToken")
        if not page_token:
            break
    return images


def _podcast_list_playlist_images(youtube, playlist_id):
    normalized = str(playlist_id or "").strip()
    if not normalized:
        return []

    errors = []
    for filter_params in (
        {"playlistId": normalized},
        {"parent": f"playlists/{normalized}"},
    ):
        try:
            return _podcast_list_playlist_images_via_rest(youtube, normalized, filter_params)
        except Exception as e:
            errors.append(str(e))

    if any(_podcast_is_playlist_images_unsupported_error(item) for item in errors):
        return []

    raise RuntimeError(" ; ".join(errors) or f"无法列出 playlist images: {normalized}")


def _podcast_resolve_playlist_image_status(images, podcast_enabled):
    detected = bool(images)
    assumed = bool(podcast_enabled and not detected)
    has_image = bool(detected or assumed)
    if detected:
        label = "yes"
    elif assumed:
        label = "yes(assumed)"
    else:
        label = "no"
    return {
        "detected": detected,
        "assumed": assumed,
        "has_image": has_image,
        "label": label,
    }


def _podcast_sync_playlist_image(
    youtube,
    playlist_id,
    image_path,
    existing_images=None,
    blind_insert=False,
):
    hero_image = {}
    if not blind_insert:
        existing_images = existing_images if existing_images is not None else _podcast_list_playlist_images(youtube, playlist_id)
        hero_image = next((item for item in existing_images if item.get("type") == "hero"), {})

    body = {
        "snippet": {
            "playlistId": playlist_id,
            "type": "hero",
        }
    }
    media = MediaFileUpload(image_path, mimetype="image/jpeg")

    if hero_image.get("image_id"):
        body["id"] = hero_image["image_id"]
        response = _podcast_execute_youtube_request(
            youtube.playlistImages().update(part="snippet", body=body, media_body=media),
            op_name=f"playlistImages.update:{playlist_id}",
        )
    else:
        response = _podcast_execute_youtube_request(
            youtube.playlistImages().insert(part="snippet", body=body, media_body=media),
            op_name=f"playlistImages.insert:{playlist_id}",
        )

    snippet = response.get("snippet") or {}
    return {
        "image_id": str(response.get("id") or body.get("id") or "").strip(),
        "playlist_id": str(snippet.get("playlistId") or playlist_id),
        "type": str(snippet.get("type") or "hero").strip().lower(),
        "width": int(snippet.get("width") or 0),
        "height": int(snippet.get("height") or 0),
    }


def _podcast_create_sensenova_client():
    return OpenAI(
        base_url=str(globals().get("SENSENOVA_BASE_URL", "https://token.sensenova.cn/v1") or "").strip(),
        api_key=str(globals().get("SENSENOVA_API_KEY", "") or "").strip(),
    )


def _podcast_extract_chat_text(response):
    try:
        return str(response.choices[0].message.content or "").strip()
    except Exception:
        return ""


def _podcast_is_rate_limited_error(error):
    text = _podcast_error_text(error).lower()
    return any(
        token in text
        for token in [
            "429",
            "rate limit",
            "too many requests",
            "quota",
            "exceeded",
            "rate_limit",
            "call limit",
        ]
    )


def _podcast_is_security_rejection_error(error):
    text = _podcast_error_text(error).lower()
    return (
        "security reasons" in text
        or ("invalid_request_error" in text and "'code': '18'" in text)
        or ('"code": "18"' in text)
        or ('"code":18' in text)
    )


def _podcast_is_retryable_ai_error(error):
    text = _podcast_error_text(error).lower()
    if _podcast_is_rate_limited_error(text):
        return True
    return any(
        token in text
        for token in [
            "timeout",
            "timed out",
            "connection",
            "temporarily",
            "temporarily unavailable",
            "server error",
            "service unavailable",
            "bad gateway",
            "502",
            "503",
            "504",
            "internal error",
            "overloaded",
        ]
    )


def _podcast_chat_complete_with_model(client, model, prompt, system_prompt="You are a helpful assistant."):
    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt},
        ],
    )
    return _podcast_extract_chat_text(response)


def _podcast_generate_text_via_models(prompt, purpose, fallback_text=""):
    api_key = str(globals().get("SENSENOVA_API_KEY", "") or "").strip()
    if not api_key:
        return {
            "text": fallback_text,
            "model": "fallback",
            "error": "SENSENOVA_API_KEY is empty",
        }

    client = _podcast_create_sensenova_client()
    attempts_log = []
    models = [
        str(globals().get("YOUTUBE_PODCAST_TEXT_MODEL_PRIMARY", "deepseek-v4-flash") or "deepseek-v4-flash").strip(),
        str(
            globals().get("YOUTUBE_PODCAST_TEXT_MODEL_FALLBACK", "sensenova-6.7-flash-lite")
            or "sensenova-6.7-flash-lite"
        ).strip(),
    ]
    retries = max(1, int(globals().get("YOUTUBE_PODCAST_TEXT_MODEL_RETRIES", 2) or 2))

    for model_index, model in enumerate(models):
        if not model:
            continue
        for attempt_index in range(retries):
            try:
                if attempt_index > 0 or model_index > 0:
                    _podcast_progress(
                        f"{purpose}: trying text model {model} (attempt {attempt_index + 1}/{retries})"
                    )
                text = _podcast_chat_complete_with_model(client, model, prompt)
                if text:
                    return {
                        "text": text,
                        "model": model,
                        "error": " ; ".join(attempts_log),
                    }
                attempts_log.append(f"{model} attempt {attempt_index + 1}: empty response")
            except Exception as e:
                err = _podcast_error_text(e)
                attempts_log.append(f"{model} attempt {attempt_index + 1}: {err}")
                if _podcast_is_rate_limited_error(err) and model_index == 0:
                    _podcast_progress(
                        f"{purpose}: {model} hit rate limit, switching to {models[-1] or 'fallback'}"
                    )
                    break
                if _podcast_is_retryable_ai_error(err) and attempt_index < retries - 1:
                    sleep_seconds = _podcast_ai_retry_sleep_seconds(attempt_index)
                    _podcast_progress(f"{purpose}: {model} retrying in {sleep_seconds:.0f}s")
                    time.sleep(sleep_seconds)
                    continue
                break

    return {
        "text": fallback_text,
        "model": "fallback",
        "error": " ; ".join(attempts_log) or "text generation failed",
    }


def _podcast_build_default_show_description(channel_name, episode_count):
    return (
        f"这里是 {channel_name} 的长篇有声书全集。我们将频道内适合完整收听的长篇有声内容整理为统一书库，"
        f"每本完整书作为一个 episode，便于连续播放、长期收藏与慢慢聆听。当前已整理 {episode_count} 本完整书，后续也会持续更新。"
    )[:5000]


def _podcast_generate_show_description(channel_name, show_title, episode_titles):
    fallback = _podcast_build_default_show_description(channel_name, len(episode_titles))
    sampled_titles = [str(item or "").strip()[:80] for item in episode_titles[:12] if str(item or "").strip()]
    titles_block = "\n".join(f"- {item}" for item in sampled_titles) or "- 暂无 episode 标题样例"
    prompt = f"""
你现在要为一个 YouTube podcast show 撰写中文简介。

频道名：{channel_name}
Show 标题：{show_title}
Episode 标题样例：
{titles_block}

要求：
1. 直接输出 120 到 220 字左右的中文简介正文。
2. 强调“每本完整书 = 一个 episode”“适合连续收听”“长期更新的长篇有声书书库”。
3. 风格自然、可信、适合 YouTube podcast show，不要列表，不要 emoji，不要引号，不要口号式空话。
4. 不要输出标题，只输出简介正文。
""".strip()
    result = _podcast_generate_text_via_models(prompt, purpose="show description", fallback_text=fallback)
    text = str(result.get("text") or "").strip()
    if text and str(result.get("model") or "") != "fallback":
        return {
            "description": text[:5000],
            "source": f"ai:{result['model']}",
            "error": str(result.get("error") or ""),
        }
    return {
        "description": fallback,
        "source": "fallback",
        "error": str(result.get("error") or "AI 返回空描述"),
    }


def _podcast_build_default_cover_prompt(channel_name, _show_title):
    return (
        "YouTube podcast cover, square 1:1 composition, premium Chinese audiobook brand identity, "
        "ancient Chinese books and bamboo scrolls arranged in a cinematic library scene, warm golden light, "
        "deep red and dark wood palette, elegant but high-contrast layout, bold readable Chinese title text "
        f"\"{channel_name}\" with subtitle \"长篇有声书全集\", luxury editorial style, clean center composition, highly detailed, 2048x2048"
    )


def _podcast_generate_show_cover_prompt(channel_name, show_title, episode_titles):
    fallback = _podcast_build_default_cover_prompt(channel_name, show_title)
    sampled_titles = [str(item or "").strip()[:60] for item in episode_titles[:8] if str(item or "").strip()]
    titles_block = "\n".join(f"- {item}" for item in sampled_titles) or "- long-form Chinese audiobooks"
    prompt = f"""
Write one single English image prompt for a YouTube podcast cover.

Channel name: {channel_name}
Show title: {show_title}
Episode samples:
{titles_block}

Requirements:
1. Square 1:1 cover for a podcast show, not 16:9 thumbnail.
2. Chinese long-form audiobook atmosphere.
3. Must emphasize premium readability and visible Chinese typography for the channel name and 长篇有声书全集.
4. No markdown, no explanation, output one prompt only.
5. Mention 2048x2048.
""".strip()
    result = _podcast_generate_text_via_models(prompt, purpose="show cover prompt", fallback_text=fallback)
    text = str(result.get("text") or "").strip()
    if text and str(result.get("model") or "") != "fallback":
        return {
            "prompt": text,
            "source": f"ai:{result['model']}",
            "error": str(result.get("error") or ""),
        }
    return {
        "prompt": fallback,
        "source": "fallback",
        "error": str(result.get("error") or "AI 返回空封面 prompt"),
    }


def _podcast_build_batch_playlist_cover_prompt_fallback(playlist_title, playlist_description):
    short_desc = str(playlist_description or "").strip().replace("\n", " ")[:240]
    return (
        "YouTube podcast cover, square 1:1 composition, premium Chinese audiobook or knowledge playlist visual identity, "
        f"bold readable Chinese title text \"{playlist_title}\" as the main focus, elegant editorial layout, warm cinematic lighting, "
        "rich dark red and gold palette, bookshelf and scroll atmosphere, high contrast, highly detailed, 2048x2048. "
        f"Context: {short_desc}"
    )


def _podcast_generate_batch_playlist_cover_prompt(playlist_title, playlist_description):
    fallback = _podcast_build_batch_playlist_cover_prompt_fallback(playlist_title, playlist_description)
    prompt = f"""
Write one single English image prompt for a square YouTube podcast cover.

Playlist title: {playlist_title}
Playlist description: {str(playlist_description or '').strip()[:800]}

Requirements:
1. Square 1:1 cover for a podcast playlist, not a 16:9 thumbnail.
2. Keep the playlist title as the main visible Chinese typography element.
3. Style should fit Chinese long-form audio, audiobooks, lectures, or serialized knowledge content.
4. Strong readability, premium editorial design, highly detailed.
5. Output one prompt only, no explanation, mention 2048x2048.
""".strip()
    result = _podcast_generate_text_via_models(prompt, purpose="playlist cover prompt", fallback_text=fallback)
    text = str(result.get("text") or "").strip()
    if text and str(result.get("model") or "") != "fallback":
        return {
            "prompt": text,
            "source": f"ai:{result['model']}",
            "error": str(result.get("error") or ""),
        }
    return {
        "prompt": fallback,
        "source": "fallback",
        "error": str(result.get("error") or "AI 返回空封面 prompt"),
    }


def _podcast_download_bytes(url):
    response = requests.get(url, timeout=180)
    response.raise_for_status()
    return response.content


def _podcast_save_square_cover_image(image_bytes, output_path, max_bytes=None):
    max_bytes = int(max_bytes or _podcast_image_max_bytes())
    os_path = Path(output_path)
    os_path.parent.mkdir(parents=True, exist_ok=True)

    with Image.open(BytesIO(image_bytes)) as img:
        base = ImageOps.fit(
            img.convert("RGB"),
            (_podcast_image_size(), _podcast_image_size()),
            method=Image.Resampling.LANCZOS,
        )
        for quality in [92, 88, 84, 80, 76, 72, 68, 64, 60]:
            base.save(os_path, format="JPEG", quality=quality, optimize=True, progressive=True)
            if os_path.stat().st_size <= max_bytes:
                return str(os_path)
    raise RuntimeError(f"生成的 podcast cover 超过 2MB 限制：{output_path}")


def _font_cache_dir():
    cache_dir = Path.cwd() / str(globals().get("YOUTUBE_PODCAST_FONT_CACHE_DIRNAME", "_podcast_font_cache") or "_podcast_font_cache")
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir


def _download_font_if_missing(url, target_path):
    try:
        if target_path.exists() and target_path.stat().st_size > 1024 * 1024:
            return target_path
        _podcast_progress(f"Downloading fallback Chinese font: {target_path.name}")
        response = requests.get(url, timeout=180)
        response.raise_for_status()
        target_path.write_bytes(response.content)
        if target_path.exists() and target_path.stat().st_size > 1024 * 1024:
            return target_path
    except Exception as e:
        _podcast_progress(f"Font download skipped: {_podcast_error_text(e)}")
    return None


def _resolve_cover_font_path(prefer_bold=True):
    local_candidates = [
        "C:/Windows/Fonts/msyhbd.ttc" if prefer_bold else "C:/Windows/Fonts/msyh.ttc",
        "C:/Windows/Fonts/simhei.ttf",
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc" if prefer_bold else "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/opentype/noto/NotoSansCJKsc-Bold.otf" if prefer_bold else "/usr/share/fonts/opentype/noto/NotoSansCJKsc-Regular.otf",
        "/usr/share/fonts/opentype/noto/NotoSerifCJK-Bold.ttc" if prefer_bold else "/usr/share/fonts/opentype/noto/NotoSerifCJK-Regular.ttc",
        "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc",
    ]
    for candidate in local_candidates:
        path = Path(candidate)
        if path.exists():
            return path

    cache_dir = _font_cache_dir()
    remote_candidates = [
        (
            "https://raw.githubusercontent.com/notofonts/noto-cjk/main/Sans/OTF/SimplifiedChinese/NotoSansCJKsc-Bold.otf",
            cache_dir / "NotoSansCJKsc-Bold.otf",
        )
        if prefer_bold
        else (
            "https://raw.githubusercontent.com/notofonts/noto-cjk/main/Sans/OTF/SimplifiedChinese/NotoSansCJKsc-Regular.otf",
            cache_dir / "NotoSansCJKsc-Regular.otf",
        ),
        (
            "https://raw.githubusercontent.com/notofonts/noto-cjk/main/Sans/OTF/SimplifiedChinese/NotoSansCJKsc-Regular.otf",
            cache_dir / "NotoSansCJKsc-Regular.otf",
        ),
    ]
    for url, path in remote_candidates:
        resolved = _download_font_if_missing(url, path)
        if resolved is not None:
            return resolved
    return None


def _pick_local_cover_font(size, prefer_bold=True):
    resolved_path = _resolve_cover_font_path(prefer_bold=prefer_bold)
    if resolved_path is not None:
        try:
            return ImageFont.truetype(str(resolved_path), size=size)
        except Exception as e:
            _podcast_progress(f"Font load fallback triggered: {_podcast_error_text(e)}")
    return ImageFont.load_default()


def _measure_text(draw, text, font):
    if not text:
        return (0, 0)
    left, top, right, bottom = draw.textbbox((0, 0), text, font=font)
    return (max(0, right - left), max(0, bottom - top))


def _draw_vertical_gradient(size, top_rgb, bottom_rgb):
    gradient = Image.new("RGB", (1, size))
    pixels = gradient.load()
    for y in range(size):
        ratio = y / max(1, size - 1)
        color = tuple(int(top_rgb[i] * (1.0 - ratio) + bottom_rgb[i] * ratio) for i in range(3))
        pixels[0, y] = color
    return gradient.resize((size, size))


def _wrap_text_to_width(draw, text, font, max_width, max_lines):
    cleaned = re.sub(r"\s+", " ", str(text or "").strip())
    if not cleaned:
        return []

    lines = []
    current = ""
    for ch in cleaned:
        candidate = current + ch
        width, _height = _measure_text(draw, candidate, font)
        if current and width > max_width:
            lines.append(current)
            current = ch
            if len(lines) >= max_lines - 1:
                break
        else:
            current = candidate

    remaining = cleaned[len("".join(lines)) :].strip()
    if current and len(lines) < max_lines:
        remaining = current + remaining[len(current) :]
    if remaining and len(lines) < max_lines:
        while remaining:
            candidate = ""
            for ch in remaining:
                probe = candidate + ch
                width, _height = _measure_text(draw, probe, font)
                if candidate and width > max_width:
                    break
                candidate = probe
            if not candidate:
                break
            lines.append(candidate)
            remaining = remaining[len(candidate) :].strip()
            if len(lines) >= max_lines:
                break
    if remaining and lines:
        lines[-1] = lines[-1].rstrip("，。、；：,. ") + "…"
    return [line for line in lines if line]


def _podcast_generate_local_text_gradient_cover(output_path, cover_title, cover_subtitle="", max_bytes=None):
    size = _podcast_image_size()
    max_bytes = int(max_bytes or _podcast_image_max_bytes())
    canvas = _draw_vertical_gradient(size, (16, 28, 54), (112, 48, 20)).convert("RGBA")
    overlay = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    draw = ImageDraw.Draw(overlay)

    draw.rounded_rectangle((96, 96, size - 96, size - 96), radius=72, outline=(242, 211, 148, 255), width=5)
    draw.rounded_rectangle((180, 260, size - 180, size - 320), radius=56, fill=(20, 16, 26, 118))
    draw.ellipse((size - 700, 150, size - 210, 640), fill=(255, 214, 120, 255))
    draw.ellipse((size - 660, 190, size - 250, 600), fill=(250, 193, 88, 255))
    draw.rectangle((180, size - 315, size - 180, size - 286), fill=(224, 186, 104, 230))
    canvas = Image.alpha_composite(canvas, overlay)
    draw = ImageDraw.Draw(canvas)

    normalized_title = re.sub(r"\s+", " ", str(cover_title or "").strip()) or "Podcast"
    max_text_width = size - 520
    title_lines = []
    title_font = None
    for font_size in [320, 300, 280, 260, 240, 220, 200, 180, 168]:
        font = _pick_local_cover_font(font_size, prefer_bold=True)
        wrapped = _wrap_text_to_width(draw, normalized_title, font, max_text_width, max_lines=3)
        if wrapped:
            title_lines = wrapped
            title_font = font
        if wrapped and len(wrapped) <= 3:
            break
    if title_font is None:
        title_font = _pick_local_cover_font(180, prefer_bold=True)
        title_lines = [normalized_title[:18] or "Podcast"]

    subtitle_text = str(cover_subtitle or "").strip()
    subtitle_font = _pick_local_cover_font(112, prefer_bold=False)
    title_line_height = max(_measure_text(draw, "国", title_font)[1], 140)
    subtitle_line_height = max(_measure_text(draw, "国", subtitle_font)[1], 86)
    title_block_height = len(title_lines) * title_line_height + max(0, len(title_lines) - 1) * 26
    subtitle_block_height = subtitle_line_height if subtitle_text else 0
    total_height = title_block_height + subtitle_block_height + (54 if subtitle_text else 0)
    start_y = max(430, int((size - total_height) * 0.52))

    y = start_y
    for line in title_lines:
        width, _height = _measure_text(draw, line, title_font)
        x = int((size - width) / 2)
        draw.text((x + 8, y + 10), line, font=title_font, fill=(10, 8, 12, 235))
        draw.text((x, y), line, font=title_font, fill=(252, 239, 208, 255))
        y += title_line_height + 26

    if subtitle_text:
        width, _height = _measure_text(draw, subtitle_text, subtitle_font)
        x = int((size - width) / 2)
        draw.text((x + 5, y + 6), subtitle_text, font=subtitle_font, fill=(10, 8, 12, 220))
        draw.text((x, y), subtitle_text, font=subtitle_font, fill=(255, 246, 228, 255))

    canvas = canvas.convert("RGB")
    temp = BytesIO()
    for quality in [92, 88, 84, 80, 76, 72, 68, 64]:
        temp.seek(0)
        temp.truncate(0)
        canvas.save(temp, format="JPEG", quality=quality, optimize=True, progressive=True)
        if temp.tell() <= max_bytes:
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            Path(output_path).write_bytes(temp.getvalue())
            return str(output_path)
    raise RuntimeError(f"本地方图封面超过 2MB 限制：{output_path}")


def _podcast_build_safe_retry_cover_prompt(cover_title, cover_subtitle=""):
    safe_title = str(cover_title or "知识播客").strip()[:80]
    safe_subtitle = str(cover_subtitle or "长篇有声书").strip()[:40]
    return (
        "Safe family-friendly YouTube podcast cover, square 1:1 composition, neutral library atmosphere, elegant bookshelf background, "
        f"soft golden light, clean editorial typography, large readable Chinese title text \"{safe_title}\", subtitle \"{safe_subtitle}\", "
        "non-violent, non-political, no blood, no weapons, no disturbing scene, premium but calm, 2048x2048"
    )


def _podcast_generate_cover_from_existing_thumbnail(thumbnail_url, output_path):
    normalized = str(thumbnail_url or "").strip()
    if not normalized:
        raise RuntimeError("playlist thumbnail URL 为空，无法裁剪生成 podcast 封面。")
    image_bytes = _podcast_download_bytes(normalized)
    return _podcast_save_square_cover_image(image_bytes, output_path)


def _podcast_generate_cover_from_local_image(image_path, output_path):
    normalized = str(image_path or "").strip()
    if not normalized or not _is_nonempty_local_file(normalized):
        raise RuntimeError("local cover image is not available")
    return _podcast_save_square_cover_image(Path(normalized).read_bytes(), output_path)


def _podcast_log_image_source(scope_label, image_result):
    source = str((image_result or {}).get("source") or "").strip()
    if not source:
        return
    if source.startswith("ai:"):
        _podcast_progress(f"{scope_label}: ai image used ({source})")
    elif source == "playlist_thumbnail_crop_fallback":
        _podcast_progress(f"{scope_label}: playlist thumbnail crop fallback")
    elif source == "local_cover_crop_fallback":
        _podcast_progress(f"{scope_label}: local cover crop fallback")
    elif source == "local_text_gradient_fallback":
        _podcast_progress(f"{scope_label}: local text gradient fallback")
    else:
        _podcast_progress(f"{scope_label}: image source {source}")


def _podcast_generate_named_cover_image(
    filename,
    cover_prompt,
    subdir=_PODCAST_PLAYLIST_ASSET_DIR,
    cover_title="",
    cover_subtitle="",
    thumbnail_fallback_url="",
    local_fallback_image_path="",
):
    output_dir = Path.cwd() / str(subdir or _PODCAST_PLAYLIST_ASSET_DIR)
    output_path = str(output_dir / _sanitize_filename_component(filename))
    client = _podcast_create_sensenova_client()
    prompt_in_use = str(cover_prompt or "").strip()
    attempts_log = []
    retries = max(1, int(globals().get("YOUTUBE_PODCAST_IMAGE_MODEL_RETRIES", 3) or 3))
    model_name = str(globals().get("YOUTUBE_PODCAST_IMAGE_MODEL_PRIMARY", "sensenova-u1-fast") or "sensenova-u1-fast").strip()

    for attempt_index in range(retries):
        try:
            response = client.images.generate(
                model=model_name,
                prompt=prompt_in_use,
                size=f"{_podcast_image_size()}x{_podcast_image_size()}",
                n=1,
            )
            image_url = str(response.data[0].url or "").strip()
            if not image_url:
                raise RuntimeError("图片接口没有返回可下载的 URL。")
            image_bytes = _podcast_download_bytes(image_url)
            final_path = _podcast_save_square_cover_image(image_bytes, output_path)
            return {
                "path": final_path,
                "url": image_url,
                "source": f"ai:{model_name}",
                "error": " ; ".join(attempts_log),
            }
        except Exception as e:
            err = _podcast_error_text(e)
            attempts_log.append(f"{model_name} attempt {attempt_index + 1}: {err}")
            if _podcast_is_security_rejection_error(err):
                prompt_in_use = _podcast_build_safe_retry_cover_prompt(cover_title or filename, cover_subtitle)
                _podcast_progress(
                    f"Image generation hit security filter, retrying with a safer prompt ({attempt_index + 1}/{retries})"
                )
            if attempt_index < retries - 1:
                sleep_seconds = _podcast_ai_retry_sleep_seconds(attempt_index)
                _podcast_progress(f"Image generation retrying in {sleep_seconds:.0f}s")
                time.sleep(sleep_seconds)
                continue

    if str(thumbnail_fallback_url or "").strip():
        _podcast_progress("AI image generation exhausted retries, switching to original playlist thumbnail crop")
        thumbnail_path = _podcast_generate_cover_from_existing_thumbnail(thumbnail_fallback_url, output_path)
        return {
            "path": thumbnail_path,
            "url": str(thumbnail_fallback_url or "").strip(),
            "source": "playlist_thumbnail_crop_fallback",
            "error": " ; ".join(attempts_log),
        }

    if _is_nonempty_local_file(local_fallback_image_path):
        _podcast_progress("AI image generation exhausted retries, switching to local cover crop")
        local_path = _podcast_generate_cover_from_local_image(local_fallback_image_path, output_path)
        return {
            "path": local_path,
            "url": "",
            "source": "local_cover_crop_fallback",
            "error": " ; ".join(attempts_log),
        }

    _podcast_progress("AI image generation exhausted retries, switching to local text/gradient cover")
    fallback_path = _podcast_generate_local_text_gradient_cover(
        output_path,
        cover_title=cover_title or filename,
        cover_subtitle=cover_subtitle,
    )
    return {
        "path": fallback_path,
        "url": "",
        "source": "local_text_gradient_fallback",
        "error": " ; ".join(attempts_log),
    }


def _podcast_generate_show_cover_image(channel_name, _show_title, cover_prompt, thumbnail_fallback_url="", local_fallback_image_path=""):
    return _podcast_generate_named_cover_image(
        _PODCAST_SHOW_IMAGE_FILENAME,
        cover_prompt,
        subdir=_PODCAST_SHOW_ASSET_DIR,
        cover_title=channel_name,
        cover_subtitle="长篇有声书全集",
        thumbnail_fallback_url=thumbnail_fallback_url,
        local_fallback_image_path=local_fallback_image_path,
    )


def _podcast_create_plain_playlist(youtube, title, description, enable_podcast=False):
    body = {
        "snippet": {
            "title": str(title or "")[:150],
            "description": str(description or "")[:5000],
        },
        "status": {
            "privacyStatus": "public",
        },
    }
    if enable_podcast:
        body["status"]["podcastStatus"] = "enabled"

    response = _podcast_execute_youtube_request(
        youtube.playlists().insert(part="snippet,status", body=body),
        op_name=f"playlists.insert:{_podcast_short(title, 48)}",
    )
    created = _podcast_playlist_row_to_record(response)
    if created.get("playlist_id"):
        fetched = _podcast_fetch_playlist_by_id(youtube, created["playlist_id"], retries=8, wait_seconds=1.5)
        return fetched or created
    return created


def _podcast_update_playlist(youtube, playlist_id, title, description, privacy_status="public", enable_podcast=True):
    normalized_privacy = normalize_playlist_privacy_status(privacy_status)
    body = {
        "id": playlist_id,
        "snippet": {
            "title": str(title or "")[:150],
            "description": str(description or "")[:5000],
        },
        "status": {
            "privacyStatus": normalized_privacy,
        },
    }
    if enable_podcast:
        body["status"]["podcastStatus"] = "enabled"

    response = _podcast_execute_youtube_request(
        youtube.playlists().update(part="snippet,status", body=body),
        op_name=f"playlists.update:{playlist_id}",
    )
    updated = _podcast_playlist_row_to_record(response)
    if enable_podcast:
        confirmed = _podcast_wait_for_playlist_podcast_status(
            youtube,
            playlist_id,
            desired_status="enabled",
            retries=15,
            wait_seconds=3.0,
        )
        if str((confirmed or {}).get("podcast_status") or "") == "enabled":
            return confirmed
        merged = {**updated, **confirmed}
        merged["podcast_status"] = "enabled"
        merged["podcast_status_pending"] = True
        return merged
    fetched = _podcast_fetch_playlist_by_id(youtube, playlist_id, retries=6, wait_seconds=1.0)
    return fetched or updated


def _podcast_resolve_existing_show_playlist(youtube, channel_name, title):
    saved_playlist_id = _podcast_load_channel_setting(channel_name, _podcast_show_setting_key())
    if saved_playlist_id:
        playlist = _podcast_fetch_playlist_by_id(youtube, saved_playlist_id)
        if playlist:
            playlist["source"] = "channel_runtime_settings"
            return playlist

    for playlist in _list_owned_playlists_with_client(youtube):
        if str(playlist.get("title") or "").strip() == str(title or "").strip():
            playlist["source"] = "title_match"
            return playlist
    return {}


def _podcast_ensure_video_in_playlist(youtube, playlist_id, video_id):
    normalized_video_id = _extract_youtube_video_id(video_id)
    if not normalized_video_id:
        raise RuntimeError("missing video_id for unified podcast show sync")

    current_items = _list_playlist_items_with_client(youtube, playlist_id)
    matches = [item for item in current_items if str(item.get("video_id") or "").strip() == normalized_video_id]
    if matches:
        ordered = sorted(matches, key=lambda item: int(item.get("position") or 0))
        for item in ordered[1:]:
            if item.get("playlist_item_id"):
                _delete_playlist_item_with_client(youtube, item["playlist_item_id"])
        return {
            "inserted": False,
            "already_present": True,
            "playlist_item_id": ordered[0].get("playlist_item_id", ""),
        }

    insert_result = _insert_playlist_video_with_client(youtube, playlist_id, normalized_video_id)
    return {
        "inserted": True,
        "already_present": False,
        "playlist_item_id": str(insert_result.get("playlist_item_id") or "").strip(),
    }


def _podcast_get_show_state_container(state):
    if not isinstance(state, dict):
        return {}
    value = state.get("podcast_show")
    if isinstance(value, dict):
        return value
    state["podcast_show"] = {}
    return state["podcast_show"]


def _podcast_apply_show_state_to_result(result, show_state):
    if not isinstance(show_state, dict):
        return result
    result.show_playlist_id = str(show_state.get("show_playlist_id") or "")
    result.show_image_source = str(show_state.get("show_image_source") or "")
    result.show_podcast_status = str(show_state.get("show_podcast_status") or "")
    result.show_last_synced_at = str(show_state.get("show_last_synced_at") or "")
    result.show_last_error = str(show_state.get("show_last_error") or "")
    return result


def sync_single_video_into_unified_podcast_show(
    channel_name,
    video_id,
    book_name="",
    cover_image_path="",
    show_thumbnail_hint="",
):
    normalized_channel = str(channel_name or "").strip()
    normalized_video_id = _extract_youtube_video_id(video_id)
    if not _podcast_runtime_enabled() or not _podcast_unified_show_enabled():
        return {
            "skipped": True,
            "reason": "podcast unified show disabled",
            "show_playlist_id": "",
            "show_image_source": "",
            "show_podcast_status": "",
            "show_last_synced_at": "",
            "show_last_error": "",
        }
    if not normalized_channel or not normalized_video_id:
        raise RuntimeError("single-video unified show sync requires channel_name and video_id")

    youtube = authenticate_youtube_from_supabase(normalized_channel)
    show_title = _podcast_show_title(normalized_channel)
    episode_titles = [str(book_name or "").strip()] if str(book_name or "").strip() else [show_title]
    show = _podcast_resolve_existing_show_playlist(youtube, normalized_channel, show_title)
    result = {
        "show_title": show_title,
        "show_playlist_id": str(show.get("playlist_id") or ""),
        "show_created": False,
        "show_metadata_updated": False,
        "show_image_uploaded": False,
        "show_image_source": "",
        "show_podcast_status": str(show.get("podcast_status") or ""),
        "video_inserted": False,
        "video_already_present": False,
        "show_last_synced_at": "",
        "show_last_error": "",
    }

    _podcast_progress(
        f"Unified show sync started for single video {normalized_video_id} -> {show_title}"
    )
    description_result = None
    need_metadata_refresh = bool(
        not show
        or str(show.get("title") or "").strip() != show_title
        or not str(show.get("description") or "").strip()
        or str(show.get("privacy_status") or "").strip() != "public"
    )
    if need_metadata_refresh:
        description_result = _podcast_generate_show_description(normalized_channel, show_title, episode_titles)
    description = (
        description_result["description"]
        if isinstance(description_result, dict)
        else str(show.get("description") or "").strip() or _podcast_build_default_show_description(normalized_channel, 1)
    )

    if not show:
        _podcast_progress("Unified show: creating playlist shell")
        show = _podcast_create_plain_playlist(youtube, show_title, description, enable_podcast=False)
        result["show_created"] = True

    if not show.get("playlist_id"):
        raise RuntimeError(f"未能创建或定位统一 podcast show。show={json.dumps(show, ensure_ascii=False)}")

    _podcast_save_channel_setting(normalized_channel, _podcast_show_setting_key(), show["playlist_id"])
    result["show_playlist_id"] = show["playlist_id"]
    show_is_podcast = str(show.get("podcast_status") or "") == "enabled"

    if need_metadata_refresh and not result["show_created"]:
        _podcast_progress("Unified show: updating title/description/privacy")
        show = _podcast_update_playlist(
            youtube,
            show["playlist_id"],
            show_title,
            description,
            privacy_status="public",
            enable_podcast=show_is_podcast,
        )
        result["show_metadata_updated"] = True
        show_is_podcast = str(show.get("podcast_status") or "") == "enabled"

    existing_images = _podcast_list_playlist_images(youtube, show["playlist_id"]) if show_is_podcast else []
    image_status = _podcast_resolve_playlist_image_status(existing_images, show_is_podcast)
    has_existing_image = bool(image_status.get("has_image"))

    if not has_existing_image:
        cover_prompt_result = _podcast_generate_show_cover_prompt(normalized_channel, show_title, episode_titles)
        _podcast_progress("Unified show: generating square cover image")
        image_result = _podcast_generate_show_cover_image(
            normalized_channel,
            show_title,
            cover_prompt_result["prompt"],
            thumbnail_fallback_url=str(show.get("thumbnail_url") or show_thumbnail_hint or ""),
            local_fallback_image_path=cover_image_path,
        )
        _podcast_log_image_source("Unified show", image_result)
        _podcast_progress("Unified show: uploading playlist image")
        _podcast_sync_playlist_image(
            youtube,
            show["playlist_id"],
            image_result["path"],
            existing_images=existing_images if show_is_podcast else None,
            blind_insert=not show_is_podcast,
        )
        result["show_image_uploaded"] = True
        result["show_image_source"] = str(image_result.get("source") or "")
        has_existing_image = True
    elif show_is_podcast and image_status.get("assumed"):
        result["show_image_source"] = "existing_assumed"
    elif has_existing_image:
        result["show_image_source"] = "existing"

    if not show_is_podcast:
        _podcast_progress("Unified show: enabling podcast status")
        show = _podcast_update_playlist(
            youtube,
            show["playlist_id"],
            show_title,
            description,
            privacy_status="public",
            enable_podcast=True,
        )
        result["show_metadata_updated"] = True
        if bool(show.get("podcast_status_pending")):
            raise RuntimeError("统一 show 的 podcastStatus 请求已提交，但当前还未回显 enabled。")
        show_is_podcast = str(show.get("podcast_status") or "") == "enabled"

    if not show_is_podcast:
        raise RuntimeError("统一 show 尚未成功切换为 podcast。")

    result["show_podcast_status"] = str(show.get("podcast_status") or "enabled") or "enabled"
    _podcast_progress("Unified show: inserting single video if missing")
    ensure_result = _podcast_ensure_video_in_playlist(youtube, show["playlist_id"], normalized_video_id)
    result["video_inserted"] = bool(ensure_result.get("inserted"))
    result["video_already_present"] = bool(ensure_result.get("already_present"))
    result["show_last_synced_at"] = _podcast_now_iso()
    result["show_last_error"] = ""
    _podcast_progress(
        f"Unified show sync finished: playlist_id={show['playlist_id']} inserted={result['video_inserted']} already_present={result['video_already_present']}"
    )
    return result


def _podcast_sync_split_playlist_podcast(result, state, book_record, book_name):
    playlist_state = get_split_playlist_state(state)
    playlist_id = str(playlist_state.get("playlist_id") or "").strip()
    if not playlist_id:
        raise RuntimeError("split playlist podcast sync requires playlist_id")

    channel_name = str(YOUTUBE_CHANNEL_NAME or "").strip()
    if not channel_name:
        raise RuntimeError("YOUTUBE_CHANNEL_NAME 未配置，无法同步 split podcast playlist")

    youtube = authenticate_youtube_from_supabase(channel_name)
    playlist_title = str(playlist_state.get("title") or result.playlist_title or book_name or "").strip()
    playlist_description = str(playlist_state.get("description") or result.seo_description or "").strip()
    playlist = _podcast_fetch_playlist_by_id(youtube, playlist_id, retries=8, wait_seconds=1.5)
    if playlist:
        playlist_title = str(playlist.get("title") or playlist_title).strip()
        playlist_description = str(playlist.get("description") or playlist_description)
        playlist_state["title"] = playlist_title
        playlist_state["description"] = playlist_description
        playlist_state["privacy_status"] = str(playlist.get("privacy_status") or "public")

    podcast_enabled = str((playlist or {}).get("podcast_status") or playlist_state.get("podcast_status") or "").strip().lower() == "enabled"
    existing_images = _podcast_list_playlist_images(youtube, playlist_id) if podcast_enabled else []
    image_status = _podcast_resolve_playlist_image_status(existing_images, podcast_enabled)
    has_existing_image = bool(image_status.get("has_image"))
    if str(playlist_state.get("podcast_image_status") or "").strip().lower() == "completed":
        has_existing_image = True

    if not has_existing_image:
        prompt_result = _podcast_generate_batch_playlist_cover_prompt(playlist_title, playlist_description)
        image_filename = f"{_sanitize_filename_component(playlist_id)}_podcast_cover.jpg"
        _podcast_progress(f"[{book_name}] Split playlist: generating square cover image")
        image_result = _podcast_generate_named_cover_image(
            image_filename,
            prompt_result["prompt"],
            subdir=_PODCAST_PLAYLIST_ASSET_DIR,
            cover_title=playlist_title,
            cover_subtitle="Podcast",
            thumbnail_fallback_url=str((playlist or {}).get("thumbnail_url") or ""),
            local_fallback_image_path=str(getattr(result, "cover_image_path", "") or ""),
        )
        _podcast_log_image_source(f"[{book_name}] Split playlist", image_result)
        _podcast_progress(f"[{book_name}] Split playlist: uploading podcast square image")
        _podcast_sync_playlist_image(
            youtube,
            playlist_id,
            image_result["path"],
            existing_images=existing_images if podcast_enabled else None,
            blind_insert=not podcast_enabled,
        )
        playlist_state["podcast_image_status"] = "completed"
        playlist_state["podcast_image_source"] = str(image_result.get("source") or "")
        has_existing_image = True
    else:
        playlist_state["podcast_image_status"] = "completed"
        if image_status.get("assumed"):
            playlist_state["podcast_image_source"] = str(playlist_state.get("podcast_image_source") or "existing_assumed")
        else:
            playlist_state["podcast_image_source"] = str(playlist_state.get("podcast_image_source") or "existing")

    if not podcast_enabled:
        _podcast_progress(f"[{book_name}] Split playlist: enabling podcast status")
        updated = _podcast_update_playlist(
            youtube,
            playlist_id,
            playlist_title,
            playlist_description,
            privacy_status=str(playlist_state.get("privacy_status") or "public"),
            enable_podcast=True,
        )
        if bool(updated.get("podcast_status_pending")):
            raise RuntimeError("split playlist 的 podcastStatus 请求已提交，但当前还未回显 enabled。")
        podcast_enabled = str(updated.get("podcast_status") or "").strip().lower() == "enabled"
        playlist = updated
    if not podcast_enabled:
        raise RuntimeError("split playlist 尚未成功切换为 podcast。")

    playlist_state["podcast_status"] = "enabled"
    playlist_state["podcast_last_synced_at"] = _podcast_now_iso()
    playlist_state["podcast_last_error"] = ""
    result.playlist_podcast_status = "enabled"
    result.playlist_podcast_image_status = str(playlist_state.get("podcast_image_status") or "")
    result.playlist_podcast_image_source = str(playlist_state.get("podcast_image_source") or "")
    result.playlist_podcast_last_synced_at = str(playlist_state.get("podcast_last_synced_at") or "")
    result.playlist_podcast_last_error = ""
    return {
        "playlist_id": playlist_id,
        "podcast_status": "enabled",
        "podcast_image_status": str(playlist_state.get("podcast_image_status") or ""),
        "podcast_image_source": str(playlist_state.get("podcast_image_source") or ""),
        "podcast_last_synced_at": str(playlist_state.get("podcast_last_synced_at") or ""),
    }


_PODCAST_RUNTIME_ORIGINAL_PROCESS_STANDARD_BOOK = process_standard_book
_PODCAST_RUNTIME_ORIGINAL_SYNC_SPLIT_PLAYLIST = sync_split_playlist
_PODCAST_RUNTIME_ORIGINAL_SYNC_RESULT_FROM_SPLIT_STATE = sync_result_from_split_state


def process_standard_book(result, book_record, book_data, chapters_sorted, book_dir, safe_name, book_name, category):
    result = _PODCAST_RUNTIME_ORIGINAL_PROCESS_STANDARD_BOOK(
        result,
        book_record,
        book_data,
        chapters_sorted,
        book_dir,
        safe_name,
        book_name,
        category,
    )
    if not _podcast_runtime_enabled() or not _podcast_unified_show_enabled():
        return result
    if not ENABLE_YOUTUBE_UPLOAD or not str(YOUTUBE_CHANNEL_NAME or "").strip():
        return result
    if not bool(getattr(result, "upload_ready", False)):
        return result

    upload_receipt_path = os.path.join(book_dir, "youtube_upload_receipt.json")
    receipt = load_youtube_upload_receipt(
        upload_receipt_path,
        video_path=result.video_path,
        channel_name=str(YOUTUBE_CHANNEL_NAME).strip(),
    )
    video_id = str(receipt.get("video_id") or "").strip()
    if not video_id:
        video_id = _extract_youtube_video_id(getattr(result, "youtube_url", ""))
    if not video_id:
        log.warning("[%s] 单 P 上传成功，但未能从上传回执中解析 video_id，跳过 unified podcast show 同步。", book_name)
        return result

    state = reload_split_processing_state(
        book_record,
        fallback_state=build_standard_processing_state(book_record),
        book_name=book_name,
    )
    if not isinstance(state, dict):
        state = build_standard_processing_state(book_record)
    show_state = _podcast_get_show_state_container(state)

    state["pending_resume"] = True
    state["last_stage"] = "standard_unified_show_syncing"
    state["last_error"] = ""
    state_ref = save_split_processing_state(book_record, state)
    result.state_path = state_ref

    try:
        sync_result = sync_single_video_into_unified_podcast_show(
            channel_name=str(YOUTUBE_CHANNEL_NAME).strip(),
            video_id=video_id,
            book_name=str(getattr(result, "seo_title", "") or book_name or "").strip(),
            cover_image_path=str(getattr(result, "cover_image_path", "") or ""),
        )
        show_state.update(
            {
                "show_playlist_id": str(sync_result.get("show_playlist_id") or ""),
                "show_image_source": str(sync_result.get("show_image_source") or ""),
                "show_podcast_status": str(sync_result.get("show_podcast_status") or ""),
                "show_last_synced_at": str(sync_result.get("show_last_synced_at") or _podcast_now_iso()),
                "show_last_error": "",
            }
        )
        state["pending_resume"] = False
        state["last_stage"] = "standard_unified_show_completed"
        state["last_error"] = ""
        state_ref = save_split_processing_state(book_record, state)
        result.state_path = state_ref
        result.pending_resume = False
        if str(getattr(result, "error", "") or "").startswith("Single-video unified podcast show sync failed:"):
            result.error = ""
        _podcast_apply_show_state_to_result(result, show_state)
        _podcast_progress(
            f"[{book_name}] Single-video unified show sync done: show={show_state.get('show_playlist_id') or ''} inserted={bool(sync_result.get('video_inserted'))}"
        )
        return result
    except Exception as e:
        show_state.update(
            {
                "show_playlist_id": str(show_state.get("show_playlist_id") or ""),
                "show_image_source": str(show_state.get("show_image_source") or ""),
                "show_podcast_status": str(show_state.get("show_podcast_status") or ""),
                "show_last_synced_at": str(show_state.get("show_last_synced_at") or _podcast_now_iso()),
                "show_last_error": str(e),
            }
        )
        state["pending_resume"] = True
        state["last_stage"] = "standard_unified_show_failed"
        state["last_error"] = str(e)
        state_ref = save_split_processing_state(book_record, state)
        result.state_path = state_ref
        result.pending_resume = True
        result.error = f"Single-video unified podcast show sync failed: {e}"
        _podcast_apply_show_state_to_result(result, show_state)
        _podcast_progress(f"[{book_name}] Single-video unified show sync failed: {e}")
        return result


def sync_split_playlist(result, state, split_plan, book_record, book_name):
    result = _PODCAST_RUNTIME_ORIGINAL_SYNC_SPLIT_PLAYLIST(result, state, split_plan, book_record, book_name)
    if not _podcast_runtime_enabled() or not _podcast_split_playlist_enabled():
        return result

    playlist_state = get_split_playlist_state(state)
    playlist_id = str(playlist_state.get("playlist_id") or "").strip()
    if not playlist_id:
        return result

    playlist_state["podcast_status"] = str(playlist_state.get("podcast_status") or "")
    playlist_state["podcast_image_status"] = str(playlist_state.get("podcast_image_status") or "")
    playlist_state["podcast_last_synced_at"] = str(playlist_state.get("podcast_last_synced_at") or "")
    playlist_state["podcast_last_error"] = ""
    state["pending_resume"] = True
    playlist_state["status"] = "podcast_syncing"
    state["last_stage"] = "playlist_podcast_syncing"
    state["last_error"] = ""
    state_ref = save_split_processing_state(book_record, state)
    result.state_path = state_ref

    try:
        podcast_result = _podcast_sync_split_playlist_podcast(result, state, book_record, book_name)
        playlist_state["podcast_status"] = str(podcast_result.get("podcast_status") or "enabled")
        playlist_state["podcast_image_status"] = str(podcast_result.get("podcast_image_status") or "completed")
        playlist_state["podcast_image_source"] = str(podcast_result.get("podcast_image_source") or "")
        playlist_state["podcast_last_synced_at"] = str(
            podcast_result.get("podcast_last_synced_at") or _podcast_now_iso()
        )
        playlist_state["podcast_last_error"] = ""
        playlist_state["status"] = "completed"
        playlist_state["last_error"] = ""
        playlist_state["last_synced_at"] = _podcast_now_iso()
        state["pending_resume"] = False
        state["last_stage"] = "playlist_completed"
        state["last_error"] = ""
        state_ref = save_split_processing_state(book_record, state)
        result.state_path = state_ref
        result.playlist_completed = True
        result.pending_resume = False
        result.error = ""
        return result
    except Exception as e:
        playlist_state["status"] = "failed"
        playlist_state["last_error"] = str(e)
        playlist_state["podcast_last_error"] = str(e)
        state["pending_resume"] = True
        state["last_stage"] = "playlist_failed"
        state["last_error"] = str(e)
        state_ref = save_split_processing_state(book_record, state)
        result.state_path = state_ref
        result.playlist_completed = False
        result.pending_resume = True
        result.error = str(e)
        _podcast_progress(f"[{book_name}] Split playlist podcast sync failed: {e}")
        return result


def sync_result_from_split_state(result, state, split_plan):
    result = _PODCAST_RUNTIME_ORIGINAL_SYNC_RESULT_FROM_SPLIT_STATE(result, state, split_plan)
    playlist_state = get_split_playlist_state(state)
    result.playlist_podcast_status = str(playlist_state.get("podcast_status") or "")
    result.playlist_podcast_image_status = str(playlist_state.get("podcast_image_status") or "")
    result.playlist_podcast_image_source = str(playlist_state.get("podcast_image_source") or "")
    result.playlist_podcast_last_synced_at = str(playlist_state.get("podcast_last_synced_at") or "")
    result.playlist_podcast_last_error = str(playlist_state.get("podcast_last_error") or "")
    if isinstance(state, dict):
        _podcast_apply_show_state_to_result(result, state.get("podcast_show") or {})
    return result


def finalize_book_result(result, book_dir, book_record=None):
    if bool(getattr(result, "skipped", False)):
        result.audio_ready = False
        result.video_ready = False
        result.upload_ready = False
        result.pending_resume = False
        result.success = False
        return result

    part_count = max(1, int(getattr(result, "part_count", 1) or 1))
    completed_part_count = max(0, int(getattr(result, "completed_part_count", 0) or 0))

    if getattr(result, "split_mode", False) or part_count > 1:
        playlist_required = bool(getattr(result, "playlist_required", False))
        playlist_completed = not playlist_required or bool(getattr(result, "playlist_completed", False))
        all_parts_completed = completed_part_count >= part_count

        result.audio_ready = all_parts_completed
        result.video_ready = all_parts_completed if ENABLE_VIDEO_GENERATION else result.audio_ready
        result.upload_ready = (
            all_parts_completed and (not playlist_required or playlist_completed)
            if ENABLE_YOUTUBE_UPLOAD
            else result.video_ready
        )
        computed_pending_resume = (not all_parts_completed) or (playlist_required and not playlist_completed)
        stale_pending_resume = bool(getattr(result, "pending_resume", False)) and not computed_pending_resume
        result.pending_resume = computed_pending_resume
        required_stages = [result.audio_ready]
        if ENABLE_VIDEO_GENERATION:
            required_stages.append(result.video_ready)
        if ENABLE_YOUTUBE_UPLOAD:
            required_stages.append(result.upload_ready)
        result.success = all(required_stages) and all_parts_completed and playlist_completed and not result.pending_resume
        if stale_pending_resume:
            log.warning(
                "[%s] Clearing stale pending_resume during final split evaluation. completed=%d/%d playlist_required=%s playlist_completed=%s state=%s",
                result.book_name,
                completed_part_count,
                part_count,
                playlist_required,
                playlist_completed,
                getattr(result, "state_path", ""),
            )
    else:
        result.audio_ready = bool(result.merged_audio_path and os.path.exists(result.merged_audio_path))
        result.video_ready = bool(result.video_path and os.path.exists(result.video_path))
        result.upload_ready = bool(result.youtube_url)

        required_stages = [result.audio_ready]
        if ENABLE_VIDEO_GENERATION:
            required_stages.append(result.video_ready)
        if ENABLE_YOUTUBE_UPLOAD:
            required_stages.append(result.upload_ready)

        result.success = all(required_stages) and not bool(getattr(result, "pending_resume", False))

    if not result.success and not result.error:
        if bool(getattr(result, "pending_resume", False)):
            if getattr(result, "split_mode", False) or part_count > 1:
                result.error = "长音频分片处理中断，已记录进度，等待下次续跑"
            else:
                result.error = "单 P 上传后的 podcast 后置同步尚未完成，已记录进度，等待下次续跑"
        elif not result.audio_ready:
            result.error = "音频成品未准备完成"
        elif ENABLE_VIDEO_GENERATION and not result.video_ready:
            result.error = "MP4 成品未准备完成"
        elif ENABLE_YOUTUBE_UPLOAD and not result.upload_ready:
            result.error = "YouTube 上传未完成"

    if getattr(result, "split_mode", False) and not result.success:
        log.error(
            "[%s] Split finalization failed: completed_part_count=%d part_count=%d pending_resume=%s playlist_required=%s playlist_completed=%s audio_ready=%s video_ready=%s upload_ready=%s state=%s error=%s",
            result.book_name,
            completed_part_count,
            part_count,
            bool(getattr(result, "pending_resume", False)),
            bool(getattr(result, "playlist_required", False)),
            bool(getattr(result, "playlist_completed", False)),
            bool(getattr(result, "audio_ready", False)),
            bool(getattr(result, "video_ready", False)),
            bool(getattr(result, "upload_ready", False)),
            getattr(result, "state_path", ""),
            str(getattr(result, "error", "") or ""),
        )
    elif not getattr(result, "split_mode", False) and bool(getattr(result, "pending_resume", False)):
        log.warning(
            "[%s] Standard finalization paused for podcast follow-up. audio_ready=%s video_ready=%s upload_ready=%s state=%s error=%s",
            result.book_name,
            bool(getattr(result, "audio_ready", False)),
            bool(getattr(result, "video_ready", False)),
            bool(getattr(result, "upload_ready", False)),
            getattr(result, "state_path", ""),
            str(getattr(result, "error", "") or ""),
        )

    report = {
        "generated_at": dt_module.datetime.now().isoformat(),
        "book_dir": book_dir,
        "result": dict(result.__dict__),
    }
    if book_record is not None:
        report["source"] = {
            "book_id": book_record.get("book_id"),
            "book_name": book_record.get("book_name"),
            "category": book_record.get("category"),
        }

    report_path = os.path.join(book_dir, "book_result.json")
    try:
        write_json_file(report_path, report)
    except Exception as e:
        log.warning("单书结果写入失败: %s", e)

    log.info("🏁 本书《%s》全程线走完。状态：%s", result.book_name, "✅" if result.success else "❌")
    return result
