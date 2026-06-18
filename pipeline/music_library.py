"""版权音乐下载模块 - Hugging Face 数据集与 Buckets 下载"""
from __future__ import annotations

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

from pipeline.config import get_config
from pipeline.log_utils import log, runtime_console_print
from pipeline.utils import parse_text_list_config, safe_music_output_path, normalize_runtime_source
from pipeline.db import execute_postgres_fetchone, get_public_table_identifier, get_postgres_dsn
from psycopg import sql


SUPPORTED_AUDIO_EXTENSIONS = (".mp3", ".wav", ".flac", ".m4a", ".ogg", ".aac", ".wma")


def load_cloud_music_runtime_setting(setting_key):
    """从数据库加载音乐相关的运行配置"""
    table_name = str(get_config("CLOUD_RUNTIME_SETTINGS_TABLE", "") or "channel_runtime_settings").strip() or "channel_runtime_settings"
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
    """解析音乐运行配置（云端优先）"""
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
    """应用音乐下载相关的云端配置覆盖"""
    from pipeline.config import get_config
    cfg = {
        "HF_DATASET_ZIP_URLS": resolve_music_runtime_setting(
            "HF_DATASET_ZIP_URLS",
            get_config("HF_DATASET_ZIP_URLS", ""),
            get_config("HF_DATASET_ZIP_URLS_SOURCE", "database"),
        ),
        "BUCKET_IDS": resolve_music_runtime_setting(
            "BUCKET_IDS",
            get_config("BUCKET_IDS", ""),
            get_config("BUCKET_IDS_SOURCE", "database"),
        ),
    }
    return cfg


def build_hf_download_headers():
    """构建 Hugging Face 下载请求头"""
    token = str(get_config("HF_TOKEN", "") or "").strip()
    if not token:
        return {}
    return {"Authorization": f"Bearer {token}"}


def normalize_hf_dataset_download_url(url):
    """规范化 Hugging Face 数据集下载 URL"""
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


def download_file_with_wget(download_url, output_path, headers=None, retries=3):
    """使用 wget 下载文件"""
    headers = headers or {}
    wget_binary = shutil.which("wget")
    if not wget_binary:
        return False

    for attempt in range(1, retries + 1):
        if os.path.exists(output_path):
            os.remove(output_path)

        cmd = [
            wget_binary,
            "-O", output_path,
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
    """使用 requests 流式下载文件"""
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


def extract_audio_files_from_zip(zip_path, output_dir, allowed_exts=None):
    """从 ZIP 中解压音频文件"""
    if allowed_exts is None:
        allowed_exts = SUPPORTED_AUDIO_EXTENSIONS
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
    """从 Hugging Face Datasets ZIP 下载音乐"""
    hf_dataset_zip_urls = get_config("HF_DATASET_ZIP_URLS", "")
    local_music_dir = get_config("LOCAL_MUSIC_DIR", "/content/music")

    url_candidates = parse_text_list_config(hf_dataset_zip_urls)
    if not url_candidates:
        runtime_console_print("⚠️ 未配置有效的 HF_DATASET_ZIP_URLS，跳过下载。", level="WARNING")
        return False

    selected_input_url = random.choice(url_candidates)
    selected_download_url = normalize_hf_dataset_download_url(selected_input_url)
    headers = build_hf_download_headers()

    os.makedirs(local_music_dir, exist_ok=True)

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

        extracted = extract_audio_files_from_zip(archive_path, local_music_dir)
        if not extracted:
            raise RuntimeError("ZIP 下载成功，但解压后未找到任何支持的音频文件")

        runtime_console_print(f"✅ Datasets ZIP 下载并解压完成，共导入 {len(extracted)} 个音频文件到 {local_music_dir}", level="INFO")
        return True
    except Exception as e:
        runtime_console_print(f"❌ Datasets ZIP 下载失败: {e}", level="ERROR")
        return False
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def download_music_from_buckets():
    """从 Hugging Face Buckets 下载音乐"""
    from huggingface_hub import list_bucket_tree, download_bucket_files, login

    bucket_ids = get_config("BUCKET_IDS", "")
    local_music_dir = get_config("LOCAL_MUSIC_DIR", "/content/music")
    hf_token = get_config("HF_TOKEN", "")

    bucket_list = [b.strip() for b in bucket_ids.split(",") if b.strip()]
    if not bucket_list or bucket_list[0].startswith("username/my-bucket"):
        runtime_console_print("⚠️ 未配置有效的 BUCKET_IDS，跳过下载。", level="WARNING")
        return False

    selected_bucket = random.choice(bucket_list)
    runtime_console_print(f"🎲 已随机选择 Bucket: {selected_bucket}", level="INFO")

    if hf_token.strip():
        runtime_console_print("🔑 正在使用 Token 登录 Hugging Face...", level="INFO")
        login(token=hf_token.strip())

    os.makedirs(local_music_dir, exist_ok=True)

    try:
        runtime_console_print(f"🔍 正在检索 Bucket {selected_bucket} 中的音频文件...", level="INFO")
        music_files = [
            item for item in list_bucket_tree(selected_bucket, recursive=True)
            if item.type == "file" and item.path.lower().endswith(SUPPORTED_AUDIO_EXTENSIONS)
        ]

        if not music_files:
            runtime_console_print(f"⚠️ 在 Bucket '{selected_bucket}' 中未找到任何音频文件。", level="WARNING")
            return False

        runtime_console_print(f"⬇️ 发现 {len(music_files)} 首音乐，开始下载到 {local_music_dir}...", level="INFO")
        download_bucket_files(
            selected_bucket,
            files=[(f, safe_music_output_path(local_music_dir, f.path)) for f in music_files],
        )
        runtime_console_print("✅ Hugging Face Buckets 版权音乐同步完成！", level="INFO")
        return True
    except Exception as e:
        runtime_console_print(f"❌ Buckets 下载失败，请检查 Bucket 名称、路径或 Token: {e}", level="ERROR")
        return False


def sync_music_library_if_enabled():
    """如果启用，同步音乐库"""
    from pipeline.config import get_config

    download_from_buckets = get_config("DOWNLOAD_FROM_BUCKETS", True)
    hf_music_download_method = get_config("HF_MUSIC_DOWNLOAD_METHOD", "datasets_zip_urls")

    # 应用云端配置覆盖
    overrides = apply_music_download_runtime_overrides()

    if download_from_buckets:
        selected_method = str(hf_music_download_method or "datasets_zip_urls").strip().lower()
        if selected_method == "buckets":
            return download_music_from_buckets()
        return download_music_from_dataset_urls()

    runtime_console_print("⏭️ 已关闭版权音乐自动同步。", level="INFO")
    return False