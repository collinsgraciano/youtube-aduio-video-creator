"""YouTube API 认证与上传模块"""
from __future__ import annotations

import os
import re
import ast
import json
import time
from urllib.parse import urlparse, parse_qs

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request as GoogleAuthRequest
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError
from PIL import Image
from datetime import datetime as dt_datetime, timedelta as dt_timedelta, timezone as dt_timezone
from zoneinfo import ZoneInfo

from pipeline.config import get_config
from pipeline.log_utils import log, runtime_console_print
from pipeline.utils import normalize_text_items, sanitize_filename, write_json_file, read_json_file, make_json_compatible
from pipeline.db import execute_postgres_fetchone, execute_postgres, get_public_table_identifier
from psycopg import sql


# ============================================================================
# YouTube 认证
# ============================================================================

class MissingYouTubeCredentialsError(RuntimeError):
    """YouTube 凭证缺失时抛出"""


def authenticate_youtube_from_supabase(channel_name):
    """从数据库加载 YouTube OAuth 凭证并构建 API 客户端"""
    normalized_channel = str(channel_name or "").strip()
    if not normalized_channel:
        raise MissingYouTubeCredentialsError("YOUTUBE_CHANNEL_NAME 为空")

    try:
        table_sql = get_public_table_identifier("youtube_credentials")
        row = execute_postgres_fetchone(
            sql.SQL(
                """
                SELECT token_json, channel_name
                FROM {}
                WHERE channel_name = %s
                LIMIT 1
                """
            ).format(table_sql),
            (normalized_channel,),
        )
    except Exception as e:
        raise MissingYouTubeCredentialsError(f"读取 YouTube 凭证失败: {e}") from e

    if not row:
        raise MissingYouTubeCredentialsError(
            f"数据库 youtube_credentials 表中没有频道 [{normalized_channel}] 的授权信息。\n"
            f"请在 Colab Notebook 中先运行「初始化 YouTube 授权」单元格。"
        )

    token_json_str = str(row.get("token_json") or "").strip()
    if not token_json_str:
        raise MissingYouTubeCredentialsError(
            f"数据库 youtube_credentials 表中频道 [{normalized_channel}] 的 token_json 为空。\n"
            f"请重新运行 Colab 中的「初始化 YouTube 授权」步骤。"
        )

    try:
        token_data = json.loads(token_json_str)
    except json.JSONDecodeError:
        try:
            token_data = ast.literal_eval(token_json_str)
        except Exception:
            raise MissingYouTubeCredentialsError(
                f"token_json 格式错误，既不是合法 JSON 也不是 Python 字面量: {token_json_str[:80]}"
            )

    credentials = Credentials.from_authorized_user_info(token_data)

    if credentials and credentials.expired and credentials.refresh_token:
        try:
            credentials.refresh(GoogleAuthRequest())
        except Exception as e:
            raise MissingYouTubeCredentialsError(f"刷新 token 失败: {e}") from e

    if not credentials or not credentials.valid:
        raise MissingYouTubeCredentialsError("YouTube 凭证无效")

    youtube = build("youtube", "v3", credentials=credentials)
    return youtube


# ============================================================================
# 繁体中文化
# ============================================================================

def build_youtube_traditional_localizations(title="", description=""):
    """构建 YouTube 繁体中文本地化内容"""
    from pipeline.config import get_config

    enable_localization = get_config("ENABLE_YOUTUBE_TRADITIONAL_LOCALIZATION", True)
    default_language = str(get_config("YOUTUBE_DEFAULT_LANGUAGE", "zh-CN") or "zh-CN").strip()
    locales_config = str(get_config("YOUTUBE_LOCALIZATION_LOCALES", "zh-TW,zh-HK,zh-SG,zh-Hant") or "").strip()
    traditional_locale = str(get_config("YOUTUBE_TRADITIONAL_LOCALE", "zh-TW") or "zh-TW").strip()
    opencc_config = str(get_config("YOUTUBE_TRADITIONAL_OPENCC_CONFIG", "s2t") or "s2t").strip()
    auto_install = get_config("ENABLE_AUTO_INSTALL_OPENCC", True)

    localizations = {}
    if not enable_localization:
        return default_language, localizations

    locale_list = [loc.strip() for loc in locales_config.split(",") if loc.strip()]
    if not locale_list:
        return default_language, localizations

    try:
        if auto_install:
            try:
                from opencc import OpenCC
            except ImportError:
                log.info("正在安装 opencc-python-reimplemented...")
                import subprocess
                import sys
                subprocess.run(
                    [sys.executable, "-m", "pip", "install", "opencc-python-reimplemented", "-q"],
                    capture_output=True,
                )
                from opencc import OpenCC

        cc = OpenCC(opencc_config)
        converted_title = cc.convert(title)
        converted_desc = cc.convert(description)

        for locale in locale_list:
            localizations[locale] = {
                "title": converted_title[:100],
                "description": converted_desc[:5000],
            }
    except Exception as e:
        log.warning("繁体中文化生成失败（不影响主流程）: %s", e)

    return default_language, localizations


# ============================================================================
# YouTube 视频上传
# ============================================================================

def upload_youtube_video(
    youtube,
    video_path,
    title,
    description,
    tags,
    category_id="",
    privacy_status="public",
    publish_at=None,
    channel_name="",
    default_language="zh-CN",
    localizations=None,
):
    """上传视频到 YouTube"""
    if not os.path.exists(video_path):
        raise FileNotFoundError(f"视频文件不存在: {video_path}")

    body = {
        "snippet": {
            "title": str(title or "")[:100],
            "description": str(description or "")[:5000],
            "tags": [t.strip("#") for t in tags.split() if t.strip()],
            "defaultLanguage": str(default_language or "zh-CN"),
        },
        "status": {
            "privacyStatus": str(privacy_status or "public").strip().lower(),
            "selfDeclaredMadeForKids": False,
        },
    }

    if category_id:
        body["snippet"]["categoryId"] = str(category_id).strip()

    if localizations:
        body["localizations"] = localizations

    if privacy_status == "schedule" and publish_at:
        body["status"]["privacyStatus"] = "private"
        body["recordingDetails"] = {}
        body["status"]["publishAt"] = publish_at

    media = MediaFileUpload(video_path, chunksize=1024 * 1024 * 5, resumable=True)
    request = youtube.videos().insert(
        part=",".join(body.keys()),
        body=body,
        media_body=media,
    )

    response = None
    last_progress = 0
    while response is None:
        status, response = request.next_chunk()
        if status:
            progress = int(status.progress() * 100)
            if progress - last_progress >= 10:
                runtime_console_print(f"  上传进度: {progress}%", level="INFO")
                last_progress = progress

    video_id = response.get("id", "")
    youtube_url = f"https://youtu.be/{video_id}"

    result = {
        "video_id": video_id,
        "youtube_url": youtube_url,
        "title": title,
        "uploaded_at": dt_datetime.now(dt_timezone.utc).isoformat(),
    }

    if privacy_status == "schedule" and publish_at:
        result["publish_at"] = publish_at
        result["schedule_reason"] = f"scheduled_at_{publish_at}"

    log.info("✅ 视频已上传: %s -> %s", video_id, youtube_url)
    return result


# ============================================================================
# 上传回执管理
# ============================================================================

def _normalize_local_path_for_compare(path):
    """规范化本地路径用于比较"""
    text = str(path or "").strip()
    if not text:
        return ""
    return os.path.normcase(os.path.abspath(text))


def _capture_local_file_signature(path):
    """捕获本地文件特征用于验证"""
    normalized_path = _normalize_local_path_for_compare(path)
    signature = {"path": normalized_path}
    if not normalized_path or not os.path.exists(path):
        return signature

    stat = os.stat(path)
    signature["size"] = int(stat.st_size)
    signature["mtime_ns"] = int(getattr(stat, "st_mtime_ns", int(stat.st_mtime * 1_000_000_000)))
    return signature


def persist_youtube_upload_receipt(
    receipt_path, video_path, upload_result,
    channel_name="", title="", privacy_status="",
    category_id="", schedule_after_hours=0,
):
    """持久化 YouTube 上传回执"""
    if not isinstance(upload_result, dict):
        return ""

    youtube_url = str(upload_result.get("youtube_url") or "").strip()
    video_id = str(upload_result.get("video_id") or "").strip()
    if not youtube_url and not video_id:
        return ""

    payload = {
        "receipt_version": 1,
        "saved_at": dt_datetime.now().isoformat(),
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
    """加载 YouTube 上传回执"""
    receipt = read_json_file(receipt_path, default={}) or {}
    if not isinstance(receipt, dict) or (not receipt.get("youtube_url") and not receipt.get("video_id")):
        # 尝试从 book_result.json 中回退读取
        fallback_report = read_json_file(os.path.join(os.path.dirname(receipt_path), "book_result.json"), default={}) or {}
        fallback_result = fallback_report.get("result") if isinstance(fallback_report, dict) else {}
        if isinstance(fallback_result, dict) and (
            str(fallback_result.get("youtube_url") or "").strip()
            or str(fallback_result.get("youtube_urls") or "").strip()
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


# ============================================================================
# YouTube 数据查询
# ============================================================================

def _extract_youtube_video_id(value):
    """从 URL 或 ID 中提取 YouTube 视频 ID"""
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


def _fetch_video_status_rows_with_client(youtube, video_ids):
    """批量查询视频状态"""
    normalized_ids = []
    seen = set()
    for vid in video_ids:
        vid = str(vid or "").strip()
        if vid and vid not in seen:
            seen.add(vid)
            normalized_ids.append(vid)

    if not normalized_ids:
        return []

    rows = []
    for i in range(0, len(normalized_ids), 50):
        batch = normalized_ids[i:i + 50]
        try:
            response = youtube.videos().list(
                part="snippet,status,liveStreamingDetails",
                id=",".join(batch),
            ).execute()
            for item in response.get("items", []):
                rows.append({
                    "id": item.get("id", ""),
                    "title": str((item.get("snippet") or {}).get("title", "")),
                    "privacy_status": str((item.get("status") or {}).get("privacyStatus", "")),
                    "upload_status": str((item.get("status") or {}).get("uploadStatus", "")),
                    "published_at": str((item.get("snippet") or {}).get("publishedAt", "")),
                    "scheduled_at": _extract_scheduled_time(item),
                })
        except HttpError as e:
            log.warning("YouTube API 查询视频状态失败: %s", e)
    return rows


def _extract_scheduled_time(video_item):
    """提取视频的预定发布时间"""
    status = video_item.get("status") or {}
    publish_at = str(status.get("publishAt") or "").strip()
    if publish_at:
        return publish_at
    return ""


def _build_existing_video_match_from_row(row):
    """从查询结果构建视频匹配信息"""
    if not isinstance(row, dict):
        return {}
    return {
        "video_id": str(row.get("id") or ""),
        "youtube_url": f"https://youtu.be/{row['id']}" if row.get("id") else "",
        "title": str(row.get("title") or ""),
        "privacy_status": str(row.get("privacy_status") or ""),
        "uploaded_at": str(row.get("published_at") or ""),
        "publish_at": str(row.get("scheduled_at") or str(row.get("published_at") or "")),
    }


def _normalize_youtube_title_key(title):
    """规范化 YouTube 标题用于匹配"""
    return re.sub(r"\s+", " ", str(title or "").strip().lower())


def _build_channel_video_title_index_with_client(youtube, max_pages=10):
    """构建频道的视频标题索引"""
    title_index = {}
    page_token = None
    pages = 0

    while pages < max_pages:
        try:
            request = youtube.search().list(
                part="snippet",
                forMine=True,
                type="video",
                maxResults=50,
                pageToken=page_token,
            )
            response = request.execute()
        except HttpError as e:
            log.warning("YouTube API 搜索视频失败: %s", e)
            break

        for item in response.get("items", []):
            video_id = str((item.get("id") or {}).get("videoId", ""))
            title = str((item.get("snippet") or {}).get("title", ""))
            if video_id and title:
                title_index[_normalize_youtube_title_key(title)] = {
                    "video_id": video_id,
                    "youtube_url": f"https://youtu.be/{video_id}",
                    "title": title,
                }

        page_token = response.get("nextPageToken")
        if not page_token:
            break
        pages += 1

    return title_index


# ============================================================================
# YouTube 构建器
# ============================================================================

def build_youtube_payload(
    result, book_name, category, youtube_chapters="",
    title_prefix="", part_hint="",
    include_youtube_chapters=True, include_part_hint=True,
):
    """构建 YouTube 视频的标题、描述和标签"""
    from pipeline.config import get_config

    final_title = result.seo_title or book_name
    final_tags = result.seo_tags or category
    final_desc = result.seo_description or ""

    append_tags_to_desc = get_config("APPEND_TAGS_TO_DESC", True)
    append_tags_to_title = get_config("APPEND_TAGS_TO_TITLE", False)

    if part_hint and include_part_hint:
        final_desc = f"{part_hint}\n\n{final_desc}".strip()

    if youtube_chapters and include_youtube_chapters:
        final_desc += "\n\n精彩章节时间轴:\n" + youtube_chapters

    if append_tags_to_desc and final_tags:
        final_desc += "\n\n" + final_tags

    if append_tags_to_title and final_tags:
        some_tags = " ".join([t for t in final_tags.split() if t.startswith("#")][:2])
        if some_tags and len(final_title) + len(some_tags) < 95:
            final_title += " " + some_tags

    if title_prefix:
        final_title = f"{title_prefix}{final_title}"

    return final_title[:100], final_desc[:5000], final_tags


# ============================================================================
# 播放列表处理
# ============================================================================

def normalize_playlist_privacy_status(value):
    """规范化播放列表隐私状态"""
    normalized = str(value or "").strip().lower()
    if normalized in ("private", "public", "unlisted"):
        return normalized
    return "public"


def create_or_get_playlist(youtube, title, description="", privacy_status="public", playlist_id=""):
    """创建或获取播放列表"""
    body = {
        "snippet": {
            "title": str(title or "")[:150],
            "description": str(description or "")[:5000],
        },
        "status": {
            "privacyStatus": normalize_playlist_privacy_status(privacy_status),
        },
    }
    if playlist_id:
        body["id"] = playlist_id
        response = youtube.playlists().update(part="snippet,status", body=body).execute()
    else:
        response = youtube.playlists().insert(part="snippet,status", body=body).execute()

    return response


def add_video_to_playlist(youtube, playlist_id, video_id):
    """将视频添加到播放列表"""
    body = {
        "snippet": {
            "playlistId": playlist_id,
            "resourceId": {
                "kind": "youtube#video",
                "videoId": video_id,
            },
        },
    }
    response = youtube.playlistItems().insert(part="snippet", body=body).execute()
    return response


def _sync_playlist_localizations_with_client(youtube, playlist_id, default_language="zh-CN", localizations=None):
    """同步播放列表本地化信息"""
    if not localizations:
        return None
    try:
        youtube.playlists().update(
            part="localizations",
            body={
                "id": playlist_id,
                "localizations": localizations,
            },
        ).execute()
        return True
    except HttpError as e:
        log.warning("播放列表本地化同步失败: %s", e)
        return None


def add_videos_to_playlist_in_order(youtube, playlist_id, ordered_video_records):
    """按顺序将视频添加到播放列表"""
    added_items = []
    for record in ordered_video_records:
        video_id = str(record.get("video_id") or "").strip()
        part_index = record.get("part_index", "?")
        if not video_id:
            log.warning("播放列表添加跳过: part=%s 没有 video_id", part_index)
            continue

        try:
            result = add_video_to_playlist(youtube, playlist_id, video_id)
            item_id = (result.get("snippet") or {}).get("position", "")
            added_items.append({"part_index": part_index, "video_id": video_id, "playlist_item_id": item_id})
            log.info("  已添加到播放列表: part=%s video=%s", part_index, video_id)
        except HttpError as e:
            if e.resp.status == 409:
                log.info("  视频已在播放列表中: part=%s video=%s", part_index, video_id)
                added_items.append({"part_index": part_index, "video_id": video_id})
            else:
                log.warning("  添加视频到播放列表失败: part=%s video=%s error=%s", part_index, video_id, e)
    return added_items