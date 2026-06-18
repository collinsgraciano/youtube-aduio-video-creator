"""核心主流程模块 - 书籍处理流水线编排"""
from __future__ import annotations

import os
import re
import json
import csv
import time
import math
import random
import shutil
import traceback
import base64
import datetime as dt_module
from dataclasses import dataclass, field
from urllib.parse import urlparse, parse_qs

from pipeline.config import get_config, apply_runtime_config
from pipeline.log_utils import log, runtime_console_print, clear_runtime_output_if_needed
from pipeline.db import (
    execute_postgres_fetchone, execute_postgres_fetchall, execute_postgres,
    execute_postgres_fetchval, get_public_table_identifier, get_postgres_dsn,
)
from pipeline.utils import (
    sanitize_filename, normalize_text_items, make_json_compatible,
    append_unique_text_items, build_supabase_text_update,
    write_json_file, read_json_file, format_seconds_hhmmss,
    normalize_runtime_source, download_file,
)
from pipeline.modelscope import (
    resolve_modelscope_token, build_modelscope_token_pool_bundle,
    CoverGenerationPolicyRejectedError,
    auto_create_youtube_cover, auto_create_youtube_seo,
    _is_nonempty_local_file, _persist_cover_fallback_image,
)
from pipeline.youtube import (
    authenticate_youtube_from_supabase, load_youtube_upload_receipt,
    persist_youtube_upload_receipt, build_youtube_payload,
    build_youtube_traditional_localizations, MissingYouTubeCredentialsError,
    upload_youtube_video,
)
from pipeline.state import (
    load_split_processing_state, save_split_processing_state,
    delete_split_processing_state, initialize_split_processing_state,
    get_split_part_state, get_split_shared_assets, get_split_playlist_state,
    evaluate_split_completion_state, reconcile_split_part_upload_states,
    build_split_state_ref, get_book_state_table_name,
    list_interrupted_book_states,
)
from pipeline.audio import (
    download_chapter_items, build_split_part_plans, build_final_audio_from_chapter_paths,
    get_explicit_total_book_duration_seconds, get_explicit_chapter_duration_seconds,
    estimate_chapter_duration_seconds, parse_duration_to_seconds,
    denoise_audio_paths_parallel, generate_video, merge_audio_ffmpeg,
    mix_with_bgm, generate_youtube_timestamps,
)
from pipeline.constants import MIN_BOOK_DURATION_SECONDS
from pipeline.music_library import sync_music_library_if_enabled
from pipeline.podcast import sync_split_playlist_podcast
from psycopg import sql as psycopg_sql


# ============================================================================
# 配置校验
# ============================================================================

def validate_runtime_config():
    """校验运行时配置的合法性"""
    errors = []
    warnings = []

    ai_features_enabled = bool(get_config("ENABLE_COVER_GENERATION", True) or get_config("ENABLE_SEO_GENERATION", True))
    modelscope_token_source = normalize_runtime_source(get_config("MODELSCOPE_TOKEN_SOURCE", "database"), default="database")
    local_modelscope_token = str(get_config("MODELSCOPE_TOKEN", "") or "").strip()
    hf_dataset_zip_urls_source = normalize_runtime_source(get_config("HF_DATASET_ZIP_URLS_SOURCE", "database"), default="database")
    bucket_ids_source = normalize_runtime_source(get_config("BUCKET_IDS_SOURCE", "database"), default="database")
    download_from_buckets = get_config("DOWNLOAD_FROM_BUCKETS", True)
    music_download_method = str(get_config("HF_MUSIC_DOWNLOAD_METHOD", "datasets_zip_urls")).strip().lower()
    enable_bgm_mix = get_config("ENABLE_BGM_MIX", True)
    music_dir = str(get_config("MUSIC_DIR", "") or "").strip()
    enable_youtube_upload = get_config("ENABLE_YOUTUBE_UPLOAD", True)
    youtube_channel_name = str(get_config("YOUTUBE_CHANNEL_NAME", "") or "").strip()
    output_root = str(get_config("OUTPUT_ROOT", "") or "").strip()
    book_state_table = str(get_config("BOOK_STATE_TABLE", "") or "").strip()
    cloud_runtime_settings_table = str(get_config("CLOUD_RUNTIME_SETTINGS_TABLE", "") or "").strip()
    modelscope_token_table = str(get_config("MODELSCOPE_TOKEN_TABLE", "") or "").strip()
    youtube_privacy_status = str(get_config("YOUTUBE_PRIVACY_STATUS", "") or "").strip()
    youtube_schedule_after_hours = get_config("YOUTUBE_SCHEDULE_AFTER_HOURS", 24)
    hf_dataset_zip_urls = str(get_config("HF_DATASET_ZIP_URLS", "") or "").strip()
    bucket_ids = str(get_config("BUCKET_IDS", "") or "").strip()
    max_runtime_hours = get_config("MAX_RUNTIME_HOURS", 11.5)
    split_trigger_hours = float(get_config("LONG_AUDIO_SPLIT_TRIGGER_HOURS", 12.0))
    part_target_hours = float(get_config("LONG_AUDIO_PART_TARGET_HOURS", 11.8))
    audio_connect_timeout = int(get_config("AUDIO_DOWNLOAD_CONNECT_TIMEOUT", 20) or 0)
    audio_read_timeout = int(get_config("AUDIO_DOWNLOAD_READ_TIMEOUT", 90) or 0)
    audio_max_attempts = int(get_config("AUDIO_DOWNLOAD_MAX_RETRY_ATTEMPTS", 12) or 0)
    audio_max_total_seconds = int(get_config("AUDIO_DOWNLOAD_MAX_TOTAL_SECONDS", 1800) or 0)
    audio_stuck_log_interval = int(get_config("AUDIO_DOWNLOAD_STUCK_LOG_INTERVAL_SECONDS", 30) or 0)

    if not get_postgres_dsn(optional=True):
        errors.append("POSTGRES_DSN 为空")
    if not output_root:
        errors.append("OUTPUT_ROOT 为空")
    if not book_state_table:
        errors.append("BOOK_STATE_TABLE 为空")
    if not cloud_runtime_settings_table:
        errors.append("CLOUD_RUNTIME_SETTINGS_TABLE 为空")
    try:
        runtime_hours = float(max_runtime_hours or 0)
    except Exception:
        runtime_hours = 0
    if runtime_hours >= 12:
        warnings.append("Colab 单次常见上限约 12 小时，建议 MAX_RUNTIME_HOURS 小于 12，给收尾留缓冲")
    if split_trigger_hours <= 0:
        errors.append("LONG_AUDIO_SPLIT_TRIGGER_HOURS 必须大于 0")
    if part_target_hours <= 0:
        errors.append("LONG_AUDIO_PART_TARGET_HOURS 必须大于 0")
    if enable_youtube_upload and not youtube_channel_name:
        errors.append("已开启 YouTube 上传，但 YOUTUBE_CHANNEL_NAME 为空")
    if output_root.startswith("/content") and "/drive/" not in output_root:
        warnings.append("当前 OUTPUT_ROOT 位于 Colab 临时盘，断线或重启后文件会丢；长期自用更建议改到 Google Drive 路径")
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

    if download_from_buckets:
        if hf_dataset_zip_urls_source not in {"database", "local"}:
            errors.append("HF_DATASET_ZIP_URLS_SOURCE 只能是 database 或 local")
        if bucket_ids_source not in {"database", "local"}:
            errors.append("BUCKET_IDS_SOURCE 只能是 database 或 local")
        if music_download_method not in {"datasets_zip_urls", "buckets"}:
            errors.append("HF_MUSIC_DOWNLOAD_METHOD 只能是 datasets_zip_urls 或 buckets")
        elif music_download_method == "datasets_zip_urls":
            if hf_dataset_zip_urls_source == "local" and not hf_dataset_zip_urls:
                warnings.append("已开启 Hugging Face 音乐下载，但 HF_DATASET_ZIP_URLS 为空")
        else:
            bucket_id_list = [x.strip() for x in bucket_ids.split(",") if x.strip()]
            if bucket_ids_source == "local" and not bucket_id_list:
                warnings.append("已选择 buckets 下载模式，但 BUCKET_IDS 为空")

    if enable_bgm_mix and not download_from_buckets:
        if not music_dir or not os.path.exists(music_dir):
            warnings.append("已开启 BGM 混音，但本地 MUSIC_DIR 不存在")

    if ai_features_enabled:
        if modelscope_token_source not in {"database", "local"}:
            errors.append("MODELSCOPE_TOKEN_SOURCE 只能是 database 或 local")
        if not modelscope_token_table:
            errors.append("启用 AI 生成时，MODELSCOPE_TOKEN_TABLE 不能为空")
        if modelscope_token_source == "local" and not local_modelscope_token:
            errors.append("MODELSCOPE_TOKEN_SOURCE=local，但 MODELSCOPE_TOKEN 为空")

    if youtube_privacy_status.lower() == "schedule":
        try:
            hours = int(youtube_schedule_after_hours or 0)
        except Exception:
            hours = 0
        if hours <= 0:
            warnings.append("YOUTUBE_PRIVACY_STATUS=schedule 但预约小时数不大于 0，将回退到最小值 1")

    for msg in warnings:
        log.warning("配置提醒：%s", msg)

    if errors:
        raise ValueError("；".join(errors))

    log.info("配置校验通过")


# ============================================================================
# 云端配置覆盖
# ============================================================================

def apply_cloud_runtime_overrides():
    """应用云端运行配置覆盖"""
    from pipeline.modelscope import load_cloud_runtime_setting_from_supabase
    from pipeline.music_library import apply_music_download_runtime_overrides

    overrides = {}

    music_overrides = apply_music_download_runtime_overrides()
    overrides.update(music_overrides)

    if not get_postgres_dsn(optional=True):
        return overrides

    channel_name = str(get_config("YOUTUBE_CHANNEL_NAME", "") or "").strip()
    if not channel_name:
        return overrides

    setting_key = get_config("YOUTUBE_PODCAST_SHOW_PLAYLIST_SETTING_KEY", "podcast_longform_show_playlist_id")
    try:
        cloud_playlist_id = load_cloud_runtime_setting_from_supabase(channel_name, setting_key)
        if cloud_playlist_id:
            overrides["podcast_show_playlist_id_from_cloud"] = cloud_playlist_id
    except Exception as e:
        log.warning("读取云端 Playlist ID 失败: %s", e)

    return overrides


# ============================================================================
# 配置快照
# ============================================================================

def collect_runtime_config_snapshot():
    """收集运行时配置快照"""
    snapshot = {
        "postgres_dsn_configured": bool(get_postgres_dsn(optional=True)),
        "enable_cover_generation": get_config("ENABLE_COVER_GENERATION", True),
        "enable_seo_generation": get_config("ENABLE_SEO_GENERATION", True),
        "modelscope_token_source": normalize_runtime_source(get_config("MODELSCOPE_TOKEN_SOURCE", "database"), default="database"),
        "modelscope_token_table": get_config("MODELSCOPE_TOKEN_TABLE", ""),
        "cloud_runtime_settings_table": get_config("CLOUD_RUNTIME_SETTINGS_TABLE", ""),
        "hf_dataset_zip_urls_source": normalize_runtime_source(get_config("HF_DATASET_ZIP_URLS_SOURCE", "database"), default="database"),
        "bucket_ids_source": normalize_runtime_source(get_config("BUCKET_IDS_SOURCE", "database"), default="database"),
        "youtube_channel_name": get_config("YOUTUBE_CHANNEL_NAME", ""),
        "youtube_privacy_status": get_config("YOUTUBE_PRIVACY_STATUS", "schedule"),
        "youtube_schedule_after_hours": get_config("YOUTUBE_SCHEDULE_AFTER_HOURS", 24),
        "youtube_schedule_local_timezone": "Asia/Shanghai",
        "youtube_daily_publish_limit": get_config("YOUTUBE_DAILY_PUBLISH_LIMIT", 3),
    }
    return snapshot


# ============================================================================
# 运行时时间管理
# ============================================================================

def get_remaining_runtime_seconds(run_started_at):
    """获取剩余运行时间（秒）"""
    try:
        budget_hours = float(get_config("MAX_RUNTIME_HOURS", 0) or 0)
    except Exception:
        budget_hours = 0

    if budget_hours <= 0:
        return None

    return budget_hours * 3600 - (time.time() - run_started_at)


def should_stop_before_next_book(run_started_at):
    """检查是否应在处理下一本书前停止"""
    remaining = get_remaining_runtime_seconds(run_started_at)
    if remaining is None:
        return False, None

    try:
        buffer_seconds = max(0, int(get_config("STOP_BUFFER_MINUTES", 20) or 0) * 60)
    except Exception:
        buffer_seconds = 0

    return remaining <= buffer_seconds, remaining


# ============================================================================
# 运行汇总
# ============================================================================

def save_run_summary(output_root, results, archive=True, extra=None):
    """保存运行汇总"""
    report_dir = os.path.join(output_root, "_run_reports")
    timestamp = dt_module.datetime.now().strftime("%Y%m%d_%H%M%S")
    success_items = [r for r in results if r.success]
    partial_items = [r for r in results if getattr(r, "pending_resume", False)]
    skipped_items = [r for r in results if getattr(r, "skipped", False)]
    failed_items = [
        r for r in results
        if not r.success and not getattr(r, "pending_resume", False) and not getattr(r, "skipped", False)
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
            {"book_id": r.book_id, "book_name": r.book_name, "youtube_url": r.youtube_url,
             "publish_at": getattr(r, "youtube_publish_at", ""),
             "schedule_reason": getattr(r, "youtube_schedule_reason", ""),
             "video_path": r.video_path}
            for r in success_items
        ],
        "partial_items": [
            {"book_id": r.book_id, "book_name": r.book_name, "error": r.error,
             "state_ref": getattr(r, "state_path", ""),
             "completed_part_count": getattr(r, "completed_part_count", 0),
             "part_count": getattr(r, "part_count", 1)}
            for r in partial_items
        ],
        "skipped_items": [
            {"book_id": r.book_id, "book_name": r.book_name,
             "reason": getattr(r, "skipped_reason", "") or r.error,
             "deleted_from_books": bool(getattr(r, "deleted_from_books", False))}
            for r in skipped_items
        ],
        "failed_items": [
            {"book_id": r.book_id, "book_name": r.book_name, "error": r.error}
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
        log.info("运行汇总已写入: %s", archive_path)
        return archive_path

    log.info("运行进度已更新: %s", latest_path)
    return latest_path


# ============================================================================
# 数据库操作（书籍相关）
# ============================================================================

def _fetch_books_page_from_database(offset, page_size):
    """从数据库分页获取书籍列表"""
    table_sql = get_public_table_identifier("books")
    statement = psycopg_sql.SQL(
        "SELECT book_id, book_name, category, book_data, status, tags FROM {}"
    ).format(table_sql)
    params = []
    target_category = str(get_config("TARGET_CATEGORY", "") or "").strip()
    if target_category:
        statement += psycopg_sql.SQL(" WHERE category = %s")
        params.append(target_category)
    statement += psycopg_sql.SQL(" ORDER BY book_id LIMIT %s OFFSET %s")
    params.extend([page_size, offset])
    return execute_postgres_fetchall(statement, tuple(params))


def _update_book_status_in_database(book_id, status_value):
    """更新数据库中书籍的状态"""
    table_sql = get_public_table_identifier("books")
    execute_postgres(
        psycopg_sql.SQL("UPDATE {} SET status = %s WHERE book_id = %s").format(table_sql),
        (status_value, str(book_id)),
    )


def _update_book_tags_in_database(book_id, tags_value):
    """更新数据库中书籍的标签（tags 列为 text[] 类型）"""
    # 将逗号分隔的字符串转为 PostgreSQL text[] 数组字面量 {"a","b"}
    if isinstance(tags_value, str):
        tags_list = [t.strip() for t in tags_value.split(",") if t.strip()]
        pg_array = "{" + ",".join(tags_list) + "}"
    else:
        pg_array = tags_value
    table_sql = get_public_table_identifier("books")
    execute_postgres(
        psycopg_sql.SQL("UPDATE {} SET tags = %s WHERE book_id = %s").format(table_sql),
        (pg_array, str(book_id)),
    )


def _delete_book_from_database(book_id):
    """从数据库删除书籍"""
    table_sql = get_public_table_identifier("books")
    execute_postgres(
        psycopg_sql.SQL("DELETE FROM {} WHERE book_id = %s").format(table_sql),
        (str(book_id),),
    )


def finalize_successful_book_for_project(book_record, result, book_name, flag):
    """标记书籍为已处理完成"""
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
            log.error("[%s] books.status updated, but deleting book_processing_states failed: %s", book_name, e)

    return True

# ============================================================================
# BookResult 数据类
# ============================================================================

@dataclass
class BookResult:
    """单本书的处理结果"""
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
    playlist_podcast_status: str = ""
    playlist_podcast_image_status: str = ""
    playlist_podcast_image_source: str = ""
    playlist_podcast_last_synced_at: str = ""
    playlist_podcast_last_error: str = ""


# ============================================================================
# 封面与 SEO 准备
# ============================================================================

def prepare_book_cover_and_seo(result, book_data, book_dir, safe_name, book_name):
    """准备书籍封面和 SEO 文案"""
    ai_cover_target_path = os.path.join(book_dir, f"{safe_name}_cover.jpg")
    seo_path_ai = os.path.join(book_dir, f"{safe_name}_seo_description.json")
    ai_cover_ready = bool(
        result.cover_image_path and os.path.exists(result.cover_image_path)
        and os.path.getsize(result.cover_image_path) > 0
        and os.path.abspath(result.cover_image_path) == os.path.abspath(ai_cover_target_path)
    )
    seo_ready = bool(
        result.seo_text_path and os.path.exists(result.seo_text_path)
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

    enable_cover_generation = get_config("ENABLE_COVER_GENERATION", True)
    enable_seo_generation = get_config("ENABLE_SEO_GENERATION", True)
    skip_existing = get_config("SKIP_EXISTING", True)
    video_resolution = get_config("VIDEO_RESOLUTION", "1080p")
    youtube_channel_name = str(get_config("YOUTUBE_CHANNEL_NAME", "") or "").strip()

    if enable_cover_generation and not ai_cover_ready and skip_existing and os.path.exists(ai_cover_target_path) and os.path.getsize(ai_cover_target_path) > 0:
        result.cover_image_path = ai_cover_target_path
        ai_cover_ready = True
        cover_ready = True
        log.info("[%s] 复用已生成的 AI 封面。", book_name)

    if enable_seo_generation and not seo_ready and skip_existing and os.path.exists(seo_path_ai) and os.path.getsize(seo_path_ai) > 0:
        seo_dict = read_json_file(seo_path_ai, default={}) or {}
        if isinstance(seo_dict, dict):
            result.seo_text_path = seo_path_ai
            result.seo_title = seo_dict.get("title", "")
            result.seo_description = seo_dict.get("Description", "")
            result.seo_tags = seo_dict.get("label", "")
            seo_ready = True
            log.info("[%s] 复用已生成的 SEO 文案。", book_name)

    needs_modelscope_token = (enable_cover_generation and not ai_cover_ready) or (enable_seo_generation and not seo_ready)
    token_pool = {}
    if needs_modelscope_token:
        resolved_token = resolve_modelscope_token(youtube_channel_name)
        token_pool = build_modelscope_token_pool_bundle(resolved_token, shuffle_once=True)
        if not any(token_pool.values()):
            raise RuntimeError("未能解析出可用的 ModelScope Token，无法继续 AI 生成。")

    if enable_cover_generation and not ai_cover_ready:
        book_desc_text = str(book_data.get("keyWord", "")) + " " + str(book_data.get("bookDescription", ""))
        try:
            ok_cover = auto_create_youtube_cover(book_name, book_desc_text, ai_cover_target_path, token_pool, video_resolution)
        except CoverGenerationPolicyRejectedError as e:
            if not _is_nonempty_local_file(fallback_cover_path):
                raise RuntimeError("AI 封面命中提供商审核拒绝，且 books 数据中没有可用封面可回退，停止后续处理。") from e
            result.cover_image_path = _persist_cover_fallback_image(fallback_cover_path, ai_cover_target_path)
            cover_ready = _is_nonempty_local_file(result.cover_image_path)
            ai_cover_ready = os.path.abspath(result.cover_image_path) == os.path.abspath(ai_cover_target_path) and cover_ready
            log.warning("[%s] AI 封面命中提供商审核拒绝，已改用 books 数据封面。", book_name)
            ok_cover = True
        if not ok_cover:
            raise RuntimeError("AI 封面生成未成功，停止后续处理。")
        if _is_nonempty_local_file(ai_cover_target_path):
            result.cover_image_path = ai_cover_target_path
            ai_cover_ready = True
            cover_ready = True

    if enable_seo_generation and not seo_ready:
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
    if enable_cover_generation and not cover_ready:
        raise RuntimeError("已开启 AI 封面生成，但封面既未生成成功，也没有可用的 books 封面可回退，停止后续处理。")

    if enable_seo_generation and not seo_ready:
        raise RuntimeError("已开启 SEO 生成，但文案未生成成功，停止后续处理。")

    return result


# ============================================================================
# 共享资源管理（用于分片模式）
# ============================================================================

def restore_split_shared_assets_from_state(result, state, book_dir, safe_name, book_name):
    """从状态中恢复共享资源"""
    shared = get_split_shared_assets(state)
    restored_items = []

    seo_title = str(shared.get("seo_title") or "").strip()
    seo_description = str(shared.get("seo_description") or "")
    seo_tags = str(shared.get("seo_tags") or "")
    if seo_title or seo_description or seo_tags:
        seo_path = os.path.join(book_dir, f"{safe_name}_seo_description.json")
        seo_dict = {"title": seo_title, "Description": seo_description, "label": seo_tags}
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
    """将共享资源持久化到状态"""
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
    """构建标准处理状态"""
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
    """为标准书籍准备封面和 SEO 并保存状态"""
    state = build_standard_processing_state(book_record)
    restore_split_shared_assets_from_state(result, state, book_dir, safe_name, book_name)
    prepare_book_cover_and_seo(result, book_data, book_dir, safe_name, book_name)
    state["last_stage"] = "standard_shared_assets_ready"
    state["last_error"] = ""
    state_ref = persist_split_shared_assets_to_state(book_record, state, result, book_dir, safe_name, book_name)
    result.state_path = state_ref
    return state_ref, state


# ============================================================================
# 跳过和删除短书籍
# ============================================================================

def skip_and_delete_short_book(book_record, result, book_name):
    """跳过并删除短书籍"""
    result.skipped = True
    result.skipped_reason = f"书籍预估总时长不足 {MIN_BOOK_DURATION_SECONDS // 60} 分钟，已标记 bad 并从待处理队列移除。"
    result.error = result.skipped_reason
    result.deleted_from_books = False

    try:
        existing_tags = normalize_text_items(book_record.get("tags"))
        if "bad" not in existing_tags:
            new_tags = build_supabase_text_update(book_record.get("tags"), ["bad"], prefer="string")
            _update_book_tags_in_database(book_record["book_id"], new_tags)
            book_record["tags"] = new_tags
            result.deleted_from_books = True
            log.info("[%s] 时长过短（< %d 分钟），已标记为 bad。", book_name, MIN_BOOK_DURATION_SECONDS // 60)
    except Exception as e:
        log.warning("[%s] 标记 bad 失败: %s", book_name, e)


# ============================================================================
# 分片结果同步
# ============================================================================

def sync_result_from_split_state(result, state, split_plan):
    """从分片状态同步处理结果到 result"""
    progress = evaluate_split_completion_state(state)
    result.part_count = max(1, progress["part_count"])
    result.completed_part_count = progress["completed_part_count"]
    result.playlist_required = progress["playlist_required"]
    result.playlist_completed = progress["playlist_completed"]

    if state.get("parts"):
        for part in state["parts"]:
            if isinstance(part, dict) and str(part.get("status") or "").strip().lower() == "completed":
                vid = str(part.get("video_id") or "").strip()
                yurl = str(part.get("youtube_url") or "").strip()
                if vid:
                    if not result.youtube_url:
                        result.youtube_url = yurl or f"https://youtu.be/{vid}"
                    if yurl and yurl not in result.youtube_urls:
                        result.youtube_urls.append(yurl)
                if yurl and not result.youtube_publish_at:
                    result.youtube_publish_at = str(part.get("publish_at") or "").strip()
                    result.youtube_schedule_reason = str(part.get("schedule_reason") or "").strip()

    playlist_state = get_split_playlist_state(state)
    result.playlist_id = str(playlist_state.get("playlist_id") or "").strip()
    result.playlist_url = str(playlist_state.get("playlist_url") or "").strip()
    result.playlist_title = str(playlist_state.get("playlist_title") or "").strip()

    return result


def finalize_book_result(result, book_dir, book_record=None):
    """最终确定书籍处理结果"""
    if bool(getattr(result, "skipped", False)):
        result.audio_ready = False
        result.video_ready = False
        result.upload_ready = False
        result.pending_resume = False
        result.success = False
        return result

    part_count = max(1, int(getattr(result, "part_count", 1) or 1))
    completed_part_count = max(0, int(getattr(result, "completed_part_count", 0) or 0))

    enable_video_generation = get_config("ENABLE_VIDEO_GENERATION", True)
    enable_youtube_upload = get_config("ENABLE_YOUTUBE_UPLOAD", True)

    if getattr(result, "split_mode", False) or part_count > 1:
        playlist_required = bool(getattr(result, "playlist_required", False))
        playlist_completed = not playlist_required or bool(getattr(result, "playlist_completed", False))
        all_parts_completed = completed_part_count >= part_count

        result.audio_ready = all_parts_completed
        result.video_ready = all_parts_completed if enable_video_generation else result.audio_ready
        result.upload_ready = (
            all_parts_completed and (not playlist_required or playlist_completed)
            if enable_youtube_upload
            else result.video_ready
        )
        computed_pending_resume = (not all_parts_completed) or (playlist_required and not playlist_completed)
        result.pending_resume = computed_pending_resume
        required_stages = [result.audio_ready]
        if enable_video_generation:
            required_stages.append(result.video_ready)
        if enable_youtube_upload:
            required_stages.append(result.upload_ready)
        result.success = all(required_stages) and all_parts_completed and playlist_completed and not result.pending_resume
    else:
        result.audio_ready = bool(result.merged_audio_path and os.path.exists(result.merged_audio_path))
        result.video_ready = bool(result.video_path and os.path.exists(result.video_path))
        result.upload_ready = bool(result.youtube_url)

        required_stages = [result.audio_ready]
        if enable_video_generation:
            required_stages.append(result.video_ready)
        if enable_youtube_upload:
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
        elif enable_video_generation and not result.video_ready:
            result.error = "MP4 成品未准备完成"
        elif enable_youtube_upload and not result.upload_ready:
            result.error = "YouTube 上传未完成"

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

    log.info("本书《%s》全程线走完。状态：%s", result.book_name, "成功" if result.success else "失败")
    return result


# ============================================================================
# 标准书籍处理
# ============================================================================

def process_standard_book(book_record, book_data, chapters_data, book_dir, safe_name, book_name, youtube, book_result=None):
    """处理标准长度的书籍"""
    if book_result is None:
        book_result = BookResult(
            book_id=str(book_record.get("book_id", "")),
            book_name=book_name,
            category=str(book_record.get("category", "") or ""),
            chapter_count=len(chapters_data),
        )
    result = book_result

    audio_type = get_config("INTERMEDIATE_AUDIO_TYPE", "wav")
    enable_video_generation = get_config("ENABLE_VIDEO_GENERATION", True)
    enable_youtube_upload = get_config("ENABLE_YOUTUBE_UPLOAD", True)
    enable_bgm_mix = get_config("ENABLE_BGM_MIX", True)
    video_resolution = get_config("VIDEO_RESOLUTION", "1080p")
    skip_existing = get_config("SKIP_EXISTING", True)
    use_poetry_audio = bool(get_config("POETRY_CHAPTER_SEGMENT_SECONDS", 0) > 0)

    final_path = build_final_audio_from_chapter_paths(book_dir, safe_name, chapters_data)
    if final_path and os.path.isfile(final_path):
        result.merged_audio_path = final_path
        log.info("[%s] 复用现成音频: %s", book_name, os.path.basename(final_path))
    else:
        if not chapters_data:
            log.warning("[%s] chapters_data 为空，跳过章节下载，仅复用已有音频。", book_name)
            result.error = "chapters_data 为空或无效，且不存在可复用的成品音频"
            return finalize_book_result(result, book_dir, book_record)

        ok_download = download_chapter_items(
            chapters_data,
            book_dir,
            safe_name,
            audio_type=audio_type,
            book_name=book_name,
            allow_skip_existing=skip_existing,
        )
        if not ok_download:
            result.error = "所有章节下载失败"
            return finalize_book_result(result, book_dir, book_record)

        result.success_count = ok_download

        try:
            denoised = denoise_audio_paths_parallel(
                book_dir, safe_name, file_format=audio_type, book_name=book_name,
                use_poetry_audio=use_poetry_audio,
            )
        except Exception as e:
            log.warning("[%s] DeepFilter 降噪失败: %s", book_name, e)
            denoised = []

        try:
            merged = merge_audio_ffmpeg(
                book_dir, safe_name, file_format=audio_type, book_name=book_name,
                use_poetry_audio=use_poetry_audio,
            )
        except Exception as e:
            merged = None

        if merged:
            result.merged_audio_path = merged
            if enable_bgm_mix:
                sync_music_library_if_enabled()
                mixed = mix_with_bgm(merged, book_dir, safe_name, book_name=book_name, audio_type=audio_type)
                if mixed:
                    result.mixed_audio_path = mixed
            if enable_video_generation and result.cover_image_path:
                video_path = generate_video(result, book_dir, safe_name, book_name=book_name, video_resolution=video_resolution)
                if video_path and os.path.isfile(video_path):
                    result.video_path = video_path
                    result.video_ready = True
                    log.info("[%s] 复用已封装的 MP4 成品。", book_name)
                else:
                    result.video_path = video_path or ""

    if enable_video_generation and result.cover_image_path and not result.video_path:
        try:
            video_path = generate_video(result, book_dir, safe_name, book_name=book_name, video_resolution=video_resolution)
            result.video_path = video_path or ""
            if video_path and os.path.isfile(video_path):
                log.info("[%s] 已封装为 MP4 成品。", book_name)
            else:
                log.warning("[%s] MP4 封装失败，本次仅保留音频成品。", book_name)
        except Exception as e:
            log.error("[%s] MP4 封装发生异常: %s", book_name, e)
    elif enable_video_generation and not result.cover_image_path:
        log.warning("[%s] 缺少音频或封面，跳过 MP4 封装。", book_name)

    if enable_video_generation and not enable_youtube_upload:
        result.upload_ready = True

    if enable_youtube_upload and result.video_path:
        try:
            upload_ok = upload_youtube_video(result, youtube, book_name=book_name, book_dir=book_dir)
            if upload_ok:
                log.info("[%s] 复用已上传回执: %s", book_name, result.youtube_url)
        except Exception as e:
            log.error("[%s] YouTube 上传失败: %s", book_name, e)
            result.error = f"YouTube 上传失败: {e}"
            return finalize_book_result(result, book_dir, book_record)

    if enable_youtube_upload and not result.video_path:
        log.warning("[%s] 没有可上传的视频文件。", book_name)

    return finalize_book_result(result, book_dir, book_record)


# ============================================================================
# 分片书籍处理
# ============================================================================

def process_split_book(book_record, book_data, chapters_data, book_dir, safe_name, book_name, youtube, book_result=None):
    """处理分片模式的长音频书籍"""
    if book_result is None:
        book_result = BookResult(
            book_id=str(book_record.get("book_id", "")),
            book_name=book_name,
            category=str(book_record.get("category", "") or ""),
            chapter_count=len(chapters_data),
            split_mode=True,
        )
    result = book_result

    audio_type = get_config("INTERMEDIATE_AUDIO_TYPE", "wav")
    enable_video_generation = get_config("ENABLE_VIDEO_GENERATION", True)
    enable_youtube_upload = get_config("ENABLE_YOUTUBE_UPLOAD", True)
    enable_bgm_mix = get_config("ENABLE_BGM_MIX", True)
    video_resolution = get_config("VIDEO_RESOLUTION", "1080p")
    skip_existing = get_config("SKIP_EXISTING", True)
    use_poetry_audio = bool(get_config("POETRY_CHAPTER_SEGMENT_SECONDS", 0) > 0)

    split_plan = build_split_part_plans(chapters_data, book_duration_seconds=get_explicit_total_book_duration_seconds(chapters_data))
    if not split_plan or not split_plan.get("parts"):
        result.error = "分片计划为空"
        return finalize_book_result(result, book_dir, book_record)

    result.part_count = len(split_plan["parts"])

    state = initialize_split_processing_state(book_record, split_plan)
    reconcile_split_part_upload_states(state, result.part_count)
    restore_split_shared_assets_from_state(result, state, book_dir, safe_name, book_name)

    needs_cover = not bool(result.cover_image_path and os.path.exists(result.cover_image_path) and os.path.getsize(result.cover_image_path) > 0)
    needs_seo = not bool(result.seo_text_path and os.path.exists(result.seo_text_path) and os.path.getsize(result.seo_text_path) > 0)

    if needs_cover or needs_seo:
        prepare_book_cover_and_seo(result, book_data, book_dir, safe_name, book_name)

    persist_split_shared_assets_to_state(book_record, state, result, book_dir, safe_name, book_name)

    for part_index, part_plan in enumerate(split_plan["parts"], start=1):
        part_state = get_split_part_state(state, part_index)
        if not part_state:
            continue
        part_status = str(part_state.get("status") or "").strip().lower()
        log.info("[%s] 分片 %d/%d 状态: %s", book_name, part_index, result.part_count, part_status)

        if part_status == "completed":
            log.info("[%s] 分片 %d/%d 已完成，跳过处理。", book_name, part_index, result.part_count)
            vid = str(part_state.get("video_id") or "").strip()
            if vid and not result.youtube_url:
                result.youtube_url = f"https://youtu.be/{vid}"
            continue

        should_stop, _ = should_stop_before_next_book(time.time())
        if should_stop:
            log.warning("[%s] 剩余时间不足，停止处理后续分片。", book_name)
            break

        result.stop_requested = False
        part_chapters = part_plan.get("chapter_ids", [])
        part_chapters_data = [c for c in chapters_data if c.get("id") in part_chapters or c.get("index") in part_chapters]

        part_safe_name = f"{safe_name}_part_{part_index:03d}" if result.part_count > 1 else safe_name
        dest_dir = os.path.join(book_dir, f"part_{part_index:03d}")

        ok_download = download_chapter_items(
            part_chapters_data,
            dest_dir,
            part_safe_name,
            audio_type=audio_type,
            book_name=f"{book_name}[Part {part_index}/{result.part_count}]",
            allow_skip_existing=skip_existing,
        )
        if not ok_download:
            raise RuntimeError(f"分片 {part_index} 没有有效的章节")

        try:
            denoised = denoise_audio_paths_parallel(
                dest_dir, part_safe_name, file_format=audio_type,
                book_name=f"{book_name}[Part {part_index}/{result.part_count}]",
                use_poetry_audio=use_poetry_audio,
            )
        except Exception as e:
            raise RuntimeError(f"分片 {part_index} 降噪失败: {e}")

        merged = merge_audio_ffmpeg(
            dest_dir, part_safe_name, file_format=audio_type,
            book_name=f"{book_name}[Part {part_index}/{result.part_count}]",
            use_poetry_audio=use_poetry_audio,
        )

        if enable_bgm_mix and merged:
            sync_music_library_if_enabled()
            mixed = mix_with_bgm(merged, dest_dir, part_safe_name, book_name=f"{book_name}[Part {part_index}/{result.part_count}]", audio_type=audio_type)

        if enable_video_generation and result.cover_image_path:
            video_path = generate_video(result, dest_dir, part_safe_name, book_name=f"{book_name}[Part {part_index}/{result.part_count}]", video_resolution=video_resolution)
            if video_path and os.path.isfile(video_path):
                log.info("[%s] 分片 %d/%d 复用已有 MP4。", book_name, part_index, result.part_count)
            elif not video_path:
                log.warning("[%s] 分片 %d/%d MP4 生成失败。", book_name, part_index, result.part_count)

        if enable_youtube_upload and result.video_path:
            try:
                part_hint = f"第 {part_index}/{result.part_count} 部分" if result.part_count > 1 else ""
                _, yt_url, publish_at, schedule_reason = upload_youtube_video(
                    result, youtube, book_name=book_name, book_dir=book_dir,
                    part_hint=part_hint, part_index=part_index, part_count=result.part_count,
                    dest_dir=dest_dir,
                )
                if yt_url:
                    log.info("[%s] 分片 %d/%d 复用回执: %s", book_name, part_index, result.part_count, yt_url)
            except Exception as e:
                raise RuntimeError(f"分片 {part_index} YouTube 上传失败: {e}")

        part_state["status"] = "completed"
        part_state["processed_at"] = dt_module.datetime.now().isoformat()
        state_ref = save_split_processing_state(book_record, state)
        result.state_path = state_ref

    sync_result_from_split_state(result, state, split_plan)

    # Podcast 后置同步：分片全部上传完成后同步到播放列表
    podcast_runtime_enabled = get_config("ENABLE_YOUTUBE_PODCAST_RUNTIME", False)
    podcast_split_enabled = get_config("ENABLE_YOUTUBE_PODCAST_SPLIT_PLAYLIST", False)
    all_parts_completed = result.completed_part_count >= result.part_count
    if podcast_runtime_enabled and podcast_split_enabled and all_parts_completed:
        try:
            podcast_result = sync_split_playlist_podcast(result, state, book_record, book_name)
            if podcast_result:
                log.info("[%s] Podcast 播放列表同步完成: %s", book_name, podcast_result.get("playlist_id", ""))
        except Exception as e:
            log.warning("[%s] Podcast 同步失败（不影响主流程）: %s", book_name, e)

    result = finalize_book_result(result, book_dir, book_record)
    return result


# ============================================================================
# 单书处理入口
# ============================================================================

def process_book(book_record, output_root, youtube):
    """单书处理入口：根据预估时长切换标准/分片模式"""
    book_id = str(book_record.get("book_id", ""))
    book_name = str(book_record.get("book_name", "") or f"untitled_{book_id}")
    category = str(book_record.get("category", "") or "未分类")
    book_data_raw = book_record.get("book_data", "{}")
    book_data = book_data_raw
    if isinstance(book_data_raw, str):
        try:
            book_data = json.loads(book_data_raw)
        except Exception as e:
            log.error("[%s] book_data JSON 解析失败: %s", book_name, e)
            return BookResult(book_id=book_id, book_name=book_name, category=category, error=f"book_data JSON 解析失败: {e}")
    if not isinstance(book_data, dict):
        log.error("[%s] book_data 不是有效字典", book_name)
        return BookResult(book_id=book_id, book_name=book_name, category=category, error="book_data 不是有效字典")

    chapters_data = book_data.get("chapters_data") or []
    if isinstance(chapters_data, str):
        try:
            chapters_data = json.loads(chapters_data)
        except Exception:
            chapters_data = []
    if not isinstance(chapters_data, list):
        chapters_data = []
    book_data_raw_chapter_count = len(chapters_data)

    chapters_data = [ch for ch in chapters_data if isinstance(ch, dict)]

    # 调试：打印前2章的完整原始数据和时长解析结果
    for ci, ch in enumerate(chapters_data[:2]):
        ch_id = ch.get("id", ci)
        raw_long = ch.get("long", ch.get("duration", ch.get("duration_seconds", "N/A")))
        parsed = parse_duration_to_seconds(ch.get("long"))
        explicit = get_explicit_chapter_duration_seconds(ch)
        estimated = estimate_chapter_duration_seconds(ch)
        log.info("[%s-第%d章] long=%s parsed=%ds explicit=%s estimated=%ds",
                 book_name, ch_id, raw_long, parsed, explicit, estimated)
        log.info("[%s-第%d章] 原始数据: %s", book_name, ch_id, json.dumps(ch, ensure_ascii=False))

    total_seconds = get_explicit_total_book_duration_seconds(chapters_data)

    # 调试：打印总时长估算明细
    if total_seconds is not None:
        log.info("[%s] 显式总时长: %d秒 (%.2f小时)", book_name, total_seconds, total_seconds / 3600.0)
    else:
        chapter_estimates = [estimate_chapter_duration_seconds(ch) for ch in chapters_data[:5]]
        log.info("[%s] 无显式总时长，前5章估算: %s", book_name, chapter_estimates)

    # 如果有显式总时长就用它，否则用估算总时长
    if total_seconds is not None:
        raw_hours = total_seconds / 3600.0
    else:
        estimated = sum(estimate_chapter_duration_seconds(ch) for ch in chapters_data)
        raw_hours = estimated / 3600.0
        log.info("书籍 %s 的章节时长无显式数据，使用估算时长: %.2f 小时",
                  book_name, raw_hours)

    split_trigger_hours = float(get_config("LONG_AUDIO_SPLIT_TRIGGER_HOURS", 12.0))

    safe_name = sanitize_filename(book_name)[:120]
    book_dir = os.path.join(output_root, safe_name)
    os.makedirs(book_dir, exist_ok=True)

    result = BookResult(
        book_id=book_id,
        book_name=book_name,
        category=category,
        chapter_count=len(chapters_data),
        estimated_total_duration_seconds=int(raw_hours * 3600),
    )

    # 只有明确知道时长太短时才跳过（遵循 runtime_core.py 逻辑）
    if total_seconds is not None and 0 < total_seconds < MIN_BOOK_DURATION_SECONDS:
        skip_and_delete_short_book(book_record, result, book_name)
        return finalize_book_result(result, book_dir, book_record)

    if raw_hours >= split_trigger_hours:
        max_part_hours = float(get_config("LONG_AUDIO_PART_TARGET_HOURS", 11.8))
        result.split_mode = True
        log.info("[%s] 长音频（%.1f 小时），进入分片模式，每片目标 %.1f 小时。", book_name, raw_hours, max_part_hours)
        return process_split_book(book_record, book_data, chapters_data, book_dir, safe_name, book_name, youtube, result)
    else:
        log.info("[%s] 标准音频（%.1f 小时），进入标准模式。", book_name, raw_hours)
        return process_standard_book(book_record, book_data, chapters_data, book_dir, safe_name, book_name, youtube, result)


# ============================================================================
# 主流程入口
# ============================================================================

def run_pipeline(runtime_config=None):
    """主流程入口 - 配置加载 → 书籍遍历 → 结果汇总"""
    if runtime_config is not None:
        if not isinstance(runtime_config, dict):
            raise TypeError("runtime_config 必须是 dict 或 None")
        apply_runtime_config(runtime_config)

    validate_runtime_config()
    clear_runtime_output_if_needed()

    overrides = apply_cloud_runtime_overrides()
    if overrides:
        apply_runtime_config(overrides)

    output_root = str(get_config("OUTPUT_ROOT", "") or "").strip()
    os.makedirs(output_root, exist_ok=True)

    page_size = int(get_config("BOOKS_PAGE_SIZE", 5) or 5)
    max_books = int(get_config("MAX_BOOKS_PER_RUN", 0) or 0)

    run_started_at = time.time()
    all_results = []
    processed_count = 0
    offset = 0
    _stop_pipeline = False  # 全局停止标志

    resume_book_id = str(get_config("RESUME_BOOK_ID", "") or "").strip()
    resume_mode = bool(resume_book_id)

    # 预查询待处理书籍总数（用于进度显示）
    total_matching = 0
    try:
        table_sql = get_public_table_identifier("books")
        count_sql = psycopg_sql.SQL("SELECT COUNT(*) AS cnt FROM {}").format(table_sql)
        count_params = []
        target_category = str(get_config("TARGET_CATEGORY", "") or "").strip()
        if target_category:
            count_sql += psycopg_sql.SQL(" WHERE category = %s")
            count_params.append(target_category)
        count_row = execute_postgres_fetchone(count_sql, tuple(count_params))
        total_matching = int(count_row["cnt"]) if count_row else 0
    except Exception:
        total_matching = 0

    has_interrupted = list_interrupted_book_states()
    if not resume_mode and has_interrupted:
        log.info("发现 %d 个未完成的分片状态，将尝试续跑。", len(has_interrupted))

    youtube = None
    enable_youtube_upload = get_config("ENABLE_YOUTUBE_UPLOAD", True)
    if enable_youtube_upload:
        try:
            youtube = authenticate_youtube_from_supabase(get_config("YOUTUBE_CHANNEL_NAME", ""))
        except MissingYouTubeCredentialsError as e:
            log.error("YouTube 认证失败，流水线无法继续: %s", e)
            raise

    # 在主循环前预先下载音乐库（如果启用 BGM）
    if get_config("ENABLE_BGM_MIX", True):
        sync_music_library_if_enabled()

    while True:
        if max_books > 0 and processed_count >= max_books:
            log.info("已达单次处理上限 %d 本，停止获取更多书籍。", max_books)
            break

        books_page = _fetch_books_page_from_database(offset, page_size)
        if not books_page:
            log.info("没有更多待处理的书籍。")
            break

        random.shuffle(books_page)

        should_stop, remaining = should_stop_before_next_book(run_started_at)
        if should_stop:
            log.warning("运行时缓冲耗尽，停止处理新书。")
            break

        for book_record in books_page:
            if max_books > 0 and processed_count >= max_books:
                break

            book_id = str(book_record.get("book_id", ""))
            book_name = str(book_record.get("book_name", "") or book_id)

            if resume_mode and book_id != resume_book_id:
                log.info("[%s] 跳过（非目标续跑书籍: %s）", book_name, resume_book_id)
                processed_count += 1
                continue

            should_stop_book, remaining = should_stop_before_next_book(run_started_at)
            if should_stop_book:
                log.warning("剩余时间不足，停止处理新书。")
                break

            log.info("[%d/%d] 开始处理书籍: %s", processed_count + 1, max_books if max_books > 0 else total_matching, book_name)
            try:
                result = process_book(book_record, output_root, youtube)
                all_results.append(result)
                processed_count += 1

                # 发生错误时立即停止整个流程
                if not result.success and not getattr(result, "skipped", False):
                    error_msg = result.error or "未知错误"
                    log.error("[%d/%d] 书籍 %s 处理失败，终止整个流程: %s",
                              processed_count, len(all_results), book_name, error_msg)
                    _stop_pipeline = True
                    break
            except Exception as e:
                log.error("[%d/%s] 处理书籍 %s 时发生未捕获异常，终止整个流程: %s",
                          processed_count + 1, "?" if max_books == 0 else str(max_books), book_name, e)
                log.error("堆栈: %s", traceback.format_exc())
                error_result = BookResult(
                    book_id=book_id,
                    book_name=book_name,
                    category=str(book_record.get("category", "") or ""),
                    error=str(e),
                )
                all_results.append(error_result)
                _stop_pipeline = True
                break  # 异常时立即停止

            if resume_mode:
                log.info("续跑模式完成，退出。")
                break

        if _stop_pipeline:
            break

        offset += page_size

    if youtube and enable_youtube_upload and hasattr(youtube, "close"):
        try:
            youtube.close()
        except Exception:
            pass

    runtime_elapsed = time.time() - run_started_at
    extra = {
        "run_started_at": dt_module.datetime.fromtimestamp(run_started_at).isoformat(),
        "runtime_seconds": runtime_elapsed,
        "runtime_formatted": format_seconds_hhmmss(runtime_elapsed),
        "total_books": processed_count,
    }

    save_run_summary(output_root, all_results, extra=extra)

    for i, result in enumerate(all_results, start=1):
        name = result.book_name or f"ID:{result.book_id}"
        if result.success:
            log.info("[%d/%d] 成功 %s", i, len(all_results), name)
        elif getattr(result, "skipped", False):
            log.info("[%d/%d] 跳过 %s - %s", i, len(all_results), name, getattr(result, "skipped_reason", "") or result.error)
        elif getattr(result, "pending_resume", False):
            log.warning("[%d/%d] 暂停 %s - %s", i, len(all_results), name, result.error)
        else:
            log.error("[%d/%d] 失败 %s - %s", i, len(all_results), name, result.error)

    return all_results