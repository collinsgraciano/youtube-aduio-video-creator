"""状态管理模块 - 断点续跑状态持久化"""
from __future__ import annotations

import os
import json
import hashlib
import time
import datetime as dt_module
import traceback
import random
from urllib.parse import urlparse, parse_qs

from psycopg import sql
from psycopg.types.json import Jsonb

from pipeline.config import get_config
from pipeline.log_utils import log
from pipeline.db import execute_postgres_fetchone, execute_postgres_fetchall, execute_postgres, get_public_table_identifier, get_postgres_dsn
from pipeline.utils import normalize_text_items, write_json_file, read_json_file, make_json_compatible, format_seconds_hhmmss
from pipeline.audio import estimate_chapter_duration_seconds
from pipeline.constants import DEFAULT_BOOK_STATE_TABLE
from psycopg import sql as psycopg_sql


# ============================================================================
# 状态引用构建
# ============================================================================

def build_split_state_ref(book_id, project_flag=None):
    """构建状态引用标识"""
    flag = str(project_flag or get_config("PROJECT_FLAG", "") or "").strip()
    ref = f"postgres:{get_book_state_table_name()}:{book_id}"
    if flag:
        ref += f":{flag}"
    return ref


def get_book_state_table_name():
    """获取书籍状态表名"""
    return str(get_config("BOOK_STATE_TABLE", "") or DEFAULT_BOOK_STATE_TABLE).strip() or DEFAULT_BOOK_STATE_TABLE


# ============================================================================
# 布尔配置读取
# ============================================================================

def _read_bool_runtime_config(name, default=False):
    """读取布尔运行时配置"""
    from pipeline.config import get_config
    value = get_config(name, default)
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _should_cleanup_completed_split_states():
    """检查是否应清理已完成的分片状态"""
    return _read_bool_runtime_config("CLEANUP_COMPLETED_SPLIT_STATES", True)


# ============================================================================
# 状态辅助函数
# ============================================================================

def _build_split_part_lookup_key(part_like):
    """构建分片查找键"""
    if isinstance(part_like, dict):
        return int(part_like.get("part_index", 0) or 0)
    return int(part_like)


def _split_part_has_uploaded_video(part_state):
    """检查分片是否已上传视频"""
    if not isinstance(part_state, dict):
        return False
    return bool(str(part_state.get("video_id") or "").strip() or str(part_state.get("youtube_url") or "").strip())


def _is_split_playlist_required(part_count):
    """检查是否需要播放列表"""
    return part_count > 1


def _split_part_is_completed(part_state):
    """检查分片是否已完成"""
    if not isinstance(part_state, dict):
        return False
    return str(part_state.get("status") or "").strip().lower() == "completed"


def _reconcile_split_part_state(part_state):
    """协调分片状态"""
    if not isinstance(part_state, dict):
        return False
    changed = False
    if str(part_state.get("status") or "").strip().lower() not in {"pending", "completed", "failed", "in_progress"}:
        part_state["status"] = "pending"
        changed = True
    return changed


def evaluate_split_completion_state(state):
    """评估分片完成状态"""
    if not isinstance(state, dict):
        return {"part_count": 1, "completed_part_count": 0, "fully_completed": False, "playlist_required": False, "playlist_completed": False}

    parts = state.get("parts", []) or []
    part_count = max(1, len(parts))
    completed_part_count = sum(1 for p in parts if isinstance(p, dict) and str(p.get("status") or "").strip().lower() == "completed")
    fully_completed = completed_part_count >= part_count and part_count > 0
    playlist_required = _is_split_playlist_required(part_count)

    playlist_completed = False
    if playlist_required:
        playlist_state = get_split_playlist_state(state)
        playlist_completed = str(playlist_state.get("status") or "").strip().lower() == "completed"

    return {
        "part_count": part_count,
        "completed_part_count": completed_part_count,
        "fully_completed": fully_completed,
        "playlist_required": playlist_required,
        "playlist_completed": playlist_completed,
    }


# ============================================================================
# 数据库状态读写
# ============================================================================

def normalize_split_state_from_row(row):
    """从数据库行规范化分片状态"""
    if not isinstance(row, dict):
        return None

    state_json = row.get("state_json")
    state = {}
    if isinstance(state_json, dict):
        state = state_json
    elif isinstance(state_json, str):
        try:
            state = json.loads(state_json) if state_json.strip() else {}
        except (json.JSONDecodeError, TypeError):
            state = {}
            log.warning("解析状态 JSON 失败，将用空状态: %s...", str(state_json)[:200])

    if not isinstance(state, dict):
        state = {}

    state["book_id"] = str(row.get("book_id") or "")
    state["book_name"] = str(row.get("book_name") or "")
    state["category"] = str(row.get("category") or "")
    state["status"] = str(row.get("state_status") or state.get("status", "pending"))
    state["pending_resume"] = bool(row.get("pending_resume", False))
    state["current_part_index"] = int(row.get("current_part_index") or state.get("current_part_index", 0) or 0)
    state["completed_part_count"] = max(0, int(row.get("completed_part_count") or state.get("completed_part_count", 0) or 0))
    state["part_count"] = max(1, int(row.get("part_count") or state.get("part_count", 1) or 1))

    return state


def load_split_processing_state(book_record):
    """从数据库加载分片处理状态"""
    table_name = get_book_state_table_name()
    book_id = str(book_record.get("book_id") or "").strip()
    if not book_id:
        return None

    project_flag = str(get_config("PROJECT_FLAG", "") or "").strip()
    table_sql = get_public_table_identifier(table_name)

    try:
        if project_flag:
            row = execute_postgres_fetchone(
                sql.SQL(
                    """
                    SELECT book_id, book_name, category, state_status, pending_resume,
                           current_part_index, completed_part_count, part_count,
                           state_json, updated_at, created_at
                    FROM {}
                    WHERE book_id = %s AND project_flag = %s
                    LIMIT 1
                    """
                ).format(table_sql),
                (book_id, project_flag),
                optional=True,
            )
            if row:
                return normalize_split_state_from_row(row)

        row = execute_postgres_fetchone(
            sql.SQL(
                """
                SELECT book_id, book_name, category, state_status, pending_resume,
                       current_part_index, completed_part_count, part_count,
                       state_json, updated_at, created_at
                FROM {}
                WHERE book_id = %s
                LIMIT 1
                """
            ).format(table_sql),
            (book_id,),
            optional=True,
        )
        return normalize_split_state_from_row(row) if row else None
    except Exception as e:
        log.warning("从数据库读取分片状态失败: %s", e)
        return None


def _build_split_state_completeness_rank(state):
    """构建状态完整度排名"""
    if not isinstance(state, dict):
        return -1

    progress = evaluate_split_completion_state(state)
    completed_part_count = progress["completed_part_count"]
    part_count = progress["part_count"]
    playlist_completed = progress["playlist_completed"]
    updated_at = str(state.get("updated_at") or "")

    score = completed_part_count * 1000 + (part_count * 10) + (1 if playlist_completed else 0)
    return score


def reload_split_processing_state(book_record, fallback_state=None, book_name=""):
    """重新加载分片处理状态"""
    state = load_split_processing_state(book_record)
    if isinstance(state, dict):
        return state

    if isinstance(fallback_state, dict):
        return fallback_state

    return None


def _save_split_processing_state_raw(book_record, state):
    """保存分片处理状态到数据库（原始操作）"""
    table_name = get_book_state_table_name()
    book_id = str(book_record.get("book_id") or "")
    project_flag = str(get_config("PROJECT_FLAG", "") or "").strip()
    table_sql = get_public_table_identifier(table_name)
    now = dt_module.datetime.now().isoformat()
    state["updated_at"] = now
    if not state.get("created_at"):
        state["created_at"] = now

    progress = evaluate_split_completion_state(state)
    part_count = max(1, progress["part_count"])
    completed_part_count = max(0, progress["completed_part_count"])

    state_json_payload = make_json_compatible(state)
    state_ref = build_split_state_ref(book_id, project_flag)

    try:
        execute_postgres(
            sql.SQL(
                """
                INSERT INTO {} (
                  book_id, project_flag, book_name, category,
                  pending_resume, state_status,
                  current_part_index, completed_part_count, part_count,
                  updated_at, created_at, state_json
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
                book_id, project_flag,
                state.get("book_name", ""), state.get("category", ""),
                bool(state.get("pending_resume", False)),
                state.get("status", "in_progress"),
                state.get("current_part_index"),
                int(state.get("completed_part_count") or 0),
                int(state.get("part_count") or 1),
                state["updated_at"], state["created_at"],
                Jsonb(state_json_payload),
            ),
        )
    except Exception as e:
        raise RuntimeError(f"写入数据库断点状态失败，请检查表 {table_name}: {e}") from e

    return state_ref


# ============================================================================
# 调试和日志输出
# ============================================================================

def _truncate_split_state_debug_value(value, limit=240):
    """截断状态调试值"""
    text = str(value or "").strip()
    if len(text) <= limit:
        return text
    return text[:limit] + "...<truncated>"


def _build_split_state_debug_payload(book_record, state):
    """构建分片状态调试信息"""
    from pipeline.youtube import _extract_youtube_video_id

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
        parts_summary.append({
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
        })

    payload = {
        "book_id": str(safe_book.get("book_id") or safe_state.get("book_id") or "").strip(),
        "project_flag": str(get_config("PROJECT_FLAG", "") or "").strip(),
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
    """记录状态持久化日志"""
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

    import re as _re
    match = _re.fullmatch(r"part_(\d+)_(.+)", last_stage)
    if match:
        part_index = int(match.group(1))
        suffix = match.group(2)
        part_state = get_split_part_state(state, part_index) or {}

        if suffix == "upload_persisted":
            if not _split_part_has_uploaded_video(part_state):
                return
            log.info(
                "[%s] 分片 %d/%d 的上传回执已写入数据库续跑状态（进度 %d/%d，state=%s）",
                book_name, part_index, part_count, completed_part_count, part_count, state_ref,
            )
            return

        if suffix == "completed":
            if not _split_part_is_completed(part_state):
                return
            log.info(
                "[%s] 分片 %d/%d 已处理完成，当前状态已写入数据库（进度 %d/%d，state=%s）",
                book_name, part_index, part_count, completed_part_count, part_count, state_ref,
            )
            return

        if suffix == "failed":
            if str(part_state.get("status") or "").strip().lower() != "failed":
                return
            log.warning(
                "[%s] 分片 %d/%d 的失败状态已写入数据库（进度 %d/%d，state=%s，error=%s）",
                book_name, part_index, part_count, completed_part_count, part_count, state_ref, last_error,
            )
            return

    if last_stage == "playlist_completed":
        if not progress["playlist_completed"]:
            return
        log.info(
            "[%s] 播放列表完成状态已写入数据库（进度 %d/%d，state=%s）",
            book_name, completed_part_count, part_count, state_ref,
        )
        return

    if last_stage == "playlist_failed":
        log.warning(
            "[%s] 播放列表失败状态已写入数据库（进度 %d/%d，state=%s，error=%s）",
            book_name, completed_part_count, part_count, state_ref, last_error,
        )
        return

    if last_stage == "all_parts_completed":
        if not progress["fully_completed"]:
            return
        log.info(
            "[%s] 多 P 最终完成状态已写入数据库（进度 %d/%d，state=%s）",
            book_name, completed_part_count, part_count, state_ref,
        )


def save_split_processing_state(book_record, state):
    """保存分片处理状态到数据库"""
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
    """删除分片处理状态"""
    table_name = get_book_state_table_name()
    book_id = str(book_record.get("book_id") or "").strip()
    if not book_id:
        return False

    project_flag = str(get_config("PROJECT_FLAG", "") or "").strip()
    table_sql = get_public_table_identifier(table_name)

    try:
        if only_if_completed:
            state = load_split_processing_state(book_record)
            if isinstance(state, dict) and not evaluate_split_completion_state(state)["fully_completed"]:
                log.info("书籍 %s 未完成，保留断点状态。", book_id)
                return False

        if project_flag:
            execute_postgres(
                sql.SQL("DELETE FROM {} WHERE book_id = %s AND project_flag = %s").format(table_sql),
                (book_id, project_flag),
                optional=True,
            )
        else:
            execute_postgres(
                sql.SQL("DELETE FROM {} WHERE book_id = %s").format(table_sql),
                (book_id,),
                optional=True,
            )
        return True
    except Exception as e:
        log.warning("删除断点状态失败: %s", e)
        return False


def cleanup_completed_split_states(project_flag=None, category=None):
    """清理已完成的分片状态"""
    from pipeline.log_utils import runtime_console_print

    table_name = get_book_state_table_name()
    flag = str(project_flag or get_config("PROJECT_FLAG", "") or "").strip()
    table_sql = get_public_table_identifier(table_name)

    try:
        if flag:
            rows = execute_postgres_fetchall(
                sql.SQL(
                    """
                    SELECT book_id, book_name, category, state_status, pending_resume,
                           completed_part_count, part_count, state_json
                    FROM {}
                    WHERE project_flag = %s
                    """
                ).format(table_sql),
                (flag,),
                optional=True,
            )
        else:
            rows = execute_postgres_fetchall(
                sql.SQL(
                    """
                    SELECT book_id, book_name, category, state_status, pending_resume,
                           completed_part_count, part_count, state_json
                    FROM {}
                    """
                ).format(table_sql),
                optional=True,
            )

        if not rows:
            runtime_console_print("没有需要清理的续跑状态。", level="INFO")
            return 0

        deleted_count = 0
        for row in rows:
            state_status = str(row.get("state_status") or "").strip().lower()
            completed_part_count = int(row.get("completed_part_count") or 0)
            part_count = max(1, int(row.get("part_count") or 1))
            row_category = str(row.get("category") or "").strip()

            if category and row_category and row_category != category:
                continue

            if state_status != "completed" and completed_part_count < part_count:
                continue

            if delete_split_processing_state(row):
                deleted_count += 1

        runtime_console_print(f"已清理 {deleted_count} 条已完成续跑状态。", level="INFO")
        return deleted_count
    except Exception as e:
        log.warning("清理已完成续跑状态失败: %s", e)
        return 0


# ============================================================================
# 状态初始化与读取
# ============================================================================

def get_split_part_state(state, part_index):
    """获取分片的状态"""
    if not isinstance(state, dict):
        return None

    key = _build_split_part_lookup_key(part_index)
    for part in state.get("parts", []) or []:
        if isinstance(part, dict) and _build_split_part_lookup_key(part) == key:
            return part
    return None


def get_split_shared_assets(state):
    """获取分片共享资源"""
    shared = state.get("shared_assets")
    if isinstance(shared, dict):
        return shared
    state["shared_assets"] = {}
    return state["shared_assets"]


def get_split_playlist_state(state):
    """获取分片播放列表状态"""
    playlist = state.get("playlist")
    if isinstance(playlist, dict):
        return playlist
    state["playlist"] = {}
    return state["playlist"]


def initialize_split_processing_state(book_record, book_dir, chapters_sorted, split_plan):
    """初始化分片处理状态"""
    existing = load_split_processing_state(book_record)
    if isinstance(existing, dict):
        existing_mode = str(existing.get("mode") or "").strip().lower()
        if existing_mode == "split_upload":
            existing_shared = existing.get("shared_assets") if isinstance(existing.get("shared_assets"), dict) else {}
            existing_parts = existing.get("parts", []) or []
            existing_parts.sort(key=lambda p: int(p.get("part_index", 0)))

            existing_part_count = max(1, len(existing_parts))
            plan_part_count = max(1, len(split_plan.get("parts", [])))
            if existing_part_count != plan_part_count:
                log.warning(
                    "分片数量发生变化（状态=%d vs 计划=%d），可能配置变更了分片策略，重新初始化。",
                    existing_part_count, plan_part_count,
                )
                fingerprint = build_split_plan_signature(chapters_sorted, split_plan)
                existing_fingerprint = str(existing.get("plan_fingerprint") or "")
                if existing_fingerprint and existing_fingerprint != fingerprint:
                    log.info(
                        "封面/SEO/DeepFilter/BGM 等配置已变更，无法复用历史分片计划，重建续跑状态。旧指纹=%s",
                        existing_fingerprint[:16],
                    )
                state = _build_fresh_split_state(book_record, split_plan)
                state["shared_assets"] = existing_shared
                state_ref = save_split_processing_state(book_record, state)
                return state, state_ref

            state = existing
            state["status"] = "in_progress"
            state["pending_resume"] = True
            state_ref = save_split_processing_state(book_record, state)
            return state, state_ref

    return _build_fresh_split_state(book_record, split_plan), ""


def _build_fresh_split_state(book_record, split_plan):
    """构建新的分片状态"""
    now = dt_module.datetime.now().isoformat()
    state = {
        "state_version": 5,
        "mode": "split_upload",
        "book_id": str(book_record.get("book_id", "")),
        "book_name": book_record.get("book_name", ""),
        "category": book_record.get("category", ""),
        "part_count": max(1, len(split_plan.get("parts", []))),
        "parts": [
            {
                "part_index": part["part_index"],
                "chapter_start_index": part["chapter_start_index"],
                "chapter_end_index": part["chapter_end_index"],
                "status": "pending",
                "error": "",
                "video_id": "",
                "youtube_url": "",
                "youtube_title": "",
                "uploaded_at": "",
                "publish_at": "",
                "schedule_reason": "",
                "playlist_item_id": "",
                "audio_path": "",
                "video_path": "",
                "completed_at": "",
                "last_stage": "",
            }
            for part in split_plan.get("parts", [])
        ],
        "shared_assets": {},
        "playlist": {},
        "current_part_index": 0,
        "completed_part_count": 0,
        "last_stage": "split_init",
        "last_error": "",
        "pending_resume": True,
        "plan_fingerprint": build_split_plan_signature(None, split_plan),
        "created_at": now,
        "updated_at": now,
    }
    return state


def build_split_plan_signature(chapters_sorted, split_plan):
    """构建分片计划签名用于变更检测"""
    from pipeline.config import get_config
    payload = {
        "project_flag": get_config("PROJECT_FLAG", ""),
        "split_trigger_hours": get_config("LONG_AUDIO_SPLIT_TRIGGER_HOURS", 12.0),
        "part_target_hours": get_config("LONG_AUDIO_PART_TARGET_HOURS", 11.8),
        "enable_deepfilter": get_config("ENABLE_DEEPFILTER", True),
        "enable_bgm_mix": get_config("ENABLE_BGM_MIX", True),
        "enable_video_generation": get_config("ENABLE_VIDEO_GENERATION", True),
        "enable_youtube_upload": get_config("ENABLE_YOUTUBE_UPLOAD", True),
        "video_resolution": get_config("VIDEO_RESOLUTION", "1080p"),
        "youtube_channel_name": get_config("YOUTUBE_CHANNEL_NAME", ""),
        "chapters": [
            {"id": chapter.get("id"), "title": chapter.get("title"), "long": chapter.get("long")}
            for chapter in chapters_sorted
        ] if chapters_sorted else [],
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


def list_interrupted_book_states(book_rows_by_id=None):
    """列出有中断状态的书籍"""
    table_name = get_book_state_table_name()
    project_flag = str(get_config("PROJECT_FLAG", "") or "").strip()
    table_sql = get_public_table_identifier(table_name)

    try:
        if project_flag:
            rows = execute_postgres_fetchall(
                sql.SQL(
                    """
                    SELECT book_id, book_name, category, state_status, pending_resume,
                           completed_part_count, part_count, state_json, updated_at
                    FROM {}
                    WHERE project_flag = %s AND pending_resume = true
                    """
                ).format(table_sql),
                (project_flag,),
                optional=True,
            )
        else:
            rows = execute_postgres_fetchall(
                sql.SQL(
                    """
                    SELECT book_id, book_name, category, state_status, pending_resume,
                           completed_part_count, part_count, state_json, updated_at
                    FROM {}
                    WHERE pending_resume = true
                    """
                ).format(table_sql),
                optional=True,
            )

        if not rows:
            return {}

        valid_states = {}
        for row in rows:
            book_id = str(row.get("book_id") or "").strip()
            if not book_id:
                continue

            if book_rows_by_id is not None and book_id not in book_rows_by_id:
                continue

            state = normalize_split_state_from_row(row)
            if isinstance(state, dict):
                valid_states[book_id] = state

        return valid_states
    except Exception as e:
        log.warning("读取中断书籍状态列表失败: %s", e)
        return {}


# ============================================================================
# 上传回执协调
# ============================================================================

def reconcile_split_part_upload_states(result, state, split_plan, book_name, category):
    """协调分片上传状态"""
    from pipeline.youtube import (
        authenticate_youtube_from_supabase, _extract_youtube_video_id,
        _fetch_video_status_rows_with_client, _build_existing_video_match_from_row,
        _build_channel_video_title_index_with_client, _normalize_youtube_title_key,
    )

    channel_name = str(get_config("YOUTUBE_CHANNEL_NAME", "") or "").strip()
    enable_youtube_upload = get_config("ENABLE_YOUTUBE_UPLOAD", True)

    if not enable_youtube_upload or not channel_name:
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
            expected_title = _build_expected_split_upload_title(result, book_name, category, part_plan["part_index"], part_count)
            if expected_title and str(part_state.get("youtube_title") or "").strip() != expected_title:
                part_state["youtube_title"] = expected_title
                changed = True

        candidates.append({
            "part_plan": part_plan,
            "part_state": part_state,
            "candidate_video_id": candidate_video_id,
            "expected_title": expected_title,
        })
        if candidate_video_id:
            candidate_video_ids.append(candidate_video_id)

    if not candidates:
        return {"changed": changed, "recovered": [], "reset": []}

    youtube = authenticate_youtube_from_supabase(channel_name)
    if not youtube:
        return {"changed": changed, "recovered": [], "reset": []}

    live_rows_by_id, _ = _wait_for_live_video_rows_with_client(
        youtube, candidate_video_ids, max_attempts=2, context_label=book_name,
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
                book_name, part_index, part_count, old_video_id or "<empty>", new_video_id or "<empty>", expected_title or "<empty>",
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
            book_name, part_index, part_count, candidate_video_id or "<empty>", expected_title or "<empty>",
        )

    return {"changed": changed, "recovered": recovered, "reset": reset}


def _build_expected_split_upload_title(result, book_name, category, part_index, part_count):
    """构建期望的分片上传标题"""
    from pipeline.youtube import build_youtube_payload
    title, _, _ = build_youtube_payload(
        result, book_name, category, youtube_chapters="",
        title_prefix=f"{part_index}-" if int(part_count or 0) > 1 else "",
        part_hint="", include_youtube_chapters=False, include_part_hint=False,
    )
    return str(title or "").strip()[:100]


def _wait_for_live_video_rows_with_client(youtube, video_ids, max_attempts=3, context_label=""):
    """等待视频变为可查询状态"""
    from pipeline.youtube import _extract_youtube_video_id, _fetch_video_status_rows_with_client

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
                context_label, attempt_index, max_attempts, ",".join(missing_ids[:10]), wait_seconds,
            )
        else:
            log.warning(
                "Waiting for YouTube videos to become readable. attempt=%d/%d missing=%s sleep=%ds",
                attempt_index, max_attempts, ",".join(missing_ids[:10]), wait_seconds,
            )
        time.sleep(wait_seconds)

    return rows_by_id, missing_ids


def _fetch_video_rows_by_id_with_client(youtube, video_ids):
    """通过视频 ID 批量查询视频信息"""
    from pipeline.youtube import _extract_youtube_video_id, _fetch_video_status_rows_with_client

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


def _apply_video_match_to_split_part(part_state, match):
    """将视频匹配信息应用到分片状态"""
    if not isinstance(part_state, dict) or not isinstance(match, dict):
        return False

    changed = False
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

    old_video_id = str(part_state.get("video_id") or "").strip()
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
    """重置分片上传状态"""
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