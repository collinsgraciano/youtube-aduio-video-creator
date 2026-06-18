"""ModelScope 令牌管理与 AI 封面/SEO 生成模块"""
from __future__ import annotations

import os
import re
import json
import time
import random
import requests
from io import BytesIO
from PIL import Image

from pipeline.config import get_config
from pipeline.log_utils import log, runtime_console_print
from pipeline.utils import normalize_runtime_source, write_json_file, read_json_file, download_file
from pipeline.db import (
    execute_postgres_fetchone, execute_postgres,
    get_public_table_identifier, get_postgres_dsn,
)
from psycopg import sql


# ============================================================================
# 令牌池管理
# ============================================================================

def normalize_modelscope_token_pool(token_value, preserve_list_reference=False):
    """规范化 ModelScope 令牌池"""
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
    """构建 ModelScope 令牌池"""
    normalized_tokens = normalize_modelscope_token_pool(token_value)
    if shuffle_once and len(normalized_tokens) > 1:
        random.shuffle(normalized_tokens)
    return normalized_tokens


def build_modelscope_token_pool_bundle(token_value, shuffle_once=False):
    """构建 ModelScope 令牌包（文本+图像）"""
    base_tokens = build_modelscope_token_pool(token_value, shuffle_once=shuffle_once)
    return {"text": list(base_tokens), "image": list(base_tokens)}


def _get_modelscope_active_tokens(token_pool):
    """获取活跃的 ModelScope 令牌列表"""
    if isinstance(token_pool, list):
        return normalize_modelscope_token_pool(token_pool, preserve_list_reference=True)
    return normalize_modelscope_token_pool(token_pool)


def _get_modelscope_usage_token_pool(token_source, usage):
    """获取指定用途的令牌池"""
    if isinstance(token_source, dict):
        token_pool = token_source.get(usage)
        if isinstance(token_pool, list):
            return normalize_modelscope_token_pool(token_pool, preserve_list_reference=True)
        return normalize_modelscope_token_pool(token_pool)
    return _get_modelscope_active_tokens(token_source)


def _remove_modelscope_token_from_pool(token_pool, token_text):
    """从令牌池中移除指定令牌"""
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


# ============================================================================
# 令牌错误检测
# ============================================================================

def is_modelscope_daily_quota_exceeded_error(error):
    """判断是否为 ModelScope 每日配额超限错误"""
    text = str(error or "")
    lowered = text.lower()
    return (
        "you have exceeded today's quota" in lowered
        or ("try again tomorrow" in lowered and "quota" in lowered)
        or ("error code: 429" in lowered and "quota" in lowered)
    )


def is_modelscope_http_429_error(error):
    """判断是否为 ModelScope HTTP 429 限流错误"""
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


def is_modelscope_http_401_error(error):
    """判断是否为 ModelScope HTTP 401 认证错误"""
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


def is_modelscope_image_review_rejection_error(error):
    """判断是否为 ModelScope 图片审核拒绝错误"""
    response = getattr(error, "response", None)
    response_text = ""
    if response is not None:
        try:
            response_text = str(response.text or "")
        except Exception:
            response_text = ""
    merged_text = "\n".join(part for part in [str(error or ""), response_text] if part).lower()
    review_keywords = ("敏感", "审核", "review", "sensitive", "moderation", "unsafe", "violation", "违规")
    if any(keyword in merged_text for keyword in review_keywords):
        return "images/generations" in (merged_text + "\n" + str(getattr(response, "url", "")))
    return False


class CoverGenerationPolicyRejectedError(RuntimeError):
    """封面生成被提供商审核拒绝时抛出"""


# ============================================================================
# 令牌数据库操作
# ============================================================================

def get_cloud_runtime_settings_table_name():
    """获取云端运行时设置表名"""
    return str(get_config("CLOUD_RUNTIME_SETTINGS_TABLE", "") or "channel_runtime_settings").strip() or "channel_runtime_settings"


def get_modelscope_token_table_name():
    """获取 ModelScope 令牌表名"""
    return str(get_config("MODELSCOPE_TOKEN_TABLE", "") or "modelscope_tokens").strip() or "modelscope_tokens"


def get_shared_cloud_runtime_scope_key():
    """获取共享云端作用域键名"""
    return "__shared__"


def load_modelscope_token_from_supabase(channel_name=None):
    """从数据库加载 ModelScope 令牌"""
    table_name = get_modelscope_token_table_name()
    shared_scope = get_shared_cloud_runtime_scope_key()
    channel = str(channel_name or get_config("YOUTUBE_CHANNEL_NAME", "") or "").strip()
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
            ).format(table_sql),
        )
        if fallback_row:
            return str(fallback_row.get("token_text") or "").strip()
        return ""
    except Exception as e:
        raise RuntimeError(f"从数据库读取 ModelScope Token 失败，请检查表 {table_name}: {e}")


def load_cloud_runtime_setting_from_supabase(channel_name, setting_key):
    """从数据库加载云端运行配置"""
    key = str(setting_key or "").strip()
    channel = str(channel_name or get_config("YOUTUBE_CHANNEL_NAME", "") or "").strip()
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

        return ""
    except Exception as e:
        log.warning("从数据库读取运行配置 %s 失败: %s", key, e)
        return ""


def resolve_modelscope_token(channel_name=None):
    """解析 ModelScope 令牌（云端优先）"""
    source = normalize_runtime_source(get_config("MODELSCOPE_TOKEN_SOURCE", "database"), default="database")
    local_token = str(get_config("MODELSCOPE_TOKEN", "") or "").strip()

    if source == "local":
        if not local_token:
            raise RuntimeError("MODELSCOPE_TOKEN_SOURCE=local，但 MODELSCOPE_TOKEN 为空")
        runtime_console_print("🔑 使用本地固定 MODELSCOPE_TOKEN", level="INFO")
        return local_token

    channel = str(channel_name or get_config("YOUTUBE_CHANNEL_NAME", "") or "").strip()

    # 从云端运行时设置表中读取
    cloud_token = ""
    try:
        cloud_token = str(load_cloud_runtime_setting_from_supabase(channel, "MODELSCOPE_TOKEN") or "").strip()
    except Exception:
        pass

    if cloud_token:
        return cloud_token

    # 从 ModelScope 令牌专用表中读取
    try:
        legacy_token = load_modelscope_token_from_supabase(channel)
        if legacy_token:
            return legacy_token
    except Exception:
        pass

    if local_token:
        runtime_console_print(
            "⚠️ 数据库中未找到任何可用的 token，回退到本地 MODELSCOPE_TOKEN",
            level="WARNING",
        )
        return local_token

    raise RuntimeError("无法解析出可用的 MODELSCOPE_TOKEN")


# ============================================================================
# AI 封面生成
# ============================================================================

def auto_create_youtube_cover(book_name, book_desc, output_path, token, resolution="1080p"):
    """使用 ModelScope 通义万相生成 YouTube 封面"""
    from pipeline.config import get_config

    connect_timeout = max(1, int(get_config("MODELSCOPE_IMAGE_CONNECT_TIMEOUT", 300)))
    read_timeout = max(1, int(get_config("MODELSCOPE_IMAGE_READ_TIMEOUT", 300)))
    poll_connect_timeout = max(1, int(get_config("MODELSCOPE_IMAGE_POLL_CONNECT_TIMEOUT", 300)))
    poll_read_timeout = max(1, int(get_config("MODELSCOPE_IMAGE_POLL_READ_TIMEOUT", 300)))
    token_switch_delay = max(1, int(get_config("MODELSCOPE_TOKEN_SWITCH_DELAY_SECONDS", 30)))

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    active_text_tokens = _get_modelscope_usage_token_pool(token, "text")
    active_image_tokens = _get_modelscope_usage_token_pool(token, "image")

    if not active_text_tokens and not active_image_tokens:
        log.error("ModelScope 没有可用的 image token")
        return False

    # 构建提示词
    category = str(get_config("TARGET_CATEGORY", "") or get_config("YOUTUBE_CHANNEL_NAME", "") or "")
    prompt = f"16:9 book cover, {category} genre, book title: {book_name}"
    if book_desc:
        desc_snippet = book_desc[:200].replace("\n", " ")
        prompt += f", {desc_snippet}"
    prompt += ", high quality, detailed, vibrant, 4k"

    negative_prompt = "blurry, low quality, distorted, text, watermark, signature, ugly"

    headers = {
        "Authorization": f"Bearer {active_image_tokens[0] if active_image_tokens else active_text_tokens[0]}",
        "Content-Type": "application/json",
    }

    payload = {
        "model": "wanx2.1-t2i-turbo",
        "input": {
            "prompt": prompt,
            "negative_prompt": negative_prompt,
            "n": 1,
            "size": "1920x1080",
        },
        "parameters": {
            "style": "<auto>",
        },
    }

    try:
        # 提交生成任务
        resp = requests.post(
            "https://api-inference.modelscope.cn/v1/images/generations",
            headers=headers,
            json=payload,
            timeout=(connect_timeout, read_timeout),
        )
        resp.raise_for_status()
        task_data = resp.json()
        task_id = task_data.get("output", {}).get("task_id", "")
        if not task_id:
            log.error("ModelScope 返回中没有 task_id: %s", task_data)
            return False

        # 轮询任务结果
        poll_url = f"https://api-inference.modelscope.cn/v1/images/generations/{task_id}"
        max_polls = 60
        for poll_idx in range(max_polls):
            time.sleep(token_switch_delay)
            poll_resp = requests.get(poll_url, headers=headers, timeout=(poll_connect_timeout, poll_read_timeout))
            if poll_resp.status_code != 200:
                continue

            poll_data = poll_resp.json()
            task_status = poll_data.get("output", {}).get("task_status", "")

            if task_status == "SUCCEEDED":
                image_urls = poll_data.get("output", {}).get("results", [])
                if image_urls:
                    img_url = image_urls[0].get("url", "")
                    if img_url:
                        img_resp = requests.get(img_url, timeout=60)
                        img_resp.raise_for_status()
                        img = Image.open(BytesIO(img_resp.content))
                        img = img.resize((1920, 1080), Image.LANCZOS)
                        img.save(output_path, "JPEG", quality=95)
                        log.info("✅ AI 封面生成成功: %s", os.path.basename(output_path))
                        return True
                break
            elif task_status in ("FAILED", "CANCELED"):
                error_msg = str(poll_data.get("output", {}).get("message", "") or poll_data)
                if is_modelscope_image_review_rejection_error(error_msg):
                    raise CoverGenerationPolicyRejectedError(f"封面审核拒绝: {error_msg}")
                log.warning("ModelScope 任务失败: status=%s, msg=%s", task_status, error_msg[:200])
                break
            elif task_status == "PENDING" or task_status == "RUNNING":
                continue
            else:
                log.warning("ModelScope 未知状态: %s, %s", task_status, str(poll_data)[:200])
                break

        return False
    except CoverGenerationPolicyRejectedError:
        raise
    except requests.exceptions.RequestException as e:
        if is_modelscope_image_review_rejection_error(e):
            raise CoverGenerationPolicyRejectedError(f"封面审核拒绝: {e}") from e
        log.error("ModelScope API 请求失败: %s", e)
        return False


# ============================================================================
# AI SEO 文案生成
# ============================================================================

def auto_create_youtube_seo(book_name, book_desc, output_path, token):
    """使用 ModelScope 通义千问生成 SEO 文案"""
    from pipeline.config import get_config

    connect_timeout = max(1, int(get_config("MODELSCOPE_IMAGE_CONNECT_TIMEOUT", 300)))
    read_timeout = max(1, int(get_config("MODELSCOPE_IMAGE_READ_TIMEOUT", 300)))
    token_switch_delay = max(1, int(get_config("MODELSCOPE_TOKEN_SWITCH_DELAY_SECONDS", 30)))

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    active_text_tokens = _get_modelscope_usage_token_pool(token, "text")
    if not active_text_tokens:
        log.error("ModelScope 没有可用的 text token")
        return False, {}

    headers = {
        "Authorization": f"Bearer {active_text_tokens[0]}",
        "Content-Type": "application/json",
    }

    prompt = f"""你是一个 YouTube 有声书频道的 SEO 专家。请为一本有声书生成以下信息：

书名：{book_name}
简介：{book_desc[:500]}

请提供：
1. 一个吸引人的视频标题（不超过 100 个字符）
2. 一段详细的视频描述（包含关键词，至少 200 字）
3. 10-15 个相关标签（用空格分隔，添加 # 前缀）

请以 JSON 格式返回：
{{"title": "...", "Description": "...", "label": "#标签1 #标签2 ..."}}
"""

    payload = {
        "model": "qwen-turbo",
        "input": {
            "prompt": prompt,
        },
        "parameters": {
            "temperature": 0.7,
            "max_tokens": 2000,
        },
    }

    try:
        resp = requests.post(
            "https://api-inference.modelscope.cn/v1/text/generations",
            headers=headers,
            json=payload,
            timeout=(connect_timeout, read_timeout),
        )
        resp.raise_for_status()
        data = resp.json()

        text = data.get("output", {}).get("text", "")
        if not text:
            log.error("ModelScope 返回为空: %s", data)
            return False, {}

        # 尝试解析 JSON
        try:
            seo_dict = json.loads(text)
        except json.JSONDecodeError:
            # 尝试从文本中提取 JSON
            json_match = re.search(r'\{.*\}', text, re.DOTALL)
            if json_match:
                try:
                    seo_dict = json.loads(json_match.group())
                except json.JSONDecodeError:
                    log.error("无法解析 SEO 返回: %s", text[:500])
                    return False, {}
            else:
                log.error("无法解析 SEO 返回: %s", text[:500])
                return False, {}

        write_json_file(output_path, seo_dict)
        log.info("✅ SEO 文案生成成功")
        return True, seo_dict

    except requests.exceptions.RequestException as e:
        log.error("ModelScope API 请求失败: %s", e)
        return False, {}


def _is_nonempty_local_file(path):
    """检查本地文件是否存在且非空"""
    return bool(path and os.path.exists(path) and os.path.getsize(path) > 0)


def _persist_cover_fallback_image(source_path, target_path):
    """持久化封面回退图片"""
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
        log.warning("封面图片回退处理失败: %s", e)
    return ""