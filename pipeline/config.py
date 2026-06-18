"""配置管理模块 - 集中管理所有运行配置"""
from __future__ import annotations

import os
import sys


DEFAULT_RUNTIME_CONFIG = {
    "POSTGRES_DSN": "",
    "YOUTUBE_CHANNEL_NAME": "",
    "MAX_PROCESS_COUNT": 10,
    "PROJECT_FLAG": "",
    "OUTPUT_ROOT": "/content/",
    "TARGET_CATEGORY": "文学小说",
    "DOWNLOAD_WORKERS": 4,
    "REQUEST_DELAY": 0.3,
    "REQUEST_TIMEOUT": 300,
    "MODELSCOPE_IMAGE_CONNECT_TIMEOUT": 300,
    "MODELSCOPE_IMAGE_READ_TIMEOUT": 300,
    "MODELSCOPE_IMAGE_POLL_CONNECT_TIMEOUT": 300,
    "MODELSCOPE_IMAGE_POLL_READ_TIMEOUT": 300,
    "MODELSCOPE_TOKEN_SWITCH_DELAY_SECONDS": 30,
    "API_PRIORITY_ORDER": "modelscope,sensenova",
    "MAX_RETRIES": 3,
    "AUDIO_DOWNLOAD_CONNECT_TIMEOUT": 20,
    "AUDIO_DOWNLOAD_READ_TIMEOUT": 90,
    "AUDIO_DOWNLOAD_MAX_RETRY_ATTEMPTS": 12,
    "AUDIO_DOWNLOAD_MAX_TOTAL_SECONDS": 1800,
    "AUDIO_DOWNLOAD_STUCK_LOG_INTERVAL_SECONDS": 30,
    "SKIP_EXISTING": True,
    "FORCE_REPROCESS": False,
    "MAX_RUNTIME_HOURS": 11.5,
    "STOP_BUFFER_MINUTES": 20,
    "LONG_AUDIO_SPLIT_TRIGGER_HOURS": 12.0,
    "LONG_AUDIO_PART_TARGET_HOURS": 11.8,
    "BOOK_STATE_TABLE": "book_processing_states",
    "CLEANUP_COMPLETED_SPLIT_STATES": True,
    "PRIORITIZE_INTERRUPTED_BOOKS": True,
    "QUIET_RUNTIME_OUTPUT": True,
    "ENABLE_DEEPFILTER": True,
    "segment_duration_minutes": 60,
    "DEEPFILTER_WORKERS": 2,
    "ENABLE_COVER_GENERATION": True,
    "MODELSCOPE_TOKEN_SOURCE": "database",
    "CLOUD_RUNTIME_SETTINGS_TABLE": "channel_runtime_settings",
    "MODELSCOPE_TOKEN_TABLE": "modelscope_tokens",
    "MODELSCOPE_TOKEN": "",
    "ENABLE_SEO_GENERATION": True,
    "ENABLE_YOUTUBE_UPLOAD": True,
    "YOUTUBE_PRIVACY_STATUS": "schedule",
    "YOUTUBE_SCHEDULE_AFTER_HOURS": 24,
    "YOUTUBE_DAILY_PUBLISH_LIMIT": 3,
    "YOUTUBE_CATEGORY_ID": "",
    "YOUTUBE_DEFAULT_LANGUAGE": "zh-CN",
    "ENABLE_YOUTUBE_TRADITIONAL_LOCALIZATION": True,
    "YOUTUBE_LOCALIZATION_LOCALES": "zh-TW,zh-HK,zh-SG,zh-Hant",
    "YOUTUBE_TRADITIONAL_LOCALE": "zh-TW",
    "YOUTUBE_TRADITIONAL_OPENCC_CONFIG": "s2t",
    "ENABLE_AUTO_INSTALL_OPENCC": True,
    "APPEND_TAGS_TO_TITLE": False,
    "APPEND_TAGS_TO_DESC": True,
    "ENABLE_VIDEO_GENERATION": True,
    "VIDEO_RESOLUTION": "1080p",
    "DOWNLOAD_FROM_BUCKETS": True,
    "HF_MUSIC_DOWNLOAD_METHOD": "datasets_zip_urls",
    "HF_DATASET_ZIP_URLS_SOURCE": "database",
    "HF_DATASET_ZIP_URLS": "",
    "BUCKET_IDS_SOURCE": "database",
    "BUCKET_IDS": "",
    "HF_TOKEN": "",
    "LOCAL_MUSIC_DIR": "/content/music",
    "ENABLE_BGM_MIX": True,
    "MUSIC_DIR": "/content/music",
    "VOLUME_OFFSET_DB": -25,
    "HIGHPASS_FREQ": 150,
    "FADE_DURATION_MS": 3000,
    "MIN_VOLUME_DB": -40,
    "ENABLE_DYNAMIC_VOLUME": True,
    "ENABLE_SPECTRAL_SHAPING": True,
    "STEREO_OFFSET": 0.0,
    # Podcast runtime defaults（后续由 podcast 模块补充）
    "ENABLE_YOUTUBE_PODCAST_RUNTIME": True,
    "ENABLE_YOUTUBE_PODCAST_UNIFIED_SHOW": True,
    "ENABLE_YOUTUBE_PODCAST_SPLIT_PLAYLIST": True,
    "YOUTUBE_PODCAST_SHOW_TITLE_TEMPLATE": "{channel_name}｜长篇有声书全集",
    "YOUTUBE_PODCAST_IMAGE_SIZE": 2048,
    "YOUTUBE_PODCAST_IMAGE_MAX_BYTES": 2097152,
    "YOUTUBE_PODCAST_SHOW_PLAYLIST_SETTING_KEY": "podcast_longform_show_playlist_id",
    "SENSENOVA_BASE_URL": "https://token.sensenova.cn/v1",
    "SENSENOVA_API_KEY": "",
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

# 将所有默认配置注入模块级别（供 from pipeline.config import * 使用）
globals().update(DEFAULT_RUNTIME_CONFIG)


def apply_runtime_config(runtime_config: dict | None = None):
    """合并运行时配置并注入所有 pipeline 模块的命名空间"""
    merged = dict(DEFAULT_RUNTIME_CONFIG)
    if runtime_config:
        merged.update(runtime_config)

    # 处理 PROJECT_FLAG 自动回退
    if not str(merged.get("PROJECT_FLAG", "") or "").strip():
        merged["PROJECT_FLAG"] = str(merged.get("YOUTUBE_CHANNEL_NAME", "") or "").strip()

    # 处理 MUSIC_DIR 自动回退
    if not str(merged.get("MUSIC_DIR", "") or "").strip():
        merged["MUSIC_DIR"] = str(merged.get("LOCAL_MUSIC_DIR", "") or "").strip()

    # 更新本模块的全局变量
    globals().update(merged)

    # 同步到所有已加载的 pipeline 子模块（保障函数内直接引用配置变量正常工作）
    for mod_name, mod in list(sys.modules.items()):
        if mod_name == __name__:
            continue
        if mod_name == "pipeline" or mod_name.startswith("pipeline."):
            mod.__dict__.update(merged)

    return merged


def get_config(key, default=None):
    """安全获取配置值"""
    return globals().get(key, default)


def __getattr__(name):
    """支持 from pipeline.config import SOME_VAR 获取配置变量"""
    if name in DEFAULT_RUNTIME_CONFIG:
        return DEFAULT_RUNTIME_CONFIG[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


class RuntimeConfig(dict):
    """运行时配置类 - 兼容 colab_loader.ipynb 的 RuntimeConfig.from_dict 调用方式"""

    @classmethod
    def from_dict(cls, config_dict: dict) -> "RuntimeConfig":
        """从字典创建 RuntimeConfig 实例"""
        instance = cls(config_dict or {})
        return instance

    def to_dict(self) -> dict:
        """返回配置字典"""
        return dict(self)
