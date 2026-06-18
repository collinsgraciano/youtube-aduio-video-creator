"""集中管理所有硬编码常量和配置默认值

本模块为纯常量集合，无任何 pipeline 内部依赖，可被任何模块安全导入。
"""
from __future__ import annotations

# ============================================================================
# 时间单位常量
# ============================================================================
SECONDS_IN_MINUTE = 60
SECONDS_IN_HOUR = 3600
MINUTES_IN_HOUR = 60
HOURS_IN_DAY = 24

# ============================================================================
# 书籍处理常量
# ============================================================================
MIN_BOOK_DURATION_SECONDS = 30 * SECONDS_IN_MINUTE  # 30 分钟
"""预估总时长低于此值的书籍直接跳过并标记 bad"""

# ============================================================================
# 数据库常量
# ============================================================================
POSTGRES_SCHEMA = "public"
"""PostgreSQL 默认 schema 名称"""

DEFAULT_BOOK_STATE_TABLE = "book_processing_states"
"""断点续跑状态表默认名"""

DEFAULT_CLOUD_RUNTIME_SETTINGS_TABLE = "channel_runtime_settings"
"""云端运行时设置表默认名"""

DEFAULT_MODELSCOPE_TOKEN_TABLE = "modelscope_tokens"
"""ModelScope Token 表默认名"""

DEFAULT_YOUTUBE_CREDENTIALS_TABLE = "youtube_credentials"
"""YouTube 凭证表默认名"""

# ============================================================================
# 音频/文件常量
# ============================================================================
SUPPORTED_AUDIO_EXTENSIONS = (".mp3", ".wav", ".flac", ".m4a", ".ogg", ".aac", ".wma")
"""支持的音频文件扩展名"""

DEFAULT_AUDIO_ENCODING = "mp3"
"""默认音频编码格式"""

ILLEGAL_FILENAME_CHARS_PATTERN = r'[\\/:*?"<>|\x00-\x1f]'
"""文件名非法字符正则"""

AUDIO_CHUNK_SIZE_BYTES = 8192
"""音频读取块大小（字节）"""

# ============================================================================
# 配置校验错误消息模板
# ============================================================================
ERR_REQUIRED_FIELD = "%s 为空"
ERR_MUST_BE_POSITIVE = "%s 必须大于 0"
ERR_INVALID_CHOICE = "%s 只能是 %s"
ERR_TARGET_CATEGORY_INVALID = "TARGET_CATEGORY 值无效: %s"
ERR_AI_TOKEN_EMPTY = "启用 AI 生成时，%s 不能为空"
ERR_AI_TOKEN_SOURCE_LOCAL = "MODELSCOPE_TOKEN_SOURCE=local，但 MODELSCOPE_TOKEN 为空"
ERR_YOUTUBE_CHANNEL_REQUIRED = "已开启 YouTube 上传，但 YOUTUBE_CHANNEL_NAME 为空"
ERR_YOUTUBE_PRIVACY_SCHEDULE = "YOUTUBE_PRIVACY_STATUS=schedule 但预约小时数不大于 0，将回退到最小值 1"

# ============================================================================
# 配置校验警告消息模板
# ============================================================================
WARN_COLAB_TMP_DIR = "当前 OUTPUT_ROOT 位于 Colab 临时盘，断线或重启后文件会丢；长期自用更建议改到 Google Drive 路径"
WARN_RUNTIME_HOURS = "Colab 单次常见上限约 12 小时，建议 MAX_RUNTIME_HOURS 小于 12，给收尾留缓冲"
WARN_MUSIC_DIR_MISSING = "已开启 BGM 混音，但本地 MUSIC_DIR 不存在"
WARN_ZIP_URLS_EMPTY = "已开启 Hugging Face 音乐下载，但 HF_DATASET_ZIP_URLS 为空"
WARN_BUCKET_IDS_EMPTY = "已选择 buckets 下载模式，但 BUCKET_IDS 为空"

# ============================================================================
# 枚举值常量
# ============================================================================
VALID_SOURCE_VALUES = ("database", "local")
"""数据库或本地来源的合法取值"""

VALID_DOWNLOAD_METHODS = ("datasets_zip_urls", "buckets")
"""音乐下载方式的合法取值"""

VALID_PRIVACY_STATUSES = ("private", "unlisted", "public", "schedule")
"""YouTube 发布权限的合法取值"""

VALID_VIDEO_RESOLUTIONS = ("720p", "1080p")
"""视频分辨率合法取值"""

VALID_CATEGORY_CHOICES = (
    "全部", "文学小说", "历史传记", "玄幻奇幻", "武侠仙侠",
    "悬疑推理", "科幻灵异", "都市言情", "经管励志", "人文社科",
    "少儿教育", "其他",
)
"""书籍分类合法取值"""

# ============================================================================
# YouTube 常量
# ============================================================================
YOUTUBE_UPLOAD_CHUNK_SIZE_BYTES = 256 * 1024  # 256KB
"""YouTube 上传分块大小"""
YOUTUBE_MAX_TITLE_LENGTH = 100
"""YouTube 视频标题最大长度"""
YOUTUBE_MAX_DESCRIPTION_LENGTH = 5000
"""YouTube 视频描述最大长度"""
YOUTUBE_MAX_TAGS_COUNT = 500
"""YouTube 视频标签最大数量"""
YOUTUBE_MAX_RETRY_QUOTA_EXCEEDED = 3
"""配额超限时的最大重试次数"""

# ============================================================================
# DeepFilter 降噪常量
# ============================================================================
DEEPFILTER_DEFAULT_SEGMENT_MINUTES = 60
"""DeepFilter 降噪时的默认分片时长（分钟）"""
DEEPFILTER_DEFAULT_WORKERS = 2
"""DeepFilter 默认并行线程数"""
DEEPFILTER_BINARY_NAME = "deepfilter"
"""DeepFilter 可执行文件名"""
DEEPFILTER_DOWNLOAD_TIMEOUT = 120
"""DeepFilter 下载超时（秒）"""

# ============================================================================
# BGM 混音常量
# ============================================================================
BGM_DEFAULT_VOLUME_OFFSET_DB = -25
"""BGM 默认音量偏移（dB）"""
BGM_DEFAULT_HIGHPASS_FREQ = 150
"""BGM 默认高通滤波频率（Hz）"""
BGM_DEFAULT_FADE_DURATION_MS = 3000
"""BGM 默认淡入淡出时长（毫秒）"""
BGM_DEFAULT_MIN_VOLUME_DB = -40
"""BGM 最小音量阈值（dB）"""
BGM_DEFAULT_STEREO_OFFSET = 0.0
"""BGM 默认立体声偏移（毫秒）"""

# ============================================================================
# 日志常量
# ============================================================================
LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")
"""合法日志级别"""
DEFAULT_LOG_LEVEL = "INFO"
"""默认日志级别"""
LOG_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
"""日志时间戳格式"""
