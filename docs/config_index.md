# 配置项索引

> 全部配置项定义在 `pipeline/config.py` 的 `DEFAULT_RUNTIME_CONFIG` 字典中，约 90 个。

## 1. 数据库

| 配置键 | 默认值 | 说明 |
|--------|--------|------|
| `POSTGRES_DSN` | `""` | PostgreSQL 连接串，格式 `postgresql://user:pass@host:5432/db?sslmode=require` |
| `BOOK_STATE_TABLE` | `"book_processing_states"` | 断点续跑状态表名 |
| `MODELSCOPE_TOKEN_TABLE` | `"modelscope_tokens"` | ModelScope Token 专用表名 |
| `CLOUD_RUNTIME_SETTINGS_TABLE` | `"channel_runtime_settings"` | 云端运行时设置表名 |

## 2. 基本运行参数

| 配置键 | 默认值 | 说明 |
|--------|--------|------|
| `YOUTUBE_CHANNEL_NAME` | `""` | YouTube 频道名称（必填） |
| `MAX_PROCESS_COUNT` | `10` | 本次最多处理 N 本书，0 不限制 |
| `PROJECT_FLAG` | `""` | 项目标记，留空自动回落为频道名 |
| `OUTPUT_ROOT` | `"/content/"` | 输出根目录 |
| `TARGET_CATEGORY` | `"文学小说"` | 分类过滤，空=全部 |
| `QUIET_RUNTIME_OUTPUT` | `True` | 静默输出模式 |
| `MAX_RUNTIME_HOURS` | `11.5` | Colab 单次运行时长上限（小时） |
| `STOP_BUFFER_MINUTES` | `20` | 停止缓冲时间（分钟） |
| `SKIP_EXISTING` | `True` | 跳过已存在的文件 |
| `FORCE_REPROCESS` | `False` | 强制重新处理所有书籍 |

## 3. 下载参数

| 配置键 | 默认值 | 说明 |
|--------|--------|------|
| `DOWNLOAD_WORKERS` | `4` | 并发下载线程数 |
| `REQUEST_DELAY` | `0.3` | 章节间等待秒数 |
| `REQUEST_TIMEOUT` | `300` | HTTP 超时（秒） |
| `MAX_RETRIES` | `3` | 普通文件最大重试次数 |
| `AUDIO_DOWNLOAD_CONNECT_TIMEOUT` | `20` | 音频 TCP 连接超时（秒） |
| `AUDIO_DOWNLOAD_READ_TIMEOUT` | `90` | 音频读取超时（秒） |
| `AUDIO_DOWNLOAD_MAX_RETRY_ATTEMPTS` | `12` | 音频最大重试次数 |
| `AUDIO_DOWNLOAD_MAX_TOTAL_SECONDS` | `1800` | 音频总耗时上限（秒） |
| `AUDIO_DOWNLOAD_STUCK_LOG_INTERVAL_SECONDS` | `30` | 卡住检测日志间隔（秒） |

## 4. 长音频分片 / 断点续跑

| 配置键 | 默认值 | 说明 |
|--------|--------|------|
| `LONG_AUDIO_SPLIT_TRIGGER_HOURS` | `12.0` | 分片触发阈值（小时） |
| `LONG_AUDIO_PART_TARGET_HOURS` | `11.8` | 每片目标时长（小时） |
| `CLEANUP_COMPLETED_SPLIT_STATES` | `True` | 自动清理已完成的分片状态 |
| `PRIORITIZE_INTERRUPTED_BOOKS` | `True` | 优先处理有续跑状态的书籍 |

## 5. DeepFilter 降噪

| 配置键 | 默认值 | 说明 |
|--------|--------|------|
| `ENABLE_DEEPFILTER` | `True` | 启用 DeepFilterNet 降噪 |
| `segment_duration_minutes` | `60` | 降噪分片时长（分钟） |
| `DEEPFILTER_WORKERS` | `2` | 降噪并行线程数 |

## 6. AI 封面生成（ModelScope）

| 配置键 | 默认值 | 说明 |
|--------|--------|------|
| `ENABLE_COVER_GENERATION` | `True` | 启用 AI 封面生成 |
| `MODELSCOPE_TOKEN_SOURCE` | `"database"` | Token 来源（database / local） |
| `MODELSCOPE_TOKEN` | `""` | 本地固定 Token |
| `MODELSCOPE_IMAGE_CONNECT_TIMEOUT` | `300` | 图片生成连接超时（秒） |
| `MODELSCOPE_IMAGE_READ_TIMEOUT` | `300` | 图片生成读取超时（秒） |
| `MODELSCOPE_IMAGE_POLL_CONNECT_TIMEOUT` | `300` | 轮询连接超时（秒） |
| `MODELSCOPE_IMAGE_POLL_READ_TIMEOUT` | `300` | 轮询读取超时（秒） |
| `MODELSCOPE_TOKEN_SWITCH_DELAY_SECONDS` | `30` | Token 轮换等待时间（秒） |
| `API_PRIORITY_ORDER` | `"modelscope,sensenova"` | API 优先级顺序 |

## 7. SEO 文本生成

| 配置键 | 默认值 | 说明 |
|--------|--------|------|
| `ENABLE_SEO_GENERATION` | `True` | 启用 AI SEO 文案生成 |

## 8. YouTube 上传

| 配置键 | 默认值 | 说明 |
|--------|--------|------|
| `ENABLE_YOUTUBE_UPLOAD` | `True` | 启用 YouTube 上传 |
| `YOUTUBE_PRIVACY_STATUS` | `"schedule"` | 发布权限（private/unlisted/public/schedule） |
| `YOUTUBE_SCHEDULE_AFTER_HOURS` | `24` | 预约延迟（小时） |
| `YOUTUBE_DAILY_PUBLISH_LIMIT` | `3` | 每日发布上限 |
| `YOUTUBE_CATEGORY_ID` | `""` | 视频分类 ID |
| `YOUTUBE_DEFAULT_LANGUAGE` | `"zh-CN"` | 视频默认语言 |

### 繁体中文化

| 配置键 | 默认值 | 说明 |
|--------|--------|------|
| `ENABLE_YOUTUBE_TRADITIONAL_LOCALIZATION` | `True` | 自动生成繁体中文本地化 |
| `YOUTUBE_LOCALIZATION_LOCALES` | `"zh-TW,zh-HK,zh-SG,zh-Hant"` | 本地化语言地区 |
| `YOUTUBE_TRADITIONAL_LOCALE` | `"zh-TW"` | 主要繁体地区 |
| `YOUTUBE_TRADITIONAL_OPENCC_CONFIG` | `"s2t"` | OpenCC 转换配置 |
| `ENABLE_AUTO_INSTALL_OPENCC` | `True` | 自动安装 OpenCC |

### 标签追加

| 配置键 | 默认值 | 说明 |
|--------|--------|------|
| `APPEND_TAGS_TO_TITLE` | `False` | 标签追加到标题 |
| `APPEND_TAGS_TO_DESC` | `True` | 标签追加到描述 |

## 9. 视频生成

| 配置键 | 默认值 | 说明 |
|--------|--------|------|
| `ENABLE_VIDEO_GENERATION` | `True` | 启用 MP4 视频封装 |
| `VIDEO_RESOLUTION` | `"1080p"` | 视频分辨率（720p / 1080p） |

## 10. 音乐 / BGM

| 配置键 | 默认值 | 说明 |
|--------|--------|------|
| `DOWNLOAD_FROM_BUCKETS` | `True` | 从 Hugging Face 下载音乐 |
| `HF_MUSIC_DOWNLOAD_METHOD` | `"datasets_zip_urls"` | 下载方式（datasets_zip_urls / buckets） |
| `HF_DATASET_ZIP_URLS_SOURCE` | `"database"` | ZIP URL 来源（database / local） |
| `HF_DATASET_ZIP_URLS` | `""` | ZIP URL 固定值 |
| `BUCKET_IDS_SOURCE` | `"database"` | Bucket ID 来源（database / local） |
| `BUCKET_IDS` | `""` | Bucket ID 固定值 |
| `HF_TOKEN` | `""` | Hugging Face API Token |
| `LOCAL_MUSIC_DIR` | `"/content/music"` | 本地音乐目录 |
| `MUSIC_DIR` | `"/content/music"` | BGM 源目录 |

### BGM 混音参数

| 配置键 | 默认值 | 说明 |
|--------|--------|------|
| `ENABLE_BGM_MIX` | `True` | 启用 BGM 混音 |
| `VOLUME_OFFSET_DB` | `-25` | BGM 音量偏移（dB） |
| `HIGHPASS_FREQ` | `150` | BGM 高通滤波频率（Hz） |
| `FADE_DURATION_MS` | `3000` | 淡入淡出时长（ms） |
| `MIN_VOLUME_DB` | `-40` | 最小音量阈值（dB） |
| `ENABLE_DYNAMIC_VOLUME` | `True` | 动态音量均衡 |
| `ENABLE_SPECTRAL_SHAPING` | `True` | 频谱塑形增强 |
| `STEREO_OFFSET` | `0.0` | 立体声偏移（ms） |

## 11. YouTube Podcast

| 配置键 | 默认值 | 说明 |
|--------|--------|------|
| `ENABLE_YOUTUBE_PODCAST_RUNTIME` | `True` | 启用 Podcast 模式 |
| `ENABLE_YOUTUBE_PODCAST_UNIFIED_SHOW` | `True` | 统一 Show 播放列表 |
| `ENABLE_YOUTUBE_PODCAST_SPLIT_PLAYLIST` | `True` | 分片播放列表 |
| `YOUTUBE_PODCAST_SHOW_TITLE_TEMPLATE` | `"{channel_name}｜长篇有声书全集"` | Show 标题模板 |
| `YOUTUBE_PODCAST_IMAGE_SIZE` | `2048` | 封面尺寸（像素） |
| `YOUTUBE_PODCAST_IMAGE_MAX_BYTES` | `2097152` | 封面文件上限（字节） |
| `YOUTUBE_PODCAST_SHOW_PLAYLIST_SETTING_KEY` | `"podcast_longform_show_playlist_id"` | Show 播放列表 ID 存储键 |

### Podcast AI

| 配置键 | 默认值 | 说明 |
|--------|--------|------|
| `SENSENOVA_BASE_URL` | `"https://token.sensenova.cn/v1"` | Sensenova API 基础地址 |
| `SENSENOVA_API_KEY` | `""` | Sensenova API 密钥 |
| `YOUTUBE_PODCAST_TEXT_MODEL_PRIMARY` | `"deepseek-v4-flash"` | 文案主模型 |
| `YOUTUBE_PODCAST_TEXT_MODEL_FALLBACK` | `"sensenova-6.7-flash-lite"` | 文案备选模型 |
| `YOUTUBE_PODCAST_IMAGE_MODEL_PRIMARY` | `"sensenova-u1-fast"` | 封面生成模型 |
| `YOUTUBE_PODCAST_TEXT_MODEL_RETRIES` | `2` | 文本生成重试次数 |
| `YOUTUBE_PODCAST_IMAGE_MODEL_RETRIES` | `3` | 图片生成重试次数 |
| `YOUTUBE_PODCAST_AI_RETRY_BASE_SECONDS` | `30.0` | AI 重试基础等待（秒） |
| `YOUTUBE_PODCAST_YT_RETRIES` | `5` | YouTube API 重试次数 |
| `YOUTUBE_PODCAST_YT_RETRY_BASE_SECONDS` | `3.0` | YouTube API 重试等待（秒） |
| `YOUTUBE_PODCAST_FONT_CACHE_DIRNAME` | `"_podcast_font_cache"` | 封面字体缓存目录 |
