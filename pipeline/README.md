# pipeline 包 — 模块说明

## 模块依赖层级（从低到高）

```
config → constants → log_utils → db → utils → modelscope → youtube → state → audio → music_library → podcast → core
```

**注意**：新增模块不可反向依赖 `core.py`，否则产生循环引用。

---

## 模块功能一览

| 模块 | 文件 | 行数 | 职责 |
|------|------|------|------|
| **config** | `config.py` | 156 | 配置管理核心，`DEFAULT_RUNTIME_CONFIG` 约 90 个配置项，`apply_runtime_config()` 合并并注入所有子模块，`RuntimeConfig` 类兼容 `colab_loader.ipynb` |
| **constants** | `constants.py` | 120 | 跨模块共享的纯常量集合（时间单位、默认值、错误模板、枚举值），无任何内部依赖 |
| **log_utils** | `log_utils.py` | 70 | `SimpleLogger` 日志类、`runtime_console_print` 静默输出控制、Colab 输出清空 |
| **db** | `db.py` | 71 | PostgreSQL 操作封装，`get_postgres_dsn()`、`execute_postgres_fetchone/fetchall/execute/fetchval` |
| **utils** | `utils.py` | 240 | 通用工具：`sanitize_filename`、`normalize_text_items`、`download_file`、`format_seconds_hhmmss` 等 |
| **modelscope** | `modelscope.py` | 556 | ModelScope 令牌池管理、AI 封面生成（通义万相）、AI SEO 文案生成（通义千问） |
| **youtube** | `youtube.py` | 604 | YouTube OAuth 认证、视频上传（分块）、播放列表管理、繁体中文本地化 |
| **state** | `state.py` | 1019 | 断点续跑状态管理：`load_split_processing_state`、`save_split_processing_state`、`reconcile_split_part_upload_states` 等 |
| **audio** | `audio.py` | 1147 | 音频处理：DeepFilter 降噪、BGM 混音、FFmpeg 合并、频谱分析、视频封装 |
| **music_library** | `music_library.py` | 341 | 从 Hugging Face Dataset/Buckets 下载版权音乐库 |
| **podcast** | `podcast.py` | 1684 | YouTube Podcast 管理：播放列表创建/更新、封面生成、Show 合并 |
| **core** | `core.py` | 1241 | 主流程编排：`run_pipeline()` 入口、`process_book()`、`process_standard_book()`、`process_split_book()` |

---

## 主流程架构

```
run_pipeline(config)
    │
    ├── apply_runtime_config()     → 注入配置
    ├── validate_runtime_config()  → 校验参数
    │
    ├── [分页循环] 读取 books 表
    │   │
    │   ├── process_book(record)
    │   │   ├── process_standard_book()    ← 时长 < 12 小时
    │   │   │   ├── download_audio         → audio.download_chapter_items
    │   │   │   ├── denoise_audio          → audio.denoise_audio_paths_parallel
    │   │   │   ├── mix_with_bgm           → audio.mix_with_bgm
    │   │   │   ├── generate_cover         → modelscope.auto_create_youtube_cover
    │   │   │   ├── generate_seo           → modelscope.auto_create_youtube_seo
    │   │   │   └── upload_video           → youtube.upload_youtube_video
    │   │   │
    │   │   └── process_split_book()       ← 时长 ≥ 12 小时
    │   │       ├── building_split_part_plans → 分片计划
    │   │       ├── [对每个分片] 下载+降噪+混音+上传
    │   │       └── sync_split_playlist_podcast → Podcast 同步
    │   │
    │   └── finalize_book_result() → 标记完成
    │
    └── save_run_summary() → 输出 JSON 汇总
```

---

## 向后兼容

`colab_loader.ipynb` 通过以下方式兼容：

```python
from pipeline.config import RuntimeConfig     # 类型安全的配置类
from pipeline.core import run_pipeline        # 主流程入口
from pipeline import *                        # 全部符号（通过 __init__.py re-export ~227 个符号）
```
