# 待优化清单

> 勾选标记 `[x]` 表示已完成，`[ ]` 表示待处理

---

## 1. 代码结构优化

- [x] **`run_pipeline` 集成 Podcast 后置同步** — 当前 `core.py` 中的 `run_pipeline()` 在处理完书籍后未调用 `_podcast_sync_split_playlist_podcast()`，导致分片书籍上传后不会自动同步到 Podcast 播放列表
- [x] **消除 `runtime_core.py` 的重复依赖** — 原始文件已移入 `archive/runtime_core.py`，仓库源码不再引用
- [x] **`podcast.py` 的函数命名规范化** — 当前 `_podcast_*` 以下划线开头表示私有函数，部分函数在 `core.py` 的主流程中需要被调用，应考虑统一公开接口
- [x] **模块间循环引用检查** — 当前 `core.py` 导入了多个子模块，`__init__.py` 中又有互相引用，需要确保在 Colab 环境下不会出现循环导入
- [x] **常量和配置项集中管理** — 已创建 `pipeline/constants.py`，集中管理所有跨模块共享常量

## 2. 性能优化

- [ ] **音频下载并行度可配置化** — 当前 `ThreadPoolExecutor` 的线程数未暴露为配置项，应加到 `config.py` 的 `DEFAULT_RUNTIME_CONFIG` 中
- [ ] **DeepFilter 降噪模型缓存** — 每次调用 `denoise_audio_paths_parallel` 都可能重新加载模型，应实现单例/缓存机制
- [ ] **BGM 混音跳过策略** — 当 `music_downloads` 目录为空时，应跳过 BGM 混音而不报错，减少无意义的处理时间
- [ ] **YouTube 上传失败快速重试** — 当前上传失败后直接标记错误，应增加指数退避重试机制
- [ ] **分片状态写入合并** — `process_split_book` 中每个分片完成后都调用 `save_split_processing_state`，可批量合并减少数据库写入次数

## 3. 安全优化

- [ ] **ModelScope Token 内存安全** — Token 在 `token_pool` 中以明文存在内存中，处理完毕后应主动清除
- [ ] **YouTube 凭证泄露防护** — `authenticate_youtube_from_supabase` 返回的凭证对象应限制作用域，避免在不必要的日志中打印
- [ ] **PostgreSQL DSN 日志脱敏** — `get_postgres_dsn()` 的返回值中可能包含密码，日志输出时应脱敏处理
- [ ] **配置文件权限** — 本地配置中的 `POSTGRES_DSN`、`MODELSCOPE_TOKEN` 等敏感信息应支持从环境变量读取，而非仅依赖配置文件

## 4. 错误处理与健壮性

- [ ] **音频下载断点续传** — `download_chapter_items` 中的 HTTP 下载应支持 `Range` 头断点续传，避免大文件下载中断后从头重来
- [ ] **FFmpeg 进程超时保护** — `merge_audio_ffmpeg` 和 `generate_video` 中调用 FFmpeg 子进程应设置超时，防止进程卡死
- [ ] **数据库连接池** — 当前每次查询都创建新连接，应实现连接池复用
- [ ] **Colab 运行时断线重连** — `run_pipeline` 中的主循环应捕获 `KeyboardInterrupt` 和 Colab 运行时断开信号，优雅保存进度
- [ ] **分片状态版本兼容** — `state.py` 中的 `state_version` 为 5，未来更新格式时需提供迁移逻辑

## 5. 测试覆盖

- [ ] **单元测试：`config.py`** — 测试 `apply_runtime_config`、`get_config`、`RuntimeConfig.from_dict`
- [ ] **单元测试：`utils.py`** — 测试 `sanitize_filename`、`normalize_text_items`、`build_supabase_text_update`
- [ ] **单元测试：`db.py`** — 使用 mock 测试 SQL 构建和执行
- [ ] **单元测试：`audio.py`** — 测试 `build_split_part_plans` 分片逻辑
- [ ] **单元测试：`state.py`** — 测试状态初始化、保存、加载、协调
- [ ] **集成测试：`process_book`** — 使用 mock 的 chapters_data 测试标准模式和分片模式
- [ ] **集成测试：`run_pipeline`** — 完整的端到端流程测试（mock 数据库和 YouTube API）

## 6. 文档完善

- [ ] **模块 README** — 为 `pipeline/` 包编写使用说明，含每个模块的功能描述
- [ ] **配置项索引** — 列出 `DEFAULT_RUNTIME_CONFIG` 中所有 ~90 个配置项的说明和默认值
- [ ] **YouTube API 授权说明** — 记录如何获取和配置 YouTube OAuth 凭据
- [ ] **ModelScope 申请指南** — 记录如何申请 ModelScope Token 和配置 AI 封面/SEO
- [ ] **数据库表结构文档** — 记录 `books`、`book_processing_states`、`modelscope_tokens` 等表的字段说明

## 7. 依赖管理

- [ ] **生成 `requirements.txt`** — 从所有模块的 import 中提取完整依赖清单
- [ ] **依赖版本锁定** — 确认 `google-api-python-client`、`psycopg`、`modelscope` 等关键库的兼容版本
- [ ] **Colab 环境检测** — 在 `config.py` 中添加自动检测是否运行在 Colab 环境中的逻辑

## 8. Podcast 功能完善

- [ ] **`_podcast_sync_split_playlist_podcast` 调用接入** — 在 `core.py` 的 `process_split_book` 完成后调用 Podcast 同步，并传递结果到 `BookResult`
- [ ] **Podcast 封面生成失败回退** — 当前 `_podcast_generate_show_cover_image` 中 AI 封面生成失败后没有回退到本地占位图
- [ ] **播放列表并发创建保护** — `_podcast_create_plain_playlist` 和 `_podcast_update_playlist` 在并发场景下可能冲突

---

> 最后更新：2026-06-18
