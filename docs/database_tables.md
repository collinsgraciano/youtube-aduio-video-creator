# 数据库表结构文档

> 以下所有表位于 `public` schema 下，建表 SQL 可在 `colab_loader.ipynb` 第 3 单元中开启开关查看。

---

## 1. `books` — 书籍主表

存储从各大听书网站爬取的有声书记录。

| 字段 | 类型 | 约束 | 说明 |
|------|------|------|------|
| `book_id` | `text` | PRIMARY KEY | 书籍唯一 ID |
| `book_name` | `text` | | 书名 |
| `author` | `text` | | 作者 |
| `category` | `text` | | 分类（文学小说 / 历史传记 / ...） |
| `total_chapters` | `integer` | | 总章节数 |
| `book_data` | `jsonb` | | 章节数据（含每个章节的 URL、标题、时长等） |
| `tags` | `text[]` | | 标签数组 |
| `note` | `text` | | 备注 |
| `status` | `text` | DEFAULT `''` | 处理状态标记（如 `"youtube频道名"` 表示已处理） |
| `created_at` | `timestamptz` | DEFAULT `now()` | 创建时间 |
| `updated_at` | `timestamptz` | DEFAULT `now()` | 更新时间（由触发器自动维护） |

### 索引

| 索引名 | 字段 | 类型 |
|--------|------|------|
| `idx_books_category` | `category` | B-tree |
| `idx_books_tags_gin` | `tags` | GIN |
| `idx_books_status` | `status` | B-tree |
| `idx_books_updated_at` | `updated_at DESC` | B-tree |

### 触发器

- `books_updated_at`: `BEFORE UPDATE` 时自动更新 `updated_at` 字段

---

## 2. `task_queue` — 任务队列

用于多 worker 互斥领取任务的队列表，支持 `FOR UPDATE SKIP LOCKED` 并发安全。

| 字段 | 类型 | 约束 | 说明 |
|------|------|------|------|
| `book_id` | `text` | PRIMARY KEY | 书籍 ID |
| `status` | `text` | NOT NULL DEFAULT `'pending'` | 状态（pending / processing / completed / failed） |
| `worker_id` | `text` | | 领取该任务的 worker ID |
| `claimed_at` | `timestamptz` | | 任务领取时间 |
| `finished_at` | `timestamptz` | | 任务完成时间 |
| `retry_count` | `integer` | NOT NULL DEFAULT `0` | 重试次数 |
| `error_msg` | `text` | | 错误信息 |
| `category` | `text` | NOT NULL DEFAULT `''` | 分类（用于按分类分配任务） |
| `created_at` | `timestamptz` | NOT NULL DEFAULT `now()` | 创建时间 |

### 索引

| 索引名 | 字段 |
|--------|------|
| `idx_task_queue_status` | `status` |
| `idx_task_queue_status_created_at` | `status, created_at` |
| `idx_task_queue_category_status` | `category, status, created_at` |
| `idx_task_queue_claimed_at` | `claimed_at` |

### 函数

| 函数名 | 说明 |
|--------|------|
| `claim_next_task(p_worker_id, p_batch_size)` | 领取待处理任务（原子操作，`FOR UPDATE SKIP LOCKED`） |
| `mark_task_failed_with_retry(p_book_id, p_error_msg, p_max_retry)` | 标记任务失败，未超重试次数时回退为 pending |

---

## 3. `youtube_credentials` — YouTube OAuth 凭证

存储每个频道的 YouTube OAuth 凭证 JSON。

| 字段 | 类型 | 约束 | 说明 |
|------|------|------|------|
| `channel_name` | `text` | PRIMARY KEY | YouTube 频道名称 |
| `token_json` | `jsonb` | NOT NULL | OAuth 2.0 凭证 JSON |
| `created_at` | `timestamptz` | NOT NULL DEFAULT `now()` | 创建时间 |
| `updated_at` | `timestamptz` | NOT NULL DEFAULT `now()` | 更新时间 |

---

## 4. `modelscope_tokens` — ModelScope 令牌池

存储用于 AI 封面生成的 ModelScope API Token，支持多 token 轮换。

| 字段 | 类型 | 约束 | 说明 |
|------|------|------|------|
| `channel_name` | `text` | PRIMARY KEY | 频道名称（或 Token 标识） |
| `token_text` | `text` | NOT NULL | ModelScope API Token |
| `created_at` | `timestamptz` | NOT NULL DEFAULT `now()` | 创建时间 |
| `updated_at` | `timestamptz` | NOT NULL DEFAULT `now()` | 更新时间 |

### 索引

| 索引名 | 字段 |
|--------|------|
| `idx_modelscope_tokens_updated_at` | `updated_at DESC` |

> 表名可通过 `MODELSCOPE_TOKEN_TABLE` 配置。

---

## 5. `channel_runtime_settings` — 云端运行时设置

存储全局共享的运行时配置（如 Token、ZIP URL、Bucket ID 等），运行时按需读取。

| 字段 | 类型 | 约束 | 说明 |
|------|------|------|------|
| `channel_name` | `text` | PRIMARY KEY（复合） | 频道名称，`__shared__` 表示全局共享 |
| `setting_key` | `text` | PRIMARY KEY（复合） | 配置键名 |
| `setting_value` | `text` | NOT NULL DEFAULT `''` | 配置值 |
| `created_at` | `timestamptz` | NOT NULL DEFAULT `now()` | 创建时间 |
| `updated_at` | `timestamptz` | NOT NULL DEFAULT `now()` | 更新时间 |

### 索引

| 索引名 | 字段 |
|--------|------|
| `idx_channel_runtime_settings_updated_at` | `updated_at DESC` |

> 表名可通过 `CLOUD_RUNTIME_SETTINGS_TABLE` 配置。

---

## 6. `book_processing_states` — 断点续跑状态

存储长音频分片的上传进度，支持跨会话断点续跑。

| 字段 | 类型 | 约束 | 说明 |
|------|------|------|------|
| `book_id` | `text` | PRIMARY KEY（复合） | 书籍 ID |
| `project_flag` | `text` | PRIMARY KEY（复合） | 项目标记（一般是频道名） |
| `book_name` | `text` | | 书名 |
| `category` | `text` | | 分类 |
| `pending_resume` | `boolean` | NOT NULL DEFAULT `true` | 是否需要续跑 |
| `state_status` | `text` | NOT NULL DEFAULT `'in_progress'` | 状态（in_progress / completed / failed） |
| `current_part_index` | `integer` | | 当前处理到第几个分片 |
| `completed_part_count` | `integer` | NOT NULL DEFAULT `0` | 已完成的片数 |
| `part_count` | `integer` | NOT NULL DEFAULT `1` | 总分片数 |
| `updated_at` | `timestamptz` | NOT NULL DEFAULT `now()` | 更新时间 |
| `created_at` | `timestamptz` | NOT NULL DEFAULT `now()` | 创建时间 |
| `state_json` | `jsonb` | NOT NULL DEFAULT `'{}'` | 完整状态 JSON |

### 索引

| 索引名 | 字段 |
|--------|------|
| `idx_book_processing_states_pending_resume` | `project_flag, pending_resume, updated_at DESC` |
| `idx_book_processing_states_category` | `category, updated_at DESC` |

> 表名可通过 `BOOK_STATE_TABLE` 配置。

---

## 7. `crawl_progress` — 爬虫进度记录

记录爬虫的进度状态。

| 字段 | 类型 | 约束 | 说明 |
|------|------|------|------|
| `id` | `bigserial` | PRIMARY KEY | 自增 ID |
| `type` | `text` | NOT NULL | 爬虫类型 |
| `value` | `text` | NOT NULL UNIQUE | 进度值 |
| `created_at` | `timestamptz` | NOT NULL DEFAULT `now()` | 创建时间 |

### 索引

| 索引名 | 字段 |
|--------|------|
| `idx_crawl_progress_type` | `type, value` |

---

## 表关系图

```
task_queue.book_id ──→ books.book_id              （外键逻辑关联）
youtube_credentials （独立，按频道名查找）
modelscope_tokens   （独立，按频道名查找）
channel_runtime_settings （独立，按频道名+键名查找）
book_processing_states.book_id ──→ books.book_id  （外键逻辑关联）
book_processing_states.project_flag ──→ youtube_credentials.channel_name
```
