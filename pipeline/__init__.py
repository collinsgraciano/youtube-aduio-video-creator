"""pipeline 包 - 向后兼容的 re-export 入口

当 colab_loader.ipynb 使用 `from pipeline.config import RuntimeConfig`
或 `from pipeline import *` 时，本 __init__.py 确保所有功能可用。

## 导入顺序（防止循环引用）

子模块按依赖层级从低到高排列：
  1. config      — 基础配置，无 pipeline 内部依赖
  2. log_utils   — 日志工具，无 pipeline 内部依赖
  3. db          — 数据库，依赖 config, log_utils
  4. utils       — 工具函数，无 pipeline 内部依赖
  5. modelscope  — AI 生成，依赖 config, log_utils, db, utils
  6. youtube     — YouTube API，依赖 config, log_utils, db, utils
  7. state       — 状态管理，依赖 config, log_utils, db, utils
  8. audio       — 音频处理，依赖 config, log_utils, utils
  9. music_library — 音乐库，依赖 config, log_utils, db, utils
  10. podcast    — Podcast 管理，依赖 config, log_utils, db, utils, youtube
  11. core       — 主流程，依赖以上所有子模块

⚠️ 任何新增子模块不可反向依赖 core.py，否则产生循环引用。
`apply_runtime_config()` 在最后调用，此时所有子模块已加载完毕。
"""
from __future__ import annotations

import sys as _sys
import os as _os

# 确保 pipeline 包在 sys.path 中
_pkg_dir = _os.path.dirname(_os.path.abspath(__file__))
if _pkg_dir not in _sys.path:
    _sys.path.insert(0, _os.path.dirname(_pkg_dir))

# ============================================================================
# 从各子模块导入所有公开符号到 pipeline 包命名空间
# ============================================================================

from pipeline.config import (
    DEFAULT_RUNTIME_CONFIG,
    apply_runtime_config,
    get_config,
    RuntimeConfig,
)
from pipeline.log_utils import (
    SimpleLogger,
    runtime_console_print,
    clear_runtime_output_if_needed,
    quiet_runtime_output_enabled,
    log,
)
from pipeline.db import (
    get_postgres_dsn,
    get_public_table_identifier,
    execute_postgres_fetchone,
    execute_postgres_fetchall,
    execute_postgres,
    execute_postgres_fetchval,
)
from pipeline.utils import (
    sanitize_filename,
    normalize_text_items,
    make_json_compatible,
    append_unique_text_items,
    build_supabase_text_update,
    parse_text_list_config,
    write_json_file,
    read_json_file,
    format_seconds_hhmmss,
    download_file,
    clear_folder,
    safe_music_output_path,
    normalize_runtime_source,
    _ILLEGAL_CHARS,
)
from pipeline.modelscope import (
    normalize_modelscope_token_pool,
    build_modelscope_token_pool,
    build_modelscope_token_pool_bundle,
    _get_modelscope_active_tokens,
    _get_modelscope_usage_token_pool,
    _remove_modelscope_token_from_pool,
    is_modelscope_daily_quota_exceeded_error,
    is_modelscope_http_429_error,
    is_modelscope_http_401_error,
    is_modelscope_image_review_rejection_error,
    CoverGenerationPolicyRejectedError,
    get_cloud_runtime_settings_table_name,
    get_modelscope_token_table_name,
    get_shared_cloud_runtime_scope_key,
    load_modelscope_token_from_supabase,
    load_cloud_runtime_setting_from_supabase,
    resolve_modelscope_token,
    auto_create_youtube_cover,
    auto_create_youtube_seo,
    _is_nonempty_local_file,
    _persist_cover_fallback_image,
)
from pipeline.youtube import (
    authenticate_youtube_from_supabase,
    upload_youtube_video,
    persist_youtube_upload_receipt,
    load_youtube_upload_receipt,
    build_youtube_payload,
    build_youtube_traditional_localizations,
    MissingYouTubeCredentialsError,
    _extract_youtube_video_id,
    _fetch_video_status_rows_with_client,
    _build_existing_video_match_from_row,
    _normalize_youtube_title_key,
    _build_channel_video_title_index_with_client,
    normalize_playlist_privacy_status,
    create_or_get_playlist,
    add_video_to_playlist,
    add_videos_to_playlist_in_order,
    _sync_playlist_localizations_with_client,
    _normalize_local_path_for_compare,
    _capture_local_file_signature,
)
from pipeline.state import (
    build_split_state_ref,
    get_book_state_table_name,
    get_split_part_state,
    get_split_shared_assets,
    get_split_playlist_state,
    evaluate_split_completion_state,
    normalize_split_state_from_row,
    load_split_processing_state,
    save_split_processing_state,
    delete_split_processing_state,
    cleanup_completed_split_states,
    initialize_split_processing_state,
    list_interrupted_book_states,
    reconcile_split_part_upload_states,
    build_split_plan_signature,
    _build_split_state_debug_payload,
    _save_split_processing_state_raw,
    _maybe_log_split_state_persisted,
    _truncate_split_state_debug_value,
    _split_part_has_uploaded_video,
    _is_split_playlist_required,
    _split_part_is_completed,
    _reconcile_split_part_state,
    _build_split_state_completeness_rank,
    reload_split_processing_state,
    _build_split_part_lookup_key,
    _read_bool_runtime_config,
    _should_cleanup_completed_split_states,
    _build_fresh_split_state,
    _apply_video_match_to_split_part,
    _reset_split_part_upload_state,
    _build_expected_split_upload_title,
    _wait_for_live_video_rows_with_client,
    _fetch_video_rows_by_id_with_client,
)
from pipeline.audio import (
    download_audio_file,
    download_chapter_items,
    merge_audio_ffmpeg,
    setup_deep_filter,
    split_audio_to_wav,
    _df_process_wav,
    df_and_merge_wav,
    denoise_audio,
    denoise_audio_keep_format,
    denoise_audio_paths_parallel,
    parse_duration_to_seconds,
    probe_audio_duration_seconds,
    estimate_chapter_duration_seconds,
    get_explicit_chapter_duration_seconds,
    get_explicit_total_book_duration_seconds,
    format_seconds_hhmmss,
    build_split_part_plans,
    generate_youtube_timestamps,
    build_final_audio_from_chapter_paths,
    mix_with_bgm,
    generate_video,
    load_music_segment_cached,
    analyze_audio,
    compute_volume_envelope,
    analyze_spectral_gaps,
    apply_highpass_filter,
    apply_spectral_shaping,
    apply_dynamic_volume,
    apply_stereo_offset,
    get_all_music_files,
    prepare_copyright_music,
    MIN_BOOK_DURATION_SECONDS,
)
from pipeline.music_library import (
    load_cloud_music_runtime_setting,
    resolve_music_runtime_setting,
    apply_music_download_runtime_overrides,
    build_hf_download_headers,
    normalize_hf_dataset_download_url,
    download_file_with_wget,
    download_file_with_requests,
    extract_audio_files_from_zip,
    download_music_from_dataset_urls,
    download_music_from_buckets,
    sync_music_library_if_enabled,
    SUPPORTED_AUDIO_EXTENSIONS,
)
from pipeline.podcast import (
    _podcast_runtime_enabled,
    _podcast_unified_show_enabled,
    _podcast_split_playlist_enabled,
    _podcast_show_setting_key,
    _podcast_show_title,
    _podcast_image_size,
    _podcast_image_max_bytes,
    _podcast_progress,
    _podcast_now_iso,
    _podcast_short,
    _podcast_load_channel_setting,
    _podcast_save_channel_setting,
    _podcast_extract_best_thumbnail_url,
    _podcast_normalize_status,
    _podcast_playlist_row_to_record,
    _podcast_error_text,
    _podcast_extract_http_error_details,
    _podcast_is_retryable_text_error,
    _podcast_is_retryable_youtube_http_error,
    _podcast_youtube_retry_sleep_seconds,
    _podcast_ai_retry_sleep_seconds,
    _podcast_execute_youtube_request,
    _podcast_fetch_playlist_by_id,
    _podcast_wait_for_playlist_podcast_status,
    _podcast_extract_youtube_credentials,
    _podcast_playlist_image_row,
    _podcast_is_playlist_images_unsupported_error,
    _podcast_list_playlist_images_via_rest,
    _podcast_list_playlist_images,
    _podcast_resolve_playlist_image_status,
    _podcast_sync_playlist_image,
    _podcast_create_sensenova_client,
    _podcast_extract_chat_text,
    _podcast_is_rate_limited_error,
    _podcast_is_security_rejection_error,
    _podcast_is_retryable_ai_error,
    _podcast_chat_complete_with_model,
    _podcast_generate_text_via_models,
    _podcast_build_default_show_description,
    _podcast_generate_show_description,
    _podcast_build_default_cover_prompt,
    _podcast_generate_show_cover_prompt,
    _podcast_build_batch_playlist_cover_prompt_fallback,
    _podcast_generate_batch_playlist_cover_prompt,
    _podcast_download_bytes,
    _podcast_save_square_cover_image,
    _podcast_generate_local_text_gradient_cover,
    _podcast_build_safe_retry_cover_prompt,
    _podcast_generate_cover_from_existing_thumbnail,
    _podcast_generate_cover_from_local_image,
    _podcast_log_image_source,
    _podcast_generate_named_cover_image,
    _podcast_generate_show_cover_image,
    _podcast_create_plain_playlist,
    _podcast_update_playlist,
    _podcast_resolve_existing_show_playlist,
    _podcast_ensure_video_in_playlist,
    _podcast_get_show_state_container,
    _podcast_apply_show_state_to_result,
    _podcast_sync_split_playlist_podcast,
    # 公开接口
    sync_split_playlist_podcast,
)
from pipeline.core import (
    validate_runtime_config,
    apply_cloud_runtime_overrides,
    collect_runtime_config_snapshot,
    get_remaining_runtime_seconds,
    should_stop_before_next_book,
    save_run_summary,
    finalize_successful_book_for_project,
    BookResult,
    prepare_book_cover_and_seo,
    restore_split_shared_assets_from_state,
    persist_split_shared_assets_to_state,
    build_standard_processing_state,
    prepare_standard_book_cover_and_seo_with_state,
    skip_and_delete_short_book,
    sync_result_from_split_state,
    finalize_book_result,
    process_standard_book,
    process_split_book,
    process_book,
    run_pipeline,
)

# ============================================================================
# 应用默认配置到所有子模块
# ============================================================================
from pipeline.config import apply_runtime_config
apply_runtime_config()

# 标记 pipeline 包已完成初始化
_initialized = True