"""音频处理模块 - 下载、合并、降噪、BGM 混音"""
from __future__ import annotations

import os
import glob
import math
import random
import shutil
import subprocess
import tempfile
import time
import concurrent.futures
from functools import lru_cache
from pathlib import Path

import numpy as np
import requests
from scipy.signal import butter, sosfilt, stft, istft
from pydub import AudioSegment
from tqdm.auto import tqdm
from concurrent.futures import ThreadPoolExecutor

from pipeline.config import get_config
from pipeline.log_utils import log, runtime_console_print
from pipeline.utils import sanitize_filename
from pipeline.constants import MIN_BOOK_DURATION_SECONDS, SUPPORTED_AUDIO_EXTENSIONS


# ============================================================================
# 章节音频下载
# ============================================================================

def download_audio_file(url: str, save_path: str, timeout_seconds: int = 300) -> dict:
    """
    下载单章节音频文件，带精细的重试和超时控制。
    返回字典: {ok, attempts, elapsed_seconds, error}
    """
    from pipeline.config import get_config

    connect_timeout = max(1, int(get_config("AUDIO_DOWNLOAD_CONNECT_TIMEOUT", 20)))
    read_timeout = max(1, int(get_config("AUDIO_DOWNLOAD_READ_TIMEOUT", 90)))
    max_attempts = max(1, int(get_config("AUDIO_DOWNLOAD_MAX_RETRY_ATTEMPTS", 12)))
    max_total_seconds = max(1, int(get_config("AUDIO_DOWNLOAD_MAX_TOTAL_SECONDS", 1800)))
    request_delay = max(0, float(get_config("REQUEST_DELAY", 0.3)))

    os.makedirs(os.path.dirname(save_path) or ".", exist_ok=True)
    start_time = time.time()

    for attempt in range(1, max_attempts + 1):
        if time.time() - start_time > max_total_seconds:
            return {
                "ok": False,
                "attempts": attempt - 1,
                "elapsed_seconds": round(time.time() - start_time, 1),
                "error": f"总耗时超过 {max_total_seconds}s 上限",
            }

        try:
            resp = requests.get(
                url,
                stream=True,
                timeout=(connect_timeout, read_timeout),
                allow_redirects=True,
            )
            resp.raise_for_status()

            content_length = resp.headers.get("Content-Length")
            expected_size = int(content_length) if content_length and content_length.isdigit() else None
            downloaded_size = 0

            with open(save_path, "wb") as handle:
                for chunk in resp.iter_content(chunk_size=1024 * 512):
                    if chunk:
                        handle.write(chunk)
                        downloaded_size += len(chunk)

            if os.path.getsize(save_path) == 0:
                os.remove(save_path)
                raise RuntimeError("下载文件大小为 0")

            if expected_size and downloaded_size != expected_size:
                os.remove(save_path)
                raise RuntimeError(f"文件大小不匹配: 预期 {expected_size}, 实际 {downloaded_size}")

            if attempt > 1:
                time.sleep(request_delay)

            return {
                "ok": True,
                "attempts": attempt,
                "elapsed_seconds": round(time.time() - start_time, 1),
                "error": "",
            }
        except Exception as e:
            if os.path.exists(save_path):
                try:
                    os.remove(save_path)
                except Exception:
                    pass
            if attempt < max_attempts:
                sleep_time = min(2 ** attempt + random.uniform(0, 2), 30)
                time.sleep(sleep_time)

    return {
        "ok": False,
        "attempts": max_attempts,
        "elapsed_seconds": round(time.time() - start_time, 1),
        "error": f"下载失败，已重试 {max_attempts} 次",
    }


def download_chapter_items(chapter_items, chapters_dir):
    """并发下载多个章节音频"""
    from pipeline.config import get_config

    if not chapter_items:
        return []

    download_workers = max(1, int(get_config("DOWNLOAD_WORKERS", 4)))
    request_delay = max(0, float(get_config("REQUEST_DELAY", 0.3)))
    stuck_log_interval = max(10, int(get_config("AUDIO_DOWNLOAD_STUCK_LOG_INTERVAL_SECONDS", 30)))

    os.makedirs(chapters_dir, exist_ok=True)

    def dl_one(item):
        mp3_url = item["chapter"].get("mp3Url", "")
        title = item.get("title") or f"chapter_{item['source_index']:04d}"
        if not mp3_url:
            return {
                "source_index": item["source_index"],
                "title": title,
                "path": None,
                "attempts": 0,
                "elapsed_seconds": 0.0,
                "error": "章节缺少 mp3Url",
            }

        path = os.path.join(chapters_dir, f"{item['source_index']:04d}_{sanitize_filename(title)}.mp3")
        result = download_audio_file(mp3_url, path, timeout_seconds=300)
        time.sleep(request_delay)
        return {
            "source_index": item["source_index"],
            "title": title,
            "path": path if result["ok"] else None,
            "attempts": result["attempts"],
            "elapsed_seconds": result["elapsed_seconds"],
            "error": result["error"],
        }

    paths_map = {}
    failures = {}
    total = len(chapter_items)
    with concurrent.futures.ThreadPoolExecutor(max_workers=download_workers) as exe:
        futures = {
            exe.submit(dl_one, item): {
                "source_index": item["source_index"],
                "title": item.get("title") or f"chapter_{item['source_index']:04d}",
                "submitted_at": time.time(),
            }
            for item in chapter_items
        }
        pending = set(futures.keys())
        with tqdm(total=total, desc="并发下载分片章节", unit="章") as progress:
            while pending:
                done, pending = concurrent.futures.wait(
                    pending,
                    timeout=stuck_log_interval,
                    return_when=concurrent.futures.FIRST_COMPLETED,
                )

                if done:
                    for future in done:
                        result = future.result()
                        idx = result["source_index"]
                        paths_map[idx] = result["path"]
                        if not result["path"]:
                            failures[idx] = result
                        progress.update(1)
                    continue

                pending_samples = []
                now = time.time()
                for future in sorted(pending, key=lambda item: futures[item]["source_index"])[:5]:
                    meta = futures[future]
                    pending_samples.append(
                        f"{meta['source_index']:04d}_{sanitize_filename(meta['title'])}({int(now - meta['submitted_at'])}s)"
                    )

                log.warning(
                    "并发下载仍在等待 %d/%d 个章节完成，可能有线程正在长时间重试或网络静默。当前等待中: %s",
                    len(pending), total,
                    " | ".join(pending_samples) if pending_samples else "无",
                )

    ordered_indexes = [item["source_index"] for item in chapter_items]
    chapter_paths = [paths_map[idx] for idx in ordered_indexes if paths_map.get(idx)]
    if len(chapter_paths) != len(ordered_indexes):
        missing_details = []
        for idx in ordered_indexes:
            if paths_map.get(idx):
                continue
            failed = failures.get(idx)
            if failed:
                missing_details.append(
                    f"{idx:04d}_{sanitize_filename(failed['title'])}"
                    f"(重试{failed['attempts']}次, 耗时{int(failed['elapsed_seconds'])}s, {failed['error']})"
                )
            else:
                missing_details.append(f"{idx:04d}_未知章节(未返回结果)")
        raise RuntimeError(f"章节下载不完整，失败章节: {'; '.join(missing_details)}")

    return chapter_paths


# ============================================================================
# 音频合并
# ============================================================================

def merge_audio_ffmpeg(mp3_paths: list, output_path: str) -> bool:
    """使用 FFmpeg concat demuxer 合并多个音频文件"""
    if not mp3_paths:
        log.warning("合并音频列表为空")
        return False

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    for p in mp3_paths:
        if not os.path.exists(p):
            log.warning("合并时找不到文件: %s", p)
            return False

    tmp_dir = tempfile.mkdtemp(prefix="merge_")
    filelist_path = os.path.join(tmp_dir, "filelist.txt")

    try:
        with open(filelist_path, "w", encoding="utf-8") as f:
            for path in mp3_paths:
                abs_path = os.path.abspath(path).replace("\\", "/")
                f.write(f"file '{abs_path}'\n")

        cmd = [
            "ffmpeg",
            "-y",
            "-f", "concat",
            "-safe", "0",
            "-i", filelist_path,
            "-c", "copy",
            output_path,
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=36000)
        if result.returncode != 0 or not os.path.exists(output_path) or os.path.getsize(output_path) == 0:
            log.warning("FFmpeg concat 失败，尝试重新编码合并: %s", result.stderr[:500])
            cmd = [
                "ffmpeg", "-y",
                "-f", "concat",
                "-safe", "0",
                "-i", filelist_path,
                "-c:a", "libmp3lame",
                "-b:a", "192k",
                "-ar", "44100",
                "-ac", "2",
                output_path,
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=36000)
            if result.returncode != 0 or not os.path.exists(output_path) or os.path.getsize(output_path) == 0:
                log.error("FFmpeg 重新编码合并失败: %s", result.stderr[:500])
                return False

        return True
    except Exception as e:
        log.error("FFmpeg 合并异常: %s", e)
        return False
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


# ============================================================================
# DeepFilter 降噪
# ============================================================================

def setup_deep_filter():
    """安装 DeepFilterNet 二进制文件"""
    deepfilter_dir = os.path.join(os.path.expanduser("~"), ".deepfilter")
    deepfilter_bin = os.path.join(deepfilter_dir, "deepfilter")

    if shutil.which("deepfilter"):
        return shutil.which("deepfilter")

    if os.path.exists(deepfilter_bin):
        return deepfilter_bin

    os.makedirs(deepfilter_dir, exist_ok=True)
    runtime_console_print("⬇️ 下载 DeepFilterNet...", level="INFO")

    try:
        import requests
        url = "https://github.com/Rikorose/DeepFilterNet/releases/download/v0.3.0/DeepFilterNet_Linux_x86_64.tar.gz"
        resp = requests.get(url, stream=True, timeout=120)
        resp.raise_for_status()

        tar_path = os.path.join(deepfilter_dir, "deepfilter.tar.gz")
        with open(tar_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)

        import tarfile
        with tarfile.open(tar_path, "r:gz") as tar:
            tar.extractall(path=deepfilter_dir)

        os.remove(tar_path)

        candidate = os.path.join(deepfilter_dir, "deepfilter")
        if os.path.exists(candidate):
            os.chmod(candidate, 0o755)
            runtime_console_print(f"✅ DeepFilterNet 已安装: {candidate}", level="INFO")
            return candidate

        for root, _, files in os.walk(deepfilter_dir):
            for fname in files:
                if fname == "deepfilter":
                    fpath = os.path.join(root, fname)
                    os.chmod(fpath, 0o755)
                    return fpath

        runtime_console_print("⚠️ DeepFilterNet 文件未找到", level="WARNING")
        return None
    except Exception as e:
        runtime_console_print(f"⚠️ DeepFilterNet 下载失败: {e}", level="WARNING")
        return None


def split_audio_to_wav(input_file, output_dir, seg_minutes=60, sr=16000):
    """将音频文件分割为多个 WAV 片段"""
    os.makedirs(output_dir, exist_ok=True)
    cmd = [
        "ffmpeg", "-y",
        "-i", input_file,
        "-f", "segment",
        "-segment_time", str(seg_minutes * 60),
        "-ar", str(sr),
        "-ac", "1",
        "-sample_fmt", "s16",
        "-c:a", "pcm_s16le",
        "-reset_timestamps", "1",
        os.path.join(output_dir, "seg_%04d.wav"),
    ]
    subprocess.run(cmd, capture_output=True, check=True)
    segs = sorted(
        [os.path.join(output_dir, f) for f in os.listdir(output_dir) if f.endswith(".wav")],
        key=lambda x: int(os.path.splitext(os.path.basename(x))[0].split("_")[1]),
    )
    return segs


def _df_process_wav(wav_file, output_dir):
    """对单个 WAV 文件执行 DeepFilter 降噪"""
    deepfilter_bin = setup_deep_filter()
    if not deepfilter_bin:
        raise RuntimeError("DeepFilter 二进制文件不可用")

    cmd = [deepfilter_bin, "-o", output_dir, wav_file]
    subprocess.run(cmd, capture_output=True, check=True, timeout=3600)

    base = os.path.splitext(os.path.basename(wav_file))[0]
    out_file = os.path.join(output_dir, f"{base}_denoised.wav")
    alt_file = os.path.join(output_dir, f"{base}.wav")
    if os.path.exists(out_file):
        return out_file
    if os.path.exists(alt_file):
        return alt_file
    return wav_file


def df_and_merge_wav(input_dir, output_dir, final_output, max_workers=1):
    """并行降噪多个 WAV 片段并合并"""
    os.makedirs(output_dir, exist_ok=True)
    wav_files = sorted(
        [os.path.join(input_dir, f) for f in os.listdir(input_dir) if f.endswith(".wav")],
    )
    if not wav_files:
        raise RuntimeError("没有找到可降噪的 WAV 文件")

    denoised_files = []
    for i, wav_path in enumerate(wav_files, 1):
        log.info("  DeepFilter %d/%d -> %s", i, len(wav_files), os.path.basename(wav_path))
        denoised = _df_process_wav(wav_path, output_dir)
        denoised_files.append(denoised)

    filelist_path = os.path.join(output_dir, "filelist.txt")
    with open(filelist_path, "w", encoding="utf-8") as f:
        for path in denoised_files:
            abs_path = os.path.abspath(path).replace("\\", "/")
            f.write(f"file '{abs_path}'\n")

    cmd = [
        "ffmpeg", "-y",
        "-f", "concat",
        "-safe", "0",
        "-i", filelist_path,
        "-c:a", "pcm_s16le",
        "-ar", "16000",
        "-ac", "1",
        final_output,
    ]
    subprocess.run(cmd, capture_output=True, check=True)
    return final_output


def denoise_audio(audio_path, segment_workers=1):
    """对音频文件执行 DeepFilter 降噪"""
    from pipeline.config import get_config

    seg_minutes = max(1, int(get_config("segment_duration_minutes", 60)))
    input_path = Path(audio_path)
    job_dir = tempfile.mkdtemp(prefix="df_")
    seg_dir = os.path.join(job_dir, "segments")
    df_dir = os.path.join(job_dir, "denoised")
    final_wav = os.path.join(job_dir, f"{input_path.stem}_denoised.wav")

    segs = split_audio_to_wav(str(input_path), seg_dir, seg_minutes=seg_minutes, sr=16000)
    df_and_merge_wav(seg_dir, df_dir, final_wav, max_workers=max(1, int(segment_workers or 1)))
    return final_wav, job_dir


def denoise_audio_keep_format(audio_path: str, output_path: str = "", segment_workers=1) -> str:
    """降噪并保持原始格式"""
    from pipeline.config import get_config

    enable_deepfilter = get_config("ENABLE_DEEPFILTER", True)
    skip_existing = get_config("SKIP_EXISTING", True)

    if not enable_deepfilter:
        return audio_path

    source = Path(audio_path)
    suffix = source.suffix.lower() or ".wav"
    target = Path(output_path) if output_path else source.with_name(f"{source.stem}_denoised{suffix}")

    if skip_existing and target.exists() and target.stat().st_size > 0:
        log.info("复用已降噪音频: %s", target.name)
        return str(target)

    temp_wav, job_dir = denoise_audio(audio_path, segment_workers=segment_workers)
    os.makedirs(target.parent, exist_ok=True)

    try:
        if target.suffix.lower() == ".wav":
            if target.exists():
                target.unlink()
            shutil.move(temp_wav, str(target))
        else:
            cmd = ["ffmpeg", "-y", "-i", temp_wav]
            if target.suffix.lower() == ".mp3":
                cmd += ["-codec:a", "libmp3lame", "-b:a", "192k"]
            elif target.suffix.lower() in {".m4a", ".aac"}:
                cmd += ["-codec:a", "aac", "-b:a", "192k"]
            elif target.suffix.lower() == ".flac":
                cmd += ["-codec:a", "flac"]
            elif target.suffix.lower() == ".ogg":
                cmd += ["-codec:a", "libvorbis", "-qscale:a", "5"]
            cmd.append(str(target))
            subprocess.run(cmd, capture_output=True, check=True)
        log.info("✅ 降噪音频已写回: %s", target.name)
        return str(target)
    finally:
        shutil.rmtree(job_dir, ignore_errors=True)


def denoise_audio_paths_parallel(audio_paths, output_paths=None, max_workers=2):
    """并行降噪多个音频文件"""
    if not audio_paths:
        return []

    total = len(audio_paths)
    worker_count = max(1, min(int(max_workers or 1), total))
    results = {}

    if output_paths is not None and len(output_paths) != total:
        raise ValueError("output_paths length must match audio_paths length")

    def _run(item):
        idx, path = item
        log.info("  DeepFilter %d/%d -> %s", idx + 1, total, os.path.basename(path))
        output_path = output_paths[idx] if output_paths is not None else ""
        return idx, denoise_audio_keep_format(path, output_path=output_path, segment_workers=1)

    with ThreadPoolExecutor(max_workers=worker_count) as ex:
        futures = {ex.submit(_run, item): item[0] for item in enumerate(audio_paths)}
        for future in tqdm(concurrent.futures.as_completed(futures), total=total, desc="DeepFilter双线程降噪", unit="轨"):
            idx, out_path = future.result()
            results[idx] = out_path

    return [results[i] for i in range(total)]


# ============================================================================
# 音频时长探测
# ============================================================================

def parse_duration_to_seconds(value):
    """解析时间戳到秒数"""
    if value is None:
        return 0

    text = str(value).strip()
    if not text:
        return 0

    try:
        parts = [int(p) for p in text.split(":")]
    except ValueError:
        return 0

    if len(parts) == 3:
        return parts[0] * 3600 + parts[1] * 60 + parts[2]
    if len(parts) == 2:
        return parts[0] * 60 + parts[1]
    if len(parts) == 1:
        return parts[0]
    return 0


def probe_audio_duration_seconds(audio_path):
    """探测音频文件时长（秒）"""
    if not audio_path or not os.path.exists(audio_path):
        return None

    try:
        result = subprocess.run(
            [
                "ffprobe",
                "-v", "error",
                "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1",
                audio_path,
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        return max(0, int(round(float(result.stdout.strip()))))
    except Exception:
        try:
            return max(0, int(round(len(AudioSegment.from_file(audio_path)) / 1000)))
        except Exception:
            return None


def estimate_chapter_duration_seconds(chapter):
    """预估章节时长"""
    if not isinstance(chapter, dict):
        return 1

    direct_value = chapter.get("duration_seconds")
    if isinstance(direct_value, (int, float)) and direct_value > 0:
        return max(1, int(round(float(direct_value))))

    for key in ("long", "duration", "audioDuration", "audio_duration"):
        value = chapter.get(key)
        if isinstance(value, (int, float)) and value > 0:
            return max(1, int(round(float(value))))
        seconds = parse_duration_to_seconds(value)
        if seconds > 0:
            return seconds

    return 1


def get_explicit_chapter_duration_seconds(chapter):
    """获取显式的章节时长"""
    if not isinstance(chapter, dict):
        return None

    direct_value = chapter.get("duration_seconds")
    if isinstance(direct_value, (int, float)) and direct_value > 0:
        return max(1, int(round(float(direct_value))))

    for key in ("long", "duration", "audioDuration", "audio_duration"):
        value = chapter.get(key)
        if isinstance(value, (int, float)) and value > 0:
            return max(1, int(round(float(value))))
        seconds = parse_duration_to_seconds(value)
        if seconds > 0:
            return seconds

    return None


def get_explicit_total_book_duration_seconds(chapters_sorted):
    """计算整本书的显式总时长"""
    if not chapters_sorted:
        return 0

    total_seconds = 0
    for chapter in chapters_sorted:
        chapter_seconds = get_explicit_chapter_duration_seconds(chapter)
        if chapter_seconds is None:
            return None
        total_seconds += chapter_seconds
    return total_seconds


# ============================================================================
# 分片计划构建
# ============================================================================

def build_split_part_plans(chapters_sorted):
    """构建音频分片计划"""
    from pipeline.config import get_config

    split_trigger_hours = float(get_config("LONG_AUDIO_SPLIT_TRIGGER_HOURS", 12.0))
    part_target_hours = float(get_config("LONG_AUDIO_PART_TARGET_HOURS", 11.8))

    split_trigger_seconds = max(1, int(split_trigger_hours * 3600))
    part_target_seconds = max(1, int(part_target_hours * 3600))

    chapter_items = []
    total_estimated_seconds = 0
    for source_index, chapter in enumerate(chapters_sorted, start=1):
        estimated_seconds = estimate_chapter_duration_seconds(chapter)
        total_estimated_seconds += estimated_seconds
        chapter_items.append({
            "source_index": source_index,
            "chapter": chapter,
            "chapter_id": chapter.get("id", source_index),
            "title": chapter.get("title", f"chapter_{source_index:04d}"),
            "estimated_seconds": estimated_seconds,
        })

    if total_estimated_seconds <= split_trigger_seconds or not chapter_items:
        return {
            "split_mode": False,
            "split_trigger_seconds": split_trigger_seconds,
            "part_target_seconds": part_target_seconds,
            "estimated_total_seconds": total_estimated_seconds,
            "parts": [
                {
                    "part_index": 1,
                    "chapter_start_index": chapter_items[0]["source_index"] if chapter_items else 1,
                    "chapter_end_index": chapter_items[-1]["source_index"] if chapter_items else 0,
                    "estimated_duration_seconds": total_estimated_seconds,
                    "items": chapter_items,
                }
            ],
        }

    parts = []
    current_items = []
    current_seconds = 0

    def flush_current():
        nonlocal current_items, current_seconds
        if not current_items:
            return
        parts.append({
            "part_index": len(parts) + 1,
            "chapter_start_index": current_items[0]["source_index"],
            "chapter_end_index": current_items[-1]["source_index"],
            "estimated_duration_seconds": current_seconds,
            "items": current_items,
        })
        current_items = []
        current_seconds = 0

    for item in chapter_items:
        item_seconds = item["estimated_seconds"]
        if current_items and current_seconds + item_seconds > part_target_seconds:
            flush_current()
        current_items.append(item)
        current_seconds += item_seconds
        if item_seconds > part_target_seconds:
            log.warning(
                "章节 %s 预估时长 %s 已超过单片目标时长 %s，将单独作为一个分片处理。",
                item.get("title") or item.get("chapter_id"),
                format_seconds_hhmmss(item_seconds),
                format_seconds_hhmmss(part_target_seconds),
            )
            flush_current()

    flush_current()

    return {
        "split_mode": True,
        "split_trigger_seconds": split_trigger_seconds,
        "part_target_seconds": part_target_seconds,
        "estimated_total_seconds": total_estimated_seconds,
        "parts": parts,
    }


def format_seconds_hhmmss(total_seconds):
    """将秒数格式化为 HH:MM:SS"""
    seconds = max(0, int(total_seconds or 0))
    return f"{seconds // 3600:02d}:{(seconds % 3600) // 60:02d}:{seconds % 60:02d}"


# ============================================================================
# BGM 混音处理
# ============================================================================

@lru_cache(maxsize=8)
def load_music_segment_cached(music_path):
    """缓存少量 BGM 源文件，减少长批处理中重复解码的开销"""
    return AudioSegment.from_file(music_path)


def analyze_audio(audio_segment):
    """分析音频特征"""
    duration_ms = len(audio_segment)
    rms_dbfs = audio_segment.dBFS
    peak_dbfs = audio_segment.max_dBFS

    chunk_size_ms = 500
    chunks = [audio_segment[i:i + chunk_size_ms]
              for i in range(0, duration_ms, chunk_size_ms)
              if i + chunk_size_ms <= duration_ms]

    chunk_levels = []
    for chunk in chunks:
        try:
            level = chunk.dBFS
            if level > -60:
                chunk_levels.append(level)
        except Exception:
            pass
    dynamic_range_db = (max(chunk_levels) - min(chunk_levels)) if len(chunk_levels) >= 2 else 0
    return {
        "rms_dbfs": rms_dbfs, "peak_dbfs": peak_dbfs,
        "dynamic_range_db": dynamic_range_db, "duration_ms": duration_ms,
        "sample_rate": audio_segment.frame_rate, "channels": audio_segment.channels,
    }


def compute_volume_envelope(audio_segment, window_ms=200):
    """计算音量包络"""
    duration_ms = len(audio_segment)
    envelope = []
    for i in range(0, duration_ms, window_ms):
        chunk = audio_segment[i:i + window_ms]
        if len(chunk) < 50:
            envelope.append(envelope[-1] if envelope else -60)
            continue
        try:
            level = max(chunk.dBFS, -60)
            envelope.append(level)
        except Exception:
            envelope.append(-60)
    return np.array(envelope), window_ms


def analyze_spectral_gaps(audio_segment, n_bands=8):
    """分析频谱空隙"""
    sample_rate = audio_segment.frame_rate
    samples = np.array(audio_segment.get_array_of_samples(), dtype=np.float64)
    if audio_segment.channels > 1:
        samples = samples.reshape((-1, audio_segment.channels)).mean(axis=1)

    max_val = 2 ** (audio_segment.sample_width * 8 - 1)
    samples = samples / max_val
    nperseg = min(4096, len(samples))
    freqs, times, Zxx = stft(samples, fs=sample_rate, nperseg=nperseg)
    power = np.abs(Zxx) ** 2

    nyquist = sample_rate / 2
    max_freq = min(nyquist, 16000)
    band_edges = np.logspace(np.log10(150), np.log10(max_freq), n_bands + 1)

    band_energies = []
    for i in range(n_bands):
        mask = (freqs >= band_edges[i]) & (freqs < band_edges[i + 1])
        band_energies.append(power[mask].mean() if mask.any() else 1e-10)

    band_energies_db = 10 * np.log10(np.array(band_energies) + 1e-10)
    max_energy_db = band_energies_db.max()
    relative_db = band_energies_db - max_energy_db
    band_gains = np.clip(-relative_db * 0.3, 0, 6)
    return band_gains, band_edges


def apply_highpass_filter(audio_segment, cutoff_freq=150, order=4):
    """应用高通滤波器"""
    sample_rate = audio_segment.frame_rate
    channels = audio_segment.channels
    samples = np.array(audio_segment.get_array_of_samples(), dtype=np.float64)
    if channels > 1:
        samples = samples.reshape((-1, channels))

    nyquist = sample_rate / 2.0
    sos = butter(order, min(cutoff_freq / nyquist, 0.99), btype='high', output='sos')

    if channels > 1:
        filtered = np.zeros_like(samples)
        for ch in range(channels):
            filtered[:, ch] = sosfilt(sos, samples[:, ch])
        filtered = filtered.flatten()
    else:
        filtered = sosfilt(sos, samples)

    max_val = 2 ** (audio_segment.sample_width * 8 - 1) - 1
    filtered = np.clip(filtered, -max_val, max_val).astype(
        np.int16 if audio_segment.sample_width == 2 else np.int32)

    return AudioSegment(data=filtered.tobytes(), sample_width=audio_segment.sample_width,
                        frame_rate=sample_rate, channels=channels)


def _shape_single_channel(samples, sample_rate, band_gains, band_edges):
    """对单声道进行频谱塑形"""
    nperseg = min(4096, len(samples))
    freqs, times, Zxx = stft(samples, fs=sample_rate, nperseg=nperseg)

    gain_curve = np.ones(len(freqs))
    for i in range(len(band_gains)):
        mask = (freqs >= band_edges[i]) & (freqs < band_edges[i + 1])
        gain_curve[mask] = 10 ** (band_gains[i] / 20.0)

    Zxx_shaped = Zxx * gain_curve[:, np.newaxis]
    _, result = istft(Zxx_shaped, fs=sample_rate, nperseg=nperseg)

    if len(result) > len(samples):
        result = result[:len(samples)]
    elif len(result) < len(samples):
        result = np.pad(result, (0, len(samples) - len(result)))
    return result


def apply_spectral_shaping(audio_segment, band_gains, band_edges):
    """应用频谱塑形"""
    sample_rate = audio_segment.frame_rate
    channels = audio_segment.channels
    samples = np.array(audio_segment.get_array_of_samples(), dtype=np.float64)

    if channels > 1:
        samples = samples.reshape((-1, channels))
        result_channels = [_shape_single_channel(samples[:, ch], sample_rate, band_gains, band_edges)
                           for ch in range(channels)]
        result = np.column_stack(result_channels).flatten()
    else:
        result = _shape_single_channel(samples, sample_rate, band_gains, band_edges)

    max_val = 2 ** (audio_segment.sample_width * 8 - 1) - 1
    result = np.clip(result, -max_val, max_val).astype(
        np.int16 if audio_segment.sample_width == 2 else np.int32)

    return AudioSegment(data=result.tobytes(), sample_width=audio_segment.sample_width,
                        frame_rate=sample_rate, channels=channels)


def apply_dynamic_volume(audio_segment, volume_envelope, window_ms, vol_offset_db=-25, min_vol_db=-40):
    """应用动态音量均衡"""
    duration_ms = len(audio_segment)
    envelope_median = np.median(volume_envelope)

    chunks = []
    for i, env_level in enumerate(volume_envelope):
        start_ms = i * window_ms
        end_ms = min(start_ms + window_ms, duration_ms)
        if start_ms >= duration_ms:
            break

        chunk = audio_segment[start_ms:end_ms]
        if len(chunk) < 10:
            continue

        deviation = env_level - envelope_median
        dynamic_adjust = np.clip(deviation * 0.4, -6, 6)
        target_volume = max(env_level + vol_offset_db + dynamic_adjust, min_vol_db)

        try:
            gain = np.clip(target_volume - chunk.dBFS, -40, 10)
            chunk = chunk.apply_gain(gain)
        except Exception:
            pass
        chunks.append(chunk)

    if not chunks:
        return audio_segment

    raw_data = b"".join([c.raw_data for c in chunks])
    result = audio_segment._spawn(raw_data)

    if len(result) > duration_ms:
        result = result[:duration_ms]
    elif len(result) < duration_ms:
        result += AudioSegment.silent(duration=duration_ms - len(result),
                                      frame_rate=audio_segment.frame_rate)
    return result


def apply_stereo_offset(audio_segment, offset=0.3):
    """应用立体声偏移"""
    if audio_segment.channels < 2:
        audio_segment = audio_segment.set_channels(2)

    samples = np.array(audio_segment.get_array_of_samples(), dtype=np.float64).reshape((-1, 2))
    left_gain = (1.0 - offset * 0.5) if offset > 0 else 1.0
    right_gain = 1.0 if offset > 0 else (1.0 + offset * 0.5)

    samples[:, 0] *= left_gain
    samples[:, 1] *= right_gain

    max_val = 2 ** (audio_segment.sample_width * 8 - 1) - 1
    result = np.clip(samples.flatten(), -max_val, max_val).astype(
        np.int16 if audio_segment.sample_width == 2 else np.int32)

    return AudioSegment(data=result.tobytes(), sample_width=audio_segment.sample_width,
                        frame_rate=audio_segment.frame_rate, channels=2)


def get_all_music_files(music_folder):
    """获取指定目录下的所有音乐文件"""
    supported_extensions = ("*.mp3", "*.wav", "*.flac", "*.ogg", "*.m4a", "*.aac", "*.wma")
    music_files = []
    for ext in supported_extensions:
        music_files.extend(glob.glob(os.path.join(music_folder, ext)))
        music_files.extend(glob.glob(os.path.join(music_folder, ext.upper())))
    music_files = list(set(music_files))
    if not music_files:
        raise FileNotFoundError(f"未找到可选的音乐文件: {music_folder}")
    return music_files


def prepare_copyright_music(music_files, target_duration_ms, original_audio,
                            original_analysis, vol_offset_db, hp_freq, fade_ms,
                            min_vol_db, dyn_vol, spec_shape, st_offset):
    """准备并混音版权音乐"""
    log.info("🎞 开启随机连串版权音乐模式")

    # 全局分析一次原声的频谱空隙
    global_bg, global_be = None, None
    if spec_shape:
        log.info("  全局频谱空袭分析与嵌入检测")
        global_bg, global_be = analyze_spectral_gaps(original_audio)

    # 随机打乱音乐库
    shuffled_files = list(music_files)
    random.shuffle(shuffled_files)

    log.info("  BGM 随机拼接池大小: %d 首 | 目标: %d s", len(shuffled_files), target_duration_ms // 1000)

    looped = AudioSegment.empty()
    music_idx = 0

    while len(looped) < target_duration_ms:
        music_path = shuffled_files[music_idx % len(shuffled_files)]
        music_idx += 1

        segment = load_music_segment_cached(music_path)
        segment_duration = len(segment)

        if hp_freq > 0:
            segment = apply_highpass_filter(segment, cutoff_freq=hp_freq)
        if spec_shape:
            segment = apply_spectral_shaping(segment, global_bg, global_be)

        remaining = target_duration_ms - len(looped)

        if remaining < segment_duration:
            segment = segment[:remaining]
            segment = segment.fade_out(min(fade_ms, remaining // 4))
        else:
            segment = segment.fade_out(min(fade_ms, segment_duration // 4))

        if len(looped) > 0 and fade_ms > 0:
            afade = min(fade_ms, len(segment) // 4)
            if afade > 0:
                segment = segment.fade_in(afade)
                looped = looped.fade_out(afade)

        looped += segment

    looped = looped[:target_duration_ms]

    if dyn_vol:
        log.info("  全局动态音量包络跟踪")
        env, w_ms = compute_volume_envelope(original_audio)
        looped = apply_dynamic_volume(looped, env, w_ms, vol_offset_db, min_vol_db)
    else:
        target_volume = max(original_analysis["rms_dbfs"] + vol_offset_db, min_vol_db)
        looped = looped.apply_gain(target_volume - looped.dBFS)

    final_fade = min(fade_ms, target_duration_ms // 10)
    if final_fade > 100:
        looped = looped.fade_in(final_fade).fade_out(final_fade)

    if st_offset != 0.0:
        log.info("  立体声偏移: %.1f", st_offset)
        looped = apply_stereo_offset(looped, offset=st_offset)

    return looped


def mix_with_bgm(
    input_path: str, output_path: str, music_dir: str,
    *, volume_offset_db=-25, highpass_freq=150, fade_duration_ms=3000,
    min_volume_db=-40, dyn_vol=True, spec_shape=True, stereo_offset=0.0
) -> bool:
    """将 BGM 混合到音频文件中"""
    try:
        music_files = get_all_music_files(music_dir)
        log.info("加载原音频: %s", os.path.basename(input_path))
        orig_audio = AudioSegment.from_file(input_path)

        analysis = analyze_audio(orig_audio)
        bgm_music = prepare_copyright_music(
            music_files, len(orig_audio), orig_audio, analysis, volume_offset_db,
            highpass_freq, fade_duration_ms, min_volume_db, dyn_vol, spec_shape, stereo_offset,
        )

        # 格式对齐
        if orig_audio.frame_rate != bgm_music.frame_rate:
            bgm_music = bgm_music.set_frame_rate(orig_audio.frame_rate)
        if orig_audio.channels != bgm_music.channels:
            bgm_music = bgm_music.set_channels(orig_audio.channels)
        if len(bgm_music) > len(orig_audio):
            bgm_music = bgm_music[:len(orig_audio)]
        elif len(bgm_music) < len(orig_audio):
            bgm_music += AudioSegment.silent(duration=len(orig_audio)-len(bgm_music), frame_rate=orig_audio.frame_rate)

        log.info("🎛️ 混合音频叠加...")
        mixed = orig_audio.overlay(bgm_music)

        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        mixed.export(output_path, format="mp3", bitrate="192k")
        log.info("✅ 混音已保存: %s", os.path.basename(output_path))
        return True
    except Exception as e:
        log.error("音频混入失败: %s", e)
        return False


def generate_youtube_timestamps(chapters_sorted, chapter_paths=None):
    """生成 YouTube 章节时间戳"""
    timestamps = []
    cumulative = 0

    for idx, chapter in enumerate(chapters_sorted):
        title = chapter.get("title", f"章节 {idx + 1}")
        hours = cumulative // 3600
        minutes = (cumulative % 3600) // 60
        seconds = cumulative % 60

        if hours > 0:
            timestamps.append(f"{hours:02d}:{minutes:02d}:{seconds:02d} {title}")
        else:
            timestamps.append(f"{minutes:02d}:{seconds:02d} {title}")

        duration = estimate_chapter_duration_seconds(chapter)
        cumulative += duration

    return "\n".join(timestamps)


def build_final_audio_from_chapter_paths(chapter_paths, working_dir, merged_path, mixed_path, book_name):
    """从章节路径构建最终音频（含 BGM 混音可选）"""
    from pipeline.config import get_config

    enable_bgm_mix = get_config("ENABLE_BGM_MIX", True)
    music_dir = get_config("MUSIC_DIR", "")
    volume_offset_db = float(get_config("VOLUME_OFFSET_DB", -25))
    highpass_freq = int(get_config("HIGHPASS_FREQ", 150))
    fade_duration_ms = int(get_config("FADE_DURATION_MS", 3000))
    min_volume_db = float(get_config("MIN_VOLUME_DB", -40))
    enable_dynamic_volume = get_config("ENABLE_DYNAMIC_VOLUME", True)
    enable_spectral_shaping = get_config("ENABLE_SPECTRAL_SHAPING", True)
    stereo_offset = float(get_config("STEREO_OFFSET", 0.0))

    if enable_bgm_mix and music_dir.strip() and os.path.exists(music_dir.strip()):
        mixed_dir = os.path.join(working_dir, "mixed_chapters")
        os.makedirs(mixed_dir, exist_ok=True)
        mixed_chapters = []

        for i, ch_path in enumerate(chapter_paths, start=1):
            mixed_basename = os.path.splitext(os.path.basename(ch_path))[0] + "_mixed.mp3"
            ch_mixed = os.path.join(mixed_dir, mixed_basename)
            if os.path.exists(ch_mixed) and os.path.getsize(ch_mixed) > 0:
                mixed_chapters.append(ch_mixed)
                continue

            log.info("[%s] 混音章节 %d/%d -> %s", book_name, i, len(chapter_paths), os.path.basename(ch_path))
            ok_mix = mix_with_bgm(
                ch_path, ch_mixed, music_dir.strip(),
                volume_offset_db=volume_offset_db,
                highpass_freq=highpass_freq,
                fade_duration_ms=fade_duration_ms,
                min_volume_db=min_volume_db,
                dyn_vol=enable_dynamic_volume,
                spec_shape=enable_spectral_shaping,
                stereo_offset=stereo_offset,
            )
            if not ok_mix:
                raise RuntimeError(f"BGM 混音失败: {os.path.basename(ch_path)}")
            mixed_chapters.append(ch_mixed)

        if not merge_audio_ffmpeg(mixed_chapters, mixed_path):
            raise RuntimeError("长音频分片混音合并失败")

        for temp_path in mixed_chapters:
            try:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
            except Exception as cleanup_error:
                log.warning("清理临时混音文件失败: %s", cleanup_error)

        return {"audio_path": mixed_path, "mixed_audio_path": mixed_path}

    if not merge_audio_ffmpeg(chapter_paths, merged_path):
        raise RuntimeError("章节音频合并失败")

    return {"audio_path": merged_path, "mixed_audio_path": ""}


def generate_video(audio_path, cover_image_path, video_output_path, resolution="1080p"):
    """生成 MP4 视频（封面图 + 音频）"""
    if resolution == "720p":
        size = "1280:720"
    elif resolution == "1080p":
        size = "1920:1080"
    else:
        size = "1920:1080"

    cmd = [
        "ffmpeg", "-y",
        "-loop", "1",
        "-i", cover_image_path,
        "-i", audio_path,
        "-c:v", "libx264",
        "-tune", "stillimage",
        "-c:a", "aac",
        "-b:a", "192k",
        "-pix_fmt", "yuv420p",
        "-vf", f"scale={size}:force_original_aspect_ratio=decrease,pad={size}:(ow-iw)/2:(oh-ih)/2",
        "-shortest",
        "-movflags", "+faststart",
        video_output_path,
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=36000)
        if result.returncode != 0 or not os.path.exists(video_output_path) or os.path.getsize(video_output_path) == 0:
            log.error("MP4 封装失败: %s", result.stderr[:500])
            return False
        return True
    except Exception as e:
        log.error("MP4 封装异常: %s", e)
        return False