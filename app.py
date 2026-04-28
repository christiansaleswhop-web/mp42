
"""
HQ TUBE LV99 - Ultimate YouTube Downloader
Production-ready, fault-tolerant, with real-time progress, format selection,
auto-cleanup, and bulletproof error handling.
"""

from flask import Flask, request, send_file, render_template_string, jsonify, Response, stream_with_context
import yt_dlp
import os
import uuid
import time
import json
import re
import shutil
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from urllib.parse import urlparse

app = Flask(__name__)

# ═══════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════
DOWNLOAD_FOLDER = os.environ.get("DOWNLOAD_DIR", "/tmp/hq_tube_downloads")
MAX_FILE_AGE_MINUTES = int(os.environ.get("MAX_FILE_AGE", "30"))
MAX_DOWNLOAD_SIZE_MB = int(os.environ.get("MAX_SIZE_MB", "2048"))
MAX_WORKERS = int(os.environ.get("MAX_WORKERS", "3"))

os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)

# Thread-safe job storage
jobs = {}
jobs_lock = Lock()
executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)

# ═══════════════════════════════════════════════════════════════
# JOB MANAGEMENT
# ═══════════════════════════════════════════════════════════════

def create_job() -> str:
    job_id = str(uuid.uuid4())[:16]
    with jobs_lock:
        jobs[job_id] = {
            "id": job_id,
            "status": "queued",
            "progress": "0%",
            "speed": "",
            "eta": "",
            "message": "Waiting in queue...",
            "filepath": None,
            "filename": None,
            "title": "",
            "created": datetime.now(),
            "updated": datetime.now(),
            "error": None,
            "logs": []
        }
    return job_id

def update_job(job_id: str, **kwargs):
    with jobs_lock:
        if job_id in jobs:
            jobs[job_id].update(kwargs)
            jobs[job_id]["updated"] = datetime.now()

def get_job(job_id: str) -> dict:
    with jobs_lock:
        return jobs.get(job_id, {}).copy()

def log_job(job_id: str, message: str):
    with jobs_lock:
        if job_id in jobs:
            jobs[job_id]["logs"].append(f"[{datetime.now().strftime('%H:%M:%S')}] {message}")
            jobs[job_id]["updated"] = datetime.now()

def cleanup_expired():
    """Remove files and jobs older than MAX_FILE_AGE_MINUTES"""
    cutoff = datetime.now() - timedelta(minutes=MAX_FILE_AGE_MINUTES)
    with jobs_lock:
        stale_jobs = [
            (jid, job) for jid, job in jobs.items()
            if job.get("created", datetime.now()) < cutoff
        ]
        for jid, job in stale_jobs:
            fp = job.get("filepath")
            if fp and os.path.exists(fp):
                try:
                    os.remove(fp)
                except:
                    pass
            jobs.pop(jid, None)

    # Also clean orphaned files in download folder
    try:
        for f in os.listdir(DOWNLOAD_FOLDER):
            path = os.path.join(DOWNLOAD_FOLDER, f)
            if os.path.getmtime(path) < time.time() - (MAX_FILE_AGE_MINUTES * 60):
                try:
                    if os.path.isfile(path):
                        os.remove(path)
                    else:
                        shutil.rmtree(path)
                except:
                    pass
    except:
        pass

# ═══════════════════════════════════════════════════════════════
# YT-DLP HELPERS
# ═══════════════════════════════════════════════════════════════

def validate_youtube_url(url: str) -> bool:
    if not url or not isinstance(url, str):
        return False
    parsed = urlparse(url.strip())
    if not parsed.scheme or not parsed.netloc:
        return False
    domain = parsed.netloc.lower()
    return any(x in domain for x in ["youtube.com", "youtu.be", "youtube-nocookie.com"])

def format_size(size):
    if not size or size <= 0:
        return None
    for unit in ["B", "KB", "MB", "GB"]:
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TB"

def format_duration(seconds):
    if not seconds:
        return "0:00"
    try:
        seconds = int(seconds)
        m, s = divmod(seconds, 60)
        h, m = divmod(m, 60)
        if h > 0:
            return f"{h}:{m:02d}:{s:02d}"
        return f"{m}:{s:02d}"
    except:
        return "0:00"

def build_progress_hook(job_id: str):
    def hook(d):
        if d["status"] == "downloading":
            percent = d.get("_percent_str", "").strip().replace("%", "") or "0"
            speed = d.get("_speed_str", "")
            eta = d.get("_eta_str", "")
            update_job(
                job_id,
                status="downloading",
                progress=f"{percent}%",
                speed=speed,
                eta=eta,
                message=f"Downloading... {percent}%"
            )
        elif d["status"] == "finished":
            update_job(
                job_id,
                status="processing",
                progress="100%",
                message="Post-processing (merging/converting)..."
            )
    return hook

def get_ydl_opts(job_id: str, dtype: str, format_id: str = None) -> dict:
    opts = {
        "outtmpl": f"{DOWNLOAD_FOLDER}/{job_id}_%(title).100s.%(ext)s",
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "progress_hooks": [build_progress_hook(job_id)],
        "retries": 10,
        "fragment_retries": 10,
        "file_access_retries": 5,
        "skip_unavailable_fragments": True,
        "keep_fragments": False,
        "buffersize": 32768,
        "http_chunk_size": 10485760,
    }

    if dtype == "video":
        if format_id and format_id not in ("best", "bestvideo+bestaudio/best"):
            opts["format"] = f"{format_id}+bestaudio[ext=m4a]/bestaudio/best"
        else:
            opts["format"] = "bestvideo[ext=mp4][vcodec^=avc1]+bestaudio[ext=m4a]/bestvideo[ext=mp4]+bestaudio[ext=m4a]/bestvideo+bestaudio/best"
        opts["merge_output_format"] = "mp4"
    else:
        if format_id and format_id not in ("best", "bestaudio/best"):
            opts["format"] = format_id
        else:
            opts["format"] = "bestaudio[ext=m4a]/bestaudio/best"
        opts["postprocessors"] = [{
            "key": "FFmpegExtractAudio",
            "preferredcodec": "mp3",
            "preferredquality": "320",
        }]

    return opts

def find_output_file(job_id: str, dtype: str) -> str:
    """Find the actual output file after yt-dlp finishes"""
    expected_ext = ".mp3" if dtype == "audio" else ".mp4"

    # First try exact match pattern
    for f in os.listdir(DOWNLOAD_FOLDER):
        if f.startswith(job_id + "_"):
            full = os.path.join(DOWNLOAD_FOLDER, f)
            if os.path.isfile(full):
                # If it is the right extension, return it
                if f.endswith(expected_ext):
                    return full
                # If it is a temp file, skip
                if f.endswith((".part", ".ytdl", ".temp")):
                    continue

    # If no exact match, look for any file starting with job_id
    for f in os.listdir(DOWNLOAD_FOLDER):
        if f.startswith(job_id + "_") and os.path.isfile(os.path.join(DOWNLOAD_FOLDER, f)):
            if not f.endswith((".part", ".ytdl", ".temp")):
                return os.path.join(DOWNLOAD_FOLDER, f)

    return None

# ═══════════════════════════════════════════════════════════════
# DOWNLOAD WORKER
# ═══════════════════════════════════════════════════════════════

def download_worker(job_id: str, url: str, dtype: str, format_id: str):
    try:
        log_job(job_id, f"Starting {dtype} download")
        update_job(job_id, status="starting", message="Initializing download...")

        opts = get_ydl_opts(job_id, dtype, format_id)

        with yt_dlp.YoutubeDL(opts) as ydl:
            # First extract info to get title
            info = ydl.extract_info(url, download=False)
            title = info.get("title", "Unknown")
            update_job(job_id, title=title)

            # Check estimated file size
            filesize = info.get("filesize") or info.get("filesize_approx", 0)
            if filesize and filesize > MAX_DOWNLOAD_SIZE_MB * 1024 * 1024:
                raise Exception(f"File too large. Max allowed: {MAX_DOWNLOAD_SIZE_MB}MB")

            log_job(job_id, f"Title: {title}")
            update_job(job_id, message=f"Downloading: {title[:50]}...")

            # Perform download
            ydl.download([url])

        # Find the resulting file
        update_job(job_id, status="finalizing", message="Locating file...")
        time.sleep(1)  # Allow filesystem to settle

        filepath = find_output_file(job_id, dtype)

        if not filepath or not os.path.exists(filepath):
            raise Exception("Download completed but file not found. FFmpeg may not be installed.")

        # Verify file is readable and non-empty
        if os.path.getsize(filepath) < 1024:
            raise Exception("Downloaded file is too small or corrupted.")

        # Build clean filename
        clean_title = re.sub(r"[\\/*?:\"<>|]", "", title).strip()[:60]
        ext = ".mp3" if dtype == "audio" else ".mp4"
        filename = f"{clean_title}{ext}"

        update_job(
            job_id,
            status="done",
            progress="100%",
            message="Complete! Click below to save.",
            filepath=filepath,
            filename=filename
        )
        log_job(job_id, "Download complete")

    except Exception as e:
        error_msg = str(e)
        log_job(job_id, f"ERROR: {error_msg}")
        update_job(
            job_id,
            status="error",
            message=error_msg,
            error=error_msg
        )

# ═══════════════════════════════════════════════════════════════
# HTML TEMPLATE - LV99 ULTRA UI
# ═══════════════════════════════════════════════════════════════

HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>HQ Tube LV99 — Ultimate Downloader</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800;900&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
    <script>
        tailwind.config = {
            theme: {
                extend: {
                    fontFamily: {
                        sans: ['Inter', 'sans-serif'],
                        mono: ['JetBrains Mono', 'monospace'],
                    },
                    animation: {
                        'pulse-slow': 'pulse 3s cubic-bezier(0.4, 0, 0.6, 1) infinite',
                        'shimmer': 'shimmer 2s linear infinite',
                        'float': 'float 6s ease-in-out infinite',
                        'glow': 'glow 2s ease-in-out infinite alternate',
                    },
                    keyframes: {
                        shimmer: {
                            '0%': { backgroundPosition: '-200% 0' },
                            '100%': { backgroundPosition: '200% 0' },
                        },
                        float: {
                            '0%, 100%': { transform: 'translateY(0)' },
                            '50%': { transform: 'translateY(-20px)' },
                        },
                        glow: {
                            '0%': { boxShadow: '0 0 20px rgba(239,68,68,0.3)' },
                            '100%': { boxShadow: '0 0 40px rgba(239,68,68,0.6)' },
                        }
                    }
                }
            }
        }
    </script>
    <style>
        body {
            background-color: #050508;
            background-image: 
                radial-gradient(ellipse 80% 50% at 50% -20%, rgba(120, 50, 50, 0.15), transparent),
                radial-gradient(ellipse 60% 40% at 80% 80%, rgba(50, 50, 120, 0.1), transparent);
        }
        .glass-panel {
            background: rgba(20, 20, 28, 0.7);
            backdrop-filter: blur(20px);
            border: 1px solid rgba(255, 255, 255, 0.06);
            box-shadow: 0 25px 50px -12px rgba(0, 0, 0, 0.5);
        }
        .neon-border {
            position: relative;
        }
        .neon-border::before {
            content: '';
            position: absolute;
            inset: -1px;
            border-radius: inherit;
            padding: 1px;
            background: linear-gradient(135deg, rgba(239,68,68,0.5), rgba(168,85,247,0.3), rgba(59,130,246,0.5));
            -webkit-mask: linear-gradient(#fff 0 0) content-box, linear-gradient(#fff 0 0);
            -webkit-mask-composite: xor;
            mask-composite: exclude;
            pointer-events: none;
        }
        .progress-glow {
            box-shadow: 0 0 10px rgba(239,68,68,0.5), 0 0 20px rgba(239,68,68,0.3);
        }
        .format-card {
            transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1);
        }
        .format-card:hover {
            transform: translateY(-2px);
        }
        .format-card.selected {
            border-color: #ef4444;
            background: rgba(239, 68, 68, 0.1);
            box-shadow: 0 0 20px rgba(239, 68, 68, 0.2);
        }
        .format-card.selected-audio {
            border-color: #10b981;
            background: rgba(16, 185, 129, 0.1);
            box-shadow: 0 0 20px rgba(16, 185, 129, 0.2);
        }
        .scrollbar-hide::-webkit-scrollbar { display: none; }
        .scrollbar-hide { -ms-overflow-style: none; scrollbar-width: none; }

        @keyframes scanline {
            0% { transform: translateY(-100%); }
            100% { transform: translateY(100%); }
        }
        .scanline::after {
            content: '';
            position: absolute;
            inset: 0;
            background: linear-gradient(to bottom, transparent 50%, rgba(239,68,68,0.03) 51%, transparent 52%);
            background-size: 100% 4px;
            pointer-events: none;
        }
    </style>
</head>
<body class="min-h-screen text-gray-200 font-sans overflow-x-hidden">
    <!-- Background Elements -->
    <div class="fixed inset-0 pointer-events-none overflow-hidden">
        <div class="absolute top-1/4 left-1/4 w-96 h-96 bg-red-600/5 rounded-full blur-3xl animate-float"></div>
        <div class="absolute bottom-1/4 right-1/4 w-96 h-96 bg-purple-600/5 rounded-full blur-3xl animate-float" style="animation-delay: -3s;"></div>
    </div>

    <div class="relative z-10 max-w-4xl mx-auto px-4 py-8 md:py-16">
        <!-- Header -->
        <header class="text-center mb-12">
            <div class="inline-flex items-center gap-3 mb-4 px-4 py-1.5 rounded-full bg-white/5 border border-white/10 text-xs font-mono text-gray-400 tracking-widest uppercase">
                <span class="w-2 h-2 rounded-full bg-green-500 animate-pulse"></span>
                System Online
            </div>
            <h1 class="text-5xl md:text-7xl font-black tracking-tighter mb-4">
                <span class="bg-gradient-to-r from-red-500 via-pink-500 to-purple-500 bg-clip-text text-transparent">HQ TUBE</span>
                <span class="text-red-500/80 text-2xl md:text-3xl align-top ml-2 font-mono font-bold">LV99</span>
            </h1>
            <p class="text-gray-500 text-lg max-w-md mx-auto">Maximum quality extraction. Zero compromises. Bulletproof reliability.</p>
        </header>

        <!-- Main Interface -->
        <main class="glass-panel rounded-3xl p-6 md:p-10 neon-border scanline">

            <!-- URL Input Section -->
            <div class="relative mb-8 group">
                <div class="absolute -inset-0.5 bg-gradient-to-r from-red-600 to-purple-600 rounded-2xl opacity-30 group-hover:opacity-50 transition duration-500 blur"></div>
                <div class="relative flex gap-2">
                    <input 
                        type="text" 
                        id="urlInput" 
                        placeholder="Paste YouTube URL here..." 
                        class="flex-1 px-6 py-5 bg-black/60 border border-white/10 rounded-2xl text-white placeholder-gray-600 text-lg focus:outline-none focus:border-red-500/50 transition-all font-mono text-sm md:text-base"
                        autocomplete="off"
                    >
                    <button 
                        onclick="analyzeURL()" 
                        id="analyzeBtn"
                        class="px-8 py-5 bg-gradient-to-r from-red-600 to-pink-600 hover:from-red-500 hover:to-pink-500 rounded-2xl font-bold text-white transition-all transform hover:scale-105 active:scale-95 shadow-lg shadow-red-900/30 whitespace-nowrap"
                    >
                        <span id="analyzeText">ANALYZE</span>
                    </button>
                </div>
            </div>

            <!-- Video Preview -->
            <div id="previewSection" class="hidden mb-8 animate-[fadeIn_0.5s_ease-out]">
                <div class="flex flex-col md:flex-row gap-6 p-5 bg-black/30 rounded-2xl border border-white/5">
                    <div class="relative shrink-0 mx-auto md:mx-0">
                        <img id="thumbImg" class="w-full md:w-56 h-32 object-cover rounded-xl shadow-2xl" alt="">
                        <div id="durationBadge" class="absolute bottom-2 right-2 px-2 py-0.5 bg-black/80 rounded text-xs font-mono text-white"></div>
                    </div>
                    <div class="flex-1 min-w-0 text-center md:text-left">
                        <h3 id="videoTitle" class="font-bold text-white text-xl mb-2 leading-tight line-clamp-2"></h3>
                        <p id="videoAuthor" class="text-gray-400 text-sm mb-3"></p>
                        <div class="flex flex-wrap gap-2 justify-center md:justify-start">
                            <span id="badgeViews" class="px-3 py-1 bg-white/5 rounded-full text-xs text-gray-400 border border-white/5"></span>
                            <span id="badgeDate" class="px-3 py-1 bg-white/5 rounded-full text-xs text-gray-400 border border-white/5"></span>
                        </div>
                    </div>
                </div>
            </div>

            <!-- Format Selection -->
            <div id="formatSection" class="hidden mb-8">
                <div class="flex items-center justify-between mb-4">
                    <h3 class="text-sm font-bold text-gray-400 uppercase tracking-widest">Select Format</h3>
                    <div class="flex gap-2">
                        <button onclick="filterFormats('all')" class="format-filter px-3 py-1 rounded-lg text-xs bg-white/10 text-white hover:bg-white/20 transition active" data-filter="all">All</button>
                        <button onclick="filterFormats('video')" class="format-filter px-3 py-1 rounded-lg text-xs bg-white/5 text-gray-400 hover:bg-white/10 transition" data-filter="video">Video</button>
                        <button onclick="filterFormats('audio')" class="format-filter px-3 py-1 rounded-lg text-xs bg-white/5 text-gray-400 hover:bg-white/10 transition" data-filter="audio">Audio</button>
                    </div>
                </div>
                <div id="formatGrid" class="grid grid-cols-2 md:grid-cols-4 gap-3 max-h-64 overflow-y-auto scrollbar-hide pr-1">
                    <!-- Injected by JS -->
                </div>
            </div>

            <!-- Action Button -->
            <button 
                onclick="startDownload()" 
                id="downloadBtn"
                disabled
                class="w-full py-5 bg-gradient-to-r from-gray-700 to-gray-800 rounded-2xl font-black text-lg tracking-wider text-gray-500 transition-all cursor-not-allowed relative overflow-hidden group"
            >
                <span id="downloadBtnText" class="relative z-10">SELECT A FORMAT</span>
                <div id="btnGlow" class="absolute inset-0 opacity-0 transition-opacity duration-300 bg-gradient-to-r from-red-600 via-pink-600 to-purple-600"></div>
            </button>

            <!-- Progress Monitor -->
            <div id="progressSection" class="hidden mt-8">
                <div class="flex justify-between items-end mb-3">
                    <div>
                        <p id="statusText" class="text-sm font-medium text-gray-300 mb-1">Initializing...</p>
                        <p id="detailText" class="text-xs text-gray-500 font-mono"></p>
                    </div>
                    <span id="percentText" class="text-3xl font-black font-mono text-red-400">0%</span>
                </div>

                <div class="h-4 bg-gray-800/80 rounded-full overflow-hidden border border-white/5 relative">
                    <div id="progressBar" class="h-full bg-gradient-to-r from-red-500 via-pink-500 to-purple-500 progress-glow relative" style="width: 0%">
                        <div class="absolute inset-0 bg-white/20 animate-shimmer" style="background-size: 200% 100%"></div>
                    </div>
                </div>

                <div class="flex justify-between mt-2 text-xs font-mono text-gray-500">
                    <span id="speedText"></span>
                    <span id="etaText"></span>
                </div>

                <!-- Live Log -->
                <div id="logPanel" class="mt-4 p-3 bg-black/50 rounded-xl border border-white/5 hidden">
                    <div class="flex items-center gap-2 mb-2">
                        <div class="w-1.5 h-1.5 rounded-full bg-green-500 animate-pulse"></div>
                        <span class="text-xs font-mono text-gray-400 uppercase tracking-wider">Live Log</span>
                    </div>
                    <div id="logContent" class="text-xs font-mono text-gray-500 space-y-0.5 max-h-32 overflow-y-auto scrollbar-hide"></div>
                </div>
            </div>

            <!-- Success State -->
            <div id="successSection" class="hidden mt-8 text-center animate-[fadeIn_0.5s_ease-out]">
                <div class="inline-flex flex-col items-center p-8 bg-gradient-to-b from-green-900/20 to-transparent border border-green-500/20 rounded-3xl">
                    <div class="w-16 h-16 mb-4 rounded-full bg-green-500/20 flex items-center justify-center">
                        <svg class="w-8 h-8 text-green-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"></path></svg>
                    </div>
                    <h3 class="text-xl font-bold text-white mb-2">Download Ready</h3>
                    <p id="successFilename" class="text-sm text-gray-400 mb-6 max-w-xs truncate"></p>
                    <a id="downloadLink" href="#" class="inline-flex items-center gap-2 px-8 py-4 bg-green-600 hover:bg-green-500 rounded-2xl font-bold text-white transition-all transform hover:scale-105 shadow-lg shadow-green-900/30">
                        <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4"></path></svg>
                        Save File
                    </a>
                </div>
            </div>

            <!-- Error State -->
            <div id="errorSection" class="hidden mt-8 p-6 bg-red-900/10 border border-red-500/20 rounded-2xl animate-[fadeIn_0.3s_ease-out]">
                <div class="flex items-start gap-4">
                    <div class="w-10 h-10 rounded-full bg-red-500/20 flex items-center justify-center shrink-0">
                        <svg class="w-5 h-5 text-red-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"></path></svg>
                    </div>
                    <div>
                        <h4 class="font-bold text-red-400 mb-1">Download Failed</h4>
                        <p id="errorText" class="text-sm text-gray-400"></p>
                        <button onclick="resetUI()" class="mt-3 text-xs text-red-400 hover:text-red-300 underline">Try Again</button>
                    </div>
                </div>
            </div>
        </main>

        <!-- Stats Grid -->
        <div class="grid grid-cols-2 md:grid-cols-4 gap-4 mt-8">
            <div class="glass-panel rounded-2xl p-5 text-center hover:bg-white/5 transition cursor-default">
                <div class="text-2xl mb-2">🎬</div>
                <p class="text-sm font-bold text-white">4K/8K Video</p>
                <p class="text-xs text-gray-500 mt-1">Max Resolution</p>
            </div>
            <div class="glass-panel rounded-2xl p-5 text-center hover:bg-white/5 transition cursor-default">
                <div class="text-2xl mb-2">🎵</div>
                <p class="text-sm font-bold text-white">320kbps MP3</p>
                <p class="text-xs text-gray-500 mt-1">Lossless Audio</p>
            </div>
            <div class="glass-panel rounded-2xl p-5 text-center hover:bg-white/5 transition cursor-default">
                <div class="text-2xl mb-2">⚡</div>
                <p class="text-sm font-bold text-white">Live Progress</p>
                <p class="text-xs text-gray-500 mt-1">Real-time SSE</p>
            </div>
            <div class="glass-panel rounded-2xl p-5 text-center hover:bg-white/5 transition cursor-default">
                <div class="text-2xl mb-2">🛡️</div>
                <p class="text-sm font-bold text-white">Auto-Cleanup</p>
                <p class="text-xs text-gray-500 mt-1">30min Expiry</p>
            </div>
        </div>

        <footer class="mt-12 text-center text-xs text-gray-600">
            <p>HQ Tube LV99 — Files are temporary and automatically deleted after 30 minutes</p>
            <p class="mt-1">Built with yt-dlp + Flask + Bulletproof Error Handling</p>
        </footer>
    </div>

    <script>
        let currentJob = null;
        let selectedFormat = null;
        let eventSource = null;
        let allFormats = [];

        function setAnalyzeLoading(loading) {
            const btn = document.getElementById('analyzeBtn');
            const text = document.getElementById('analyzeText');
            if (loading) {
                btn.disabled = true;
                btn.classList.add('opacity-75', 'cursor-wait');
                text.innerHTML = '<span class="inline-flex items-center gap-2"><svg class="animate-spin h-4 w-4" viewBox="0 0 24 24"><circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4" fill="none"/><path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"/></svg>SCANNING</span>';
            } else {
                btn.disabled = false;
                btn.classList.remove('opacity-75', 'cursor-wait');
                text.textContent = 'ANALYZE';
            }
        }

        function setDownloadState(state) {
            const btn = document.getElementById('downloadBtn');
            const text = document.getElementById('downloadBtnText');
            const glow = document.getElementById('btnGlow');

            if (state === 'ready') {
                btn.disabled = false;
                btn.classList.remove('cursor-not-allowed', 'from-gray-700', 'to-gray-800', 'text-gray-500');
                btn.classList.add('cursor-pointer', 'text-white');
                glow.classList.remove('opacity-0');
                glow.classList.add('opacity-100');
                text.textContent = 'INITIATE DOWNLOAD';
            } else if (state === 'loading') {
                btn.disabled = true;
                text.innerHTML = '<span class="inline-flex items-center gap-2"><svg class="animate-spin h-5 w-5" viewBox="0 0 24 24"><circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4" fill="none"/><path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"/></svg>PROCESSING...</span>';
            } else {
                btn.disabled = true;
                btn.classList.add('cursor-not-allowed', 'from-gray-700', 'to-gray-800', 'text-gray-500');
                btn.classList.remove('cursor-pointer', 'text-white');
                glow.classList.add('opacity-0');
                glow.classList.remove('opacity-100');
                text.textContent = 'SELECT A FORMAT';
            }
        }

        function showSection(id, show) {
            const el = document.getElementById(id);
            if (show) el.classList.remove('hidden');
            else el.classList.add('hidden');
        }

        function resetUI() {
            showSection('previewSection', false);
            showSection('formatSection', false);
            showSection('progressSection', false);
            showSection('successSection', false);
            showSection('errorSection', false);
            showSection('logPanel', false);
            document.getElementById('urlInput').value = '';
            document.getElementById('progressBar').style.width = '0%';
            document.getElementById('percentText').textContent = '0%';
            setDownloadState('disabled');
            if (eventSource) { eventSource.close(); eventSource = null; }
            selectedFormat = null;
            currentJob = null;
            allFormats = [];
        }

        async function analyzeURL() {
            const url = document.getElementById('urlInput').value.trim();
            if (!url) return;

            setAnalyzeLoading(true);
            resetUI();

            try {
                const res = await fetch('/info', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ url })
                });
                const data = await res.json();

                if (!res.ok) throw new Error(data.error || 'Analysis failed');

                document.getElementById('thumbImg').src = data.thumbnail;
                document.getElementById('videoTitle').textContent = data.title;
                document.getElementById('videoAuthor').textContent = data.uploader;
                document.getElementById('durationBadge').textContent = data.duration;
                document.getElementById('badgeViews').textContent = data.view_count ? parseInt(data.view_count).toLocaleString() + ' views' : '';
                document.getElementById('badgeDate').textContent = data.upload_date || '';
                showSection('previewSection', true);

                allFormats = [];
                const grid = document.getElementById('formatGrid');
                grid.innerHTML = '';

                data.formats.video.forEach((f) => {
                    allFormats.push({...f, type: 'video'});
                    const el = document.createElement('button');
                    el.className = 'format-card group relative p-4 bg-white/5 border border-white/10 rounded-xl text-left hover:border-red-500/50';
                    el.dataset.type = 'video';
                    el.onclick = () => selectFormat(f.format_id, 'video', el);
                    el.innerHTML = `
                        <div class="flex items-center justify-between mb-2">
                            <span class="font-bold text-white">${f.quality}</span>
                            <span class="text-xs px-2 py-0.5 bg-red-500/20 text-red-400 rounded">MP4</span>
                        </div>
                        <div class="text-xs text-gray-500 group-hover:text-gray-400 transition">${f.size || 'Auto size'}</div>
                        <div class="absolute top-2 right-2 opacity-0 group-hover:opacity-100 transition">
                            <svg class="w-4 h-4 text-red-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"></path></svg>
                        </div>
                    `;
                    grid.appendChild(el);
                });

                data.formats.audio.forEach((f) => {
                    allFormats.push({...f, type: 'audio'});
                    const el = document.createElement('button');
                    el.className = 'format-card group relative p-4 bg-white/5 border border-white/10 rounded-xl text-left hover:border-emerald-500/50';
                    el.dataset.type = 'audio';
                    el.onclick = () => selectFormat(f.format_id, 'audio', el);
                    el.innerHTML = `
                        <div class="flex items-center justify-between mb-2">
                            <span class="font-bold text-white">${f.quality}</span>
                            <span class="text-xs px-2 py-0.5 bg-emerald-500/20 text-emerald-400 rounded">MP3</span>
                        </div>
                        <div class="text-xs text-gray-500 group-hover:text-gray-400 transition">${f.size || 'Auto size'}</div>
                        <div class="absolute top-2 right-2 opacity-0 group-hover:opacity-100 transition">
                            <svg class="w-4 h-4 text-emerald-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"></path></svg>
                        </div>
                    `;
                    grid.appendChild(el);
                });

                showSection('formatSection', true);

            } catch (err) {
                showError(err.message);
            } finally {
                setAnalyzeLoading(false);
            }
        }

        function filterFormats(type) {
            document.querySelectorAll('.format-filter').forEach(b => {
                if (b.dataset.filter === type) {
                    b.classList.add('bg-white/10', 'text-white', 'active');
                    b.classList.remove('bg-white/5', 'text-gray-400');
                } else {
                    b.classList.remove('bg-white/10', 'text-white', 'active');
                    b.classList.add('bg-white/5', 'text-gray-400');
                }
            });

            document.querySelectorAll('.format-card').forEach(card => {
                if (type === 'all' || card.dataset.type === type) {
                    card.style.display = 'block';
                } else {
                    card.style.display = 'none';
                }
            });
        }

        function selectFormat(formatId, type, element) {
            selectedFormat = { format_id: formatId, type };

            document.querySelectorAll('.format-card').forEach(c => {
                c.classList.remove('selected', 'selected-audio');
            });

            if (type === 'video') {
                element.classList.add('selected');
            } else {
                element.classList.add('selected-audio');
            }

            setDownloadState('ready');
        }

        async function startDownload() {
            if (!selectedFormat) return;
            const url = document.getElementById('urlInput').value.trim();

            setDownloadState('loading');
            showSection('progressSection', true);
            showSection('successSection', false);
            showSection('errorSection', false);
            showSection('logPanel', true);

            try {
                const res = await fetch('/download', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        url,
                        type: selectedFormat.type,
                        format_id: selectedFormat.format_id
                    })
                });
                const data = await res.json();

                if (!res.ok) throw new Error(data.error || 'Failed to start');

                currentJob = data.job_id;
                connectSSE(currentJob);

            } catch (err) {
                showError(err.message);
                setDownloadState('ready');
            }
        }

        function connectSSE(jobId) {
            if (eventSource) eventSource.close();

            eventSource = new EventSource(`/stream/${jobId}`);

            eventSource.onmessage = (e) => {
                try {
                    const data = JSON.parse(e.data);
                    updateProgress(data);
                } catch {}
            };

            eventSource.onerror = () => {
                eventSource.close();
            };
        }

        function updateProgress(data) {
            const bar = document.getElementById('progressBar');
            const pct = document.getElementById('percentText');
            const status = document.getElementById('statusText');
            const detail = document.getElementById('detailText');
            const speed = document.getElementById('speedText');
            const eta = document.getElementById('etaText');
            const logContent = document.getElementById('logContent');

            let percent = 0;
            if (data.progress) {
                const match = data.progress.match(/([\d.]+)/);
                if (match) percent = parseFloat(match[1]);
            }

            bar.style.width = Math.min(percent, 100) + '%';
            pct.textContent = data.progress || '0%';
            status.textContent = data.message || data.status;
            detail.textContent = data.title || '';
            speed.textContent = data.speed ? '⚡ ' + data.speed : '';
            eta.textContent = data.eta ? 'ETA: ' + data.eta : '';

            if (data.logs && data.logs.length > 0) {
                logContent.innerHTML = data.logs.slice(-5).map(l => '<div class="truncate">> ' + l + '</div>').join('');
                logContent.scrollTop = logContent.scrollHeight;
            }

            if (data.status === 'done') {
                eventSource.close();
                showSection('progressSection', false);
                showSection('successSection', true);
                document.getElementById('successFilename').textContent = data.filename || 'download';
                document.getElementById('downloadLink').href = `/file/${currentJob}`;
                setDownloadState('disabled');
            } else if (data.status === 'error') {
                eventSource.close();
                showError(data.message || 'Unknown error');
                setDownloadState('ready');
            }
        }

        function showError(msg) {
            showSection('errorSection', true);
            document.getElementById('errorText').textContent = msg;
            showSection('progressSection', false);
            showSection('logPanel', false);
        }

        document.getElementById('urlInput').addEventListener('keypress', (e) => {
            if (e.key === 'Enter') analyzeURL();
        });
    </script>
</body>
</html>
"""

# ═══════════════════════════════════════════════════════════════
# ROUTES
# ═══════════════════════════════════════════════════════════════

@app.route("/")
def index():
    cleanup_expired()
    return render_template_string(HTML)

@app.route("/info", methods=["POST"])
def get_info():
    data = request.get_json()
    url = data.get("url") if data else None

    if not validate_youtube_url(url):
        return jsonify({"error": "Invalid or unsupported YouTube URL"}), 400

    try:
        opts = {
            "quiet": True,
            "no_warnings": True,
            "extract_flat": False,
            "skip_download": True,
        }

        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=False)

            # Gather formats
            video_formats = []
            audio_formats = []
            seen_vq = set()
            seen_aq = set()

            for f in info.get("formats", []):
                # Video streams (no audio, or combined)
                if f.get("vcodec") and f.get("vcodec") != "none":
                    height = f.get("height", 0)
                    fps = f.get("fps", 0)
                    if height and height >= 360 and height not in seen_vq:
                        seen_vq.add(height)
                        filesize = f.get("filesize") or f.get("filesize_approx")
                        label = f"{height}p"
                        if fps and fps > 30:
                            label += f" {int(fps)}fps"
                        video_formats.append({
                            "format_id": f["format_id"],
                            "quality": label,
                            "ext": f.get("ext", "mp4"),
                            "size": format_size(filesize)
                        })

                # Audio streams
                if f.get("acodec") and f.get("acodec") != "none" and (not f.get("vcodec") or f.get("vcodec") == "none"):
                    abr = f.get("abr", 0)
                    if abr and abr >= 128 and int(abr) not in seen_aq:
                        seen_aq.add(int(abr))
                        filesize = f.get("filesize") or f.get("filesize_approx")
                        audio_formats.append({
                            "format_id": f["format_id"],
                            "quality": f"{int(abr)}kbps",
                            "ext": f.get("ext", "m4a"),
                            "size": format_size(filesize)
                        })

            # Sort and limit
            video_formats = sorted(video_formats, key=lambda x: int(x["quality"].split("p")[0]), reverse=True)[:6]
            audio_formats = sorted(audio_formats, key=lambda x: int(x["quality"].replace("kbps", "")), reverse=True)[:4]

            # Add best options at top
            video_formats.insert(0, {
                "format_id": "best",
                "quality": "Best Quality",
                "ext": "mp4",
                "size": "Auto"
            })
            audio_formats.insert(0, {
                "format_id": "best",
                "quality": "Best Audio",
                "ext": "mp3",
                "size": "Auto"
            })

            return jsonify({
                "title": info.get("title", "Unknown"),
                "thumbnail": info.get("thumbnail", ""),
                "uploader": info.get("uploader", "Unknown"),
                "duration": format_duration(info.get("duration")),
                "view_count": info.get("view_count"),
                "upload_date": info.get("upload_date"),
                "formats": {
                    "video": video_formats,
                    "audio": audio_formats
                }
            })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/download", methods=["POST"])
def start_download():
    data = request.get_json()
    url = data.get("url") if data else None
    dtype = data.get("type", "video")
    format_id = data.get("format_id", "best")

    if not validate_youtube_url(url):
        return jsonify({"error": "Invalid YouTube URL"}), 400

    job_id = create_job()

    # Submit to thread pool
    executor.submit(download_worker, job_id, url, dtype, format_id)

    return jsonify({"job_id": job_id, "status": "queued"}), 200

@app.route("/stream/<job_id>")
def stream_progress(job_id):
    def generate():
        last_data = None
        retries = 0
        while retries < 7200:  # Max 1 hour
            job = get_job(job_id)
            if not job:
                yield f"data: {json.dumps({'status': 'error', 'message': 'Job not found'})}\n\n"
                break

            # Only send if changed
            current = {k: v for k, v in job.items() if k != "updated"}
            if current != last_data:
                last_data = current.copy()
                yield f"data: {json.dumps(job)}\n\n"

            if job.get("status") in ("done", "error"):
                break

            time.sleep(0.5)
            retries += 1

    return Response(stream_with_context(generate()), mimetype="text/event-stream")

@app.route("/file/<job_id>")
def serve_file(job_id):
    job = get_job(job_id)

    if not job:
        return "Job expired or not found", 404

    filepath = job.get("filepath")
    filename = job.get("filename", "download")

    if not filepath or not os.path.exists(filepath):
        return "File not found or expired", 404

    try:
        mime = "audio/mpeg" if filename.endswith(".mp3") else "video/mp4"
        return send_file(
            filepath,
            as_attachment=True,
            download_name=filename,
            mimetype=mime
        )
    except Exception as e:
        return str(e), 500

@app.route("/health")
def health():
    cleanup_expired()
    return jsonify({
        "status": "ok",
        "active_jobs": len(jobs),
        "download_dir": DOWNLOAD_FOLDER,
        "timestamp": datetime.now().isoformat()
    })

# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("""
    ╔══════════════════════════════════════════════════════════════╗
    ║                                                              ║
    ║   HQ TUBE LV99 — Ultimate YouTube Downloader               ║
    ║                                                              ║
    ║   Local: http://127.0.0.1:5000                               ║
    ║   Health: http://127.0.0.1:5000/health                       ║
    ║                                                              ║
    ╚══════════════════════════════════════════════════════════════╝
    """)
    app.run(host="0.0.0.0", port=5000, debug=False, threaded=True)
