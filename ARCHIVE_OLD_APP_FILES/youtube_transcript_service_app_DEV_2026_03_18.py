from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlparse

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from yt_dlp import YoutubeDL
from youtube_transcript_api import YouTubeTranscriptApi

from trusted_sources import ALL_TRUSTED_TERMS


app = FastAPI(title="Trusted Bible Source Search")


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class TranscriptRequest(BaseModel):
    video_url: Optional[str] = None
    query: Optional[str] = None
    preferred_language: str = "en"
    trusted_only: bool = True
    max_search_results: int = 10


def extract_video_id(video_url: str) -> Optional[str]:
    if not video_url:
        return None

    if "youtu.be/" in video_url:
        return video_url.rstrip("/").split("/")[-1].split("?")[0]

    parsed = urlparse(video_url)

    if parsed.hostname and "youtube.com" in parsed.hostname:
        query_params = parse_qs(parsed.query)
        if "v" in query_params:
            return query_params["v"][0]

    return None


def transcript_items_to_text(items: Any) -> str:
    return "\n".join([item.get("text", "") for item in items]).strip()


def normalize_text(value: Optional[str]) -> str:
    return (value or "").lower()


def is_trusted_result(entry: Dict[str, Any]) -> bool:
    haystack = (
        normalize_text(entry.get("title")) +
        normalize_text(entry.get("uploader")) +
        normalize_text(entry.get("channel"))
    )
    return any(term in haystack for term in ALL_TRUSTED_TERMS)


# 🔥 NEW: MULTI-PASS QUERY EXPANSION
def build_queries(query: str) -> List[str]:
    base = query.strip()

    queries = [
        base,
        f"{base} commentary",
        f"{base} bible study",
        f"{base} explained",
        f"{base.split(':')[0]} commentary",
        f"{base.split(':')[0]} background",
    ]

    # Daniel-specific boost
    if "daniel" in base.lower():
        queries.extend([
            "Daniel 1 commentary",
            "Daniel 1 Babylon exile",
            "Jehoiakim Nebuchadnezzar Daniel",
        ])

    return list(set(queries))


def search_youtube(query: str, trusted_only: bool) -> Optional[Dict[str, Any]]:
    ydl_opts = {
        "quiet": True,
        "skip_download": True,
        "extract_flat": True,
        "noplaylist": True,
    }

    with YoutubeDL(ydl_opts) as ydl:
        results = ydl.extract_info(f"ytsearch5:{query}", download=False)

    entries = (results or {}).get("entries") or []
    entries = [e for e in entries if e and e.get("id")]

    if not entries:
        return None

    if trusted_only:
        entries = [e for e in entries if is_trusted_result(e)]
        if not entries:
            return None

    return entries[0]


def fetch_transcript(video_id: str) -> Optional[str]:
    try:
        transcript = YouTubeTranscriptApi().fetch(video_id)
        return transcript_items_to_text(transcript)
    except Exception:
        return None


@app.post("/get_youtube_transcript")
def get_youtube_transcript(payload: TranscriptRequest) -> Dict[str, Any]:

    debug_queries = []
    selected_video = None

    if payload.query:
        queries = build_queries(payload.query)

        for q in queries:
            debug_queries.append(q)
            result = search_youtube(q, payload.trusted_only)
            if result:
                selected_video = result
                break

    if not selected_video:
        return {
            "success": False,
            "trusted_match": False,
            "transcript_text": None,
            "error_message": "No usable results found",
            "debug_queries": debug_queries
        }

    video_id = selected_video["id"]
    transcript = fetch_transcript(video_id)

    return {
        "success": True,
        "source_type": "YouTube transcript",
        "video_url": f"https://www.youtube.com/watch?v={video_id}",
        "title": selected_video.get("title"),
        "channel": selected_video.get("channel"),
        "trusted_match": True,
        "transcript_text": transcript[:4000] if transcript else None,
        "debug_queries": debug_queries
    }
