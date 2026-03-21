from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlparse

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from yt_dlp import YoutubeDL
from youtube_transcript_api import YouTubeTranscriptApi

from trusted_sources import ALL_TRUSTED_TERMS


app = FastAPI(title="YouTube Transcript Service")


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

    video_url = video_url.strip()

    if "youtu.be/" in video_url:
        return video_url.rstrip("/").split("/")[-1].split("?")[0]

    parsed = urlparse(video_url)

    if parsed.hostname and "youtube.com" in parsed.hostname:
        query_params = parse_qs(parsed.query)
        if "v" in query_params and query_params["v"]:
            return query_params["v"][0]

        path_parts = [part for part in parsed.path.split("/") if part]
        if len(path_parts) >= 2 and path_parts[0] in {"shorts", "live", "embed"}:
            return path_parts[1]

    return None


def transcript_items_to_text(items: Any) -> str:
    lines: List[str] = []

    for item in items:
        if hasattr(item, "text"):
            text = item.text
        elif isinstance(item, dict):
            text = item.get("text", "")
        else:
            text = str(item)

        text = text.strip()
        if text:
            lines.append(text)

    return "\n".join(lines).strip()


def normalize_text(value: Optional[str]) -> str:
    if not value:
        return ""
    return value.strip().lower()


def is_trusted_result(entry: Dict[str, Any]) -> bool:
    title = normalize_text(entry.get("title"))
    uploader = normalize_text(entry.get("uploader"))
    channel = normalize_text(entry.get("channel"))
    description = normalize_text(entry.get("description"))

    haystack = f"{title} {uploader} {channel} {description}"

    return any(term in haystack for term in ALL_TRUSTED_TERMS)


def search_youtube(
    query: str,
    trusted_only: bool = True,
    max_search_results: int = 10,
) -> Optional[Dict[str, Any]]:
    safe_max = max(1, min(max_search_results, 25))

    ydl_opts = {
        "quiet": True,
        "skip_download": True,
        "extract_flat": True,
        "noplaylist": True,
    }

    with YoutubeDL(ydl_opts) as ydl:
        results = ydl.extract_info(f"ytsearch{safe_max}:{query}", download=False)

    entries = (results or {}).get("entries") or []
    if not entries:
        return None

    cleaned_entries = [entry for entry in entries if entry and entry.get("id")]
    if not cleaned_entries:
        return None

    if trusted_only:
        trusted_entries = [entry for entry in cleaned_entries if is_trusted_result(entry)]
        if not trusted_entries:
            return None
        chosen = trusted_entries[0]
        trusted_match = True
    else:
        chosen = cleaned_entries[0]
        trusted_match = is_trusted_result(chosen)

    return {
        "video_url": f"https://www.youtube.com/watch?v={chosen['id']}",
        "video_id": chosen.get("id"),
        "video_title": chosen.get("title"),
        "video_uploader": chosen.get("uploader"),
        "video_channel": chosen.get("channel"),
        "trusted_match": trusted_match,
    }


def fetch_transcript(video_id: str, preferred_language: str = "en") -> Dict[str, Any]:
    api = YouTubeTranscriptApi()
    transcript = api.fetch(video_id, languages=[preferred_language])

    transcript_text = transcript_items_to_text(transcript)
    language = preferred_language
    is_generated = None

    try:
        if hasattr(transcript, "language"):
            language = transcript.language
    except Exception:
        pass

    try:
        if hasattr(transcript, "is_generated"):
            is_generated = transcript.is_generated
    except Exception:
        pass

    return {
        "language": language,
        "is_generated": is_generated,
        "transcript_text": transcript_text,
    }


@app.get("/")
def root() -> Dict[str, Any]:
    return {
        "service": "YouTube Transcript Service",
        "status": "ok",
        "endpoints": ["/", "/health", "/get_youtube_transcript"],
    }


@app.get("/health")
def health() -> Dict[str, Any]:
    return {"status": "ok"}


@app.post("/get_youtube_transcript")
def get_youtube_transcript(payload: TranscriptRequest) -> Dict[str, Any]:
    video_url = payload.video_url
    query = payload.query
    preferred_language = payload.preferred_language
    trusted_only = payload.trusted_only
    max_search_results = payload.max_search_results

    selected_title = None
    selected_uploader = None
    selected_channel = None
    trusted_match = False

    if not video_url and query:
        search_result = search_youtube(
            query=query,
            trusted_only=trusted_only,
            max_search_results=max_search_results,
        )

        if not search_result:
            return {
                "success": False,
                "source_type": "YouTube search",
                "method_used": "yt-dlp search",
                "video_id": None,
                "video_url": None,
                "title": None,
                "uploader": None,
                "channel": None,
                "language": preferred_language,
                "is_generated": None,
                "trusted_match": False,
                "transcript_text": None,
                "error_message": (
                    "No trusted YouTube result found for query."
                    if trusted_only
                    else "No YouTube result found for query."
                ),
            }

        video_url = search_result["video_url"]
        selected_title = search_result.get("video_title")
        selected_uploader = search_result.get("video_uploader")
        selected_channel = search_result.get("video_channel")
        trusted_match = search_result.get("trusted_match", False)

    if not video_url:
        return {
            "success": False,
            "source_type": "YouTube transcript",
            "method_used": None,
            "video_id": None,
            "video_url": None,
            "title": None,
            "uploader": None,
            "channel": None,
            "language": preferred_language,
            "is_generated": None,
            "trusted_match": False,
            "transcript_text": None,
            "error_message": "No video_url or query provided.",
        }

    video_id = extract_video_id(video_url)
    if not video_id:
        return {
            "success": False,
            "source_type": "YouTube transcript",
            "method_used": None,
            "video_id": None,
            "video_url": video_url,
            "title": selected_title,
            "uploader": selected_uploader,
            "channel": selected_channel,
            "language": preferred_language,
            "is_generated": None,
            "trusted_match": trusted_match,
            "transcript_text": None,
            "error_message": "Could not extract video ID from URL.",
        }

    try:
        transcript_data = fetch_transcript(
            video_id=video_id,
            preferred_language=preferred_language,
        )

        return {
            "success": True,
            "source_type": "YouTube transcript",
            "method_used": "youtube-transcript-api",
            "video_id": video_id,
            "video_url": video_url,
            "title": selected_title,
            "uploader": selected_uploader,
            "channel": selected_channel,
            "language": transcript_data["language"],
            "is_generated": transcript_data["is_generated"],
            "trusted_match": trusted_match,
            "transcript_text": transcript_data["transcript_text"],
            "error_message": None,
        }

    except Exception as e:
        return {
            "success": False,
            "source_type": "YouTube transcript",
            "method_used": "youtube-transcript-api",
            "video_id": video_id,
            "video_url": video_url,
            "title": selected_title,
            "uploader": selected_uploader,
            "channel": selected_channel,
            "language": preferred_language,
            "is_generated": None,
            "trusted_match": trusted_match,
            "transcript_text": None,
            "error_message": str(e),
        }
