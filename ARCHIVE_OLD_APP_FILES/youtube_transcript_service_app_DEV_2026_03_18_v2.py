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
    return (value or "").strip().lower()


def is_trusted_result(entry: Dict[str, Any]) -> bool:
    title = normalize_text(entry.get("title"))
    uploader = normalize_text(entry.get("uploader"))
    channel = normalize_text(entry.get("channel"))
    description = normalize_text(entry.get("description"))

    haystack = f"{title} {uploader} {channel} {description}"
    return any(term in haystack for term in ALL_TRUSTED_TERMS)


def build_queries(query: str) -> List[str]:
    base = query.strip()
    bookish = base.split(":")[0].strip()

    queries = [
        base,
        f"{base} commentary",
        f"{base} bible study",
        f"{base} explained",
        f"{bookish} commentary",
        f"{bookish} background",
    ]

    if "daniel" in base.lower():
        queries.extend([
            "Daniel 1 commentary",
            "Daniel 1 bible study",
            "Daniel 1 historical background",
            "Daniel 1 Babylon exile",
            "Jehoiakim Nebuchadnezzar Daniel",
            "Daniel first deportation",
            "Daniel chapter 1 Walvoord",
            "Daniel chapter 1 Leon Wood",
            "Daniel chapter 1 Renald Showers",
        ])

    deduped: List[str] = []
    seen = set()
    for q in queries:
        if q not in seen:
            deduped.append(q)
            seen.add(q)

    return deduped


def score_entry(entry: Dict[str, Any], original_query: str) -> int:
    score = 0
    haystack = " ".join([
        normalize_text(entry.get("title")),
        normalize_text(entry.get("uploader")),
        normalize_text(entry.get("channel")),
        normalize_text(entry.get("description")),
    ])

    oq = normalize_text(original_query)

    for token in ["daniel", "1", "commentary", "bible", "study", "jehoiakim", "nebuchadnezzar", "exile"]:
        if token in haystack:
            score += 1

    if oq and oq in haystack:
        score += 3

    if is_trusted_result(entry):
        score += 10

    return score


def search_youtube_candidates(
    query: str,
    max_search_results: int = 10,
) -> List[Dict[str, Any]]:
    safe_max = max(5, min(max_search_results, 25))

    ydl_opts = {
        "quiet": True,
        "skip_download": True,
        "extract_flat": True,
        "noplaylist": True,
    }

    with YoutubeDL(ydl_opts) as ydl:
        results = ydl.extract_info(f"ytsearch{safe_max}:{query}", download=False)

    entries = (results or {}).get("entries") or []
    return [e for e in entries if e and e.get("id")]


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
        "service": "Trusted Bible Source Search",
        "status": "ok",
        "endpoints": ["/", "/health", "/get_youtube_transcript"],
    }


@app.get("/health")
def health() -> Dict[str, Any]:
    return {"status": "ok"}


@app.post("/get_youtube_transcript")
def get_youtube_transcript(payload: TranscriptRequest) -> Dict[str, Any]:
    preferred_language = payload.preferred_language
    trusted_only = payload.trusted_only
    max_search_results = payload.max_search_results

    debug_queries: List[str] = []
    debug_candidates: List[Dict[str, Any]] = []

    selected_entry: Optional[Dict[str, Any]] = None
    selected_query: Optional[str] = None

    # Direct URL mode
    if payload.video_url:
        video_id = extract_video_id(payload.video_url)
        if not video_id:
            return {
                "success": False,
                "source_type": "YouTube transcript",
                "method_used": "direct-url",
                "video_id": None,
                "video_url": payload.video_url,
                "title": None,
                "uploader": None,
                "channel": None,
                "language": preferred_language,
                "is_generated": None,
                "trusted_match": False,
                "transcript_text": None,
                "error_message": "Could not extract video ID from URL.",
                "debug_queries": [],
                "debug_selected_query": None,
                "debug_candidates": [],
            }

        try:
            transcript_data = fetch_transcript(video_id, preferred_language)
            return {
                "success": True,
                "source_type": "YouTube transcript",
                "method_used": "direct-url",
                "video_id": video_id,
                "video_url": payload.video_url,
                "title": None,
                "uploader": None,
                "channel": None,
                "language": transcript_data["language"],
                "is_generated": transcript_data["is_generated"],
                "trusted_match": False,
                "transcript_text": transcript_data["transcript_text"][:5000],
                "error_message": None,
                "debug_queries": [],
                "debug_selected_query": None,
                "debug_candidates": [],
            }
        except Exception as e:
            return {
                "success": False,
                "source_type": "YouTube transcript",
                "method_used": "direct-url",
                "video_id": video_id,
                "video_url": payload.video_url,
                "title": None,
                "uploader": None,
                "channel": None,
                "language": preferred_language,
                "is_generated": None,
                "trusted_match": False,
                "transcript_text": None,
                "error_message": str(e),
                "debug_queries": [],
                "debug_selected_query": None,
                "debug_candidates": [],
            }

    # Query mode
    if not payload.query:
        return {
            "success": False,
            "source_type": "YouTube search",
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
            "debug_queries": [],
            "debug_selected_query": None,
            "debug_candidates": [],
        }

    queries = build_queries(payload.query)

    best_non_trusted: Optional[Dict[str, Any]] = None
    best_non_trusted_query: Optional[str] = None

    for q in queries:
        debug_queries.append(q)
        candidates = search_youtube_candidates(q, max_search_results=max_search_results)

        if not candidates:
            continue

        sorted_candidates = sorted(
            candidates,
            key=lambda e: score_entry(e, payload.query or ""),
            reverse=True,
        )

        for e in sorted_candidates[:3]:
            debug_candidates.append({
                "query": q,
                "title": e.get("title"),
                "uploader": e.get("uploader"),
                "channel": e.get("channel"),
                "video_id": e.get("id"),
                "trusted": is_trusted_result(e),
            })

        trusted_candidates = [e for e in sorted_candidates if is_trusted_result(e)]

        if trusted_candidates:
            selected_entry = trusted_candidates[0]
            selected_query = q
            break

        if sorted_candidates and best_non_trusted is None:
            best_non_trusted = sorted_candidates[0]
            best_non_trusted_query = q

    if not selected_entry and not trusted_only and best_non_trusted:
        selected_entry = best_non_trusted
        selected_query = best_non_trusted_query

    if not selected_entry:
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
                else "No usable YouTube result found for query."
            ),
            "debug_queries": debug_queries,
            "debug_selected_query": None,
            "debug_candidates": debug_candidates[:12],
        }

    video_id = selected_entry.get("id")
    video_url = f"https://www.youtube.com/watch?v={video_id}"

    try:
        transcript_data = fetch_transcript(video_id, preferred_language)
        return {
            "success": True,
            "source_type": "YouTube transcript",
            "method_used": "youtube-transcript-api",
            "video_id": video_id,
            "video_url": video_url,
            "title": selected_entry.get("title"),
            "uploader": selected_entry.get("uploader"),
            "channel": selected_entry.get("channel"),
            "language": transcript_data["language"],
            "is_generated": transcript_data["is_generated"],
            "trusted_match": is_trusted_result(selected_entry),
            "transcript_text": transcript_data["transcript_text"][:5000],
            "error_message": None,
            "debug_queries": debug_queries,
            "debug_selected_query": selected_query,
            "debug_candidates": debug_candidates[:12],
        }
    except Exception as e:
        return {
            "success": False,
            "source_type": "YouTube transcript",
            "method_used": "youtube-transcript-api",
            "video_id": video_id,
            "video_url": video_url,
            "title": selected_entry.get("title"),
            "uploader": selected_entry.get("uploader"),
            "channel": selected_entry.get("channel"),
            "language": preferred_language,
            "is_generated": None,
            "trusted_match": is_trusted_result(selected_entry),
            "transcript_text": None,
            "error_message": str(e),
            "debug_queries": debug_queries,
            "debug_selected_query": selected_query,
            "debug_candidates": debug_candidates[:12],
        }
