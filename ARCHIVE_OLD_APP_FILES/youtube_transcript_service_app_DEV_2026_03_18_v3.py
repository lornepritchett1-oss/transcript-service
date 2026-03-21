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
    query: str
    preferred_language: str = "en"
    trusted_only: bool = True
    max_search_results: int = 20


def normalize_text(value: Optional[str]) -> str:
    return (value or "").strip().lower()


def is_trusted_result(entry: Dict[str, Any]) -> bool:
    haystack = " ".join([
        normalize_text(entry.get("title")),
        normalize_text(entry.get("uploader")),
        normalize_text(entry.get("channel")),
        normalize_text(entry.get("description")),
    ])
    return any(term in haystack for term in ALL_TRUSTED_TERMS)


def build_queries(query: str) -> List[str]:
    base = query.strip()
    bookish = base.split(":")[0].strip()

    queries = [
        base,
        f"{base} commentary",
        f"{base} bible study",
        f"{bookish} commentary",
    ]

    if "daniel" in base.lower():
        queries.extend([
            "Daniel 1 commentary",
            "Daniel 1 bible study",
            "Daniel first deportation",
            "Jehoiakim Nebuchadnezzar Daniel",
            "Daniel chapter 1 MacArthur",
            "Daniel chapter 1 Walvoord",
            "Daniel chapter 1 Leon Wood",
        ])

    out: List[str] = []
    seen = set()
    for q in queries:
        if q not in seen:
            out.append(q)
            seen.add(q)
    return out


def search_youtube_candidates(query: str, max_search_results: int) -> List[Dict[str, Any]]:
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


def score_entry(entry: Dict[str, Any], original_query: str) -> int:
    haystack = " ".join([
        normalize_text(entry.get("title")),
        normalize_text(entry.get("uploader")),
        normalize_text(entry.get("channel")),
        normalize_text(entry.get("description")),
    ])

    score = 0
    if is_trusted_result(entry):
        score += 10

    for token in normalize_text(original_query).replace(":", " ").split():
        if token and token in haystack:
            score += 1

    for token in ["daniel", "commentary", "bible", "study", "jehoiakim", "nebuchadnezzar", "exile"]:
        if token in haystack:
            score += 1

    return score


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


def fetch_transcript_excerpt(video_id: str, preferred_language: str) -> Optional[str]:
    try:
        transcript = YouTubeTranscriptApi().fetch(video_id, languages=[preferred_language])
        text = transcript_items_to_text(transcript)
        return text[:1200] if text else None
    except Exception:
        return None


@app.get("/")
def root() -> Dict[str, str]:
    return {"service": "Trusted Bible Source Search", "status": "ok"}


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.post("/get_youtube_transcript")
def get_youtube_transcript(payload: TranscriptRequest) -> Dict[str, Any]:
    queries = build_queries(payload.query)

    best_entry: Optional[Dict[str, Any]] = None
    best_query: Optional[str] = None

    for q in queries:
        candidates = search_youtube_candidates(q, payload.max_search_results)
        if not candidates:
            continue

        ranked = sorted(candidates, key=lambda e: score_entry(e, payload.query), reverse=True)

        trusted = [e for e in ranked if is_trusted_result(e)]
        if trusted:
            best_entry = trusted[0]
            best_query = q
            break

        if not payload.trusted_only and ranked and best_entry is None:
            best_entry = ranked[0]
            best_query = q

    if not best_entry:
        return {
            "success": False,
            "query": payload.query,
            "trusted_match": False,
            "source_kind": "none",
            "source_name": None,
            "title": None,
            "video_url": None,
            "query_used": None,
            "excerpt": None,
            "error_message": "No usable result found."
        }

    video_id = best_entry.get("id")
    video_url = f"https://www.youtube.com/watch?v={video_id}"
    excerpt = fetch_transcript_excerpt(video_id, payload.preferred_language)

    return {
        "success": True,
        "query": payload.query,
        "trusted_match": is_trusted_result(best_entry),
        "source_kind": "video_transcript",
        "source_name": best_entry.get("channel") or best_entry.get("uploader"),
        "title": best_entry.get("title"),
        "video_url": video_url,
        "query_used": best_query,
        "excerpt": excerpt,
        "error_message": None
    }
