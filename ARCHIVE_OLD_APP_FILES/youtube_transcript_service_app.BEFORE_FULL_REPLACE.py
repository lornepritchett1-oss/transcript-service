from typing import Any, Dict, List, Optional

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
    haystack = " ".join(
        [
            normalize_text(entry.get("title")),
            normalize_text(entry.get("uploader")),
            normalize_text(entry.get("channel")),
            normalize_text(entry.get("description")),
        ]
    )
    return any(term in haystack for term in ALL_TRUSTED_TERMS)


def build_queries(query: str) -> List[str]:
    base = query.strip()
    base_l = normalize_text(base)

    # For a verse like "Daniel 1:1-3", extract "Daniel 1"
    chapter_stub = base
    if ":" in base:
        chapter_stub = base.split(":")[0].strip()

    queries: List[str] = [
        base,
        f"{base} commentary",
        f"{base} bible study",
        f"{chapter_stub} commentary",
        f"{chapter_stub} bible study",
    ]

    # Daniel-specific enrichment
    if "daniel" in base_l:
        queries.extend(
            [
                "Daniel 1 commentary",
                "Daniel 1 bible study",
                "Daniel 1 historical background",
                "Daniel 1 first deportation",
                "Jehoiakim Nebuchadnezzar Daniel 1",
                "Daniel chapter 1 John MacArthur",
                "Daniel chapter 1 Walvoord",
                "Daniel chapter 1 Leon Wood",
                "Daniel chapter 1 Renald Showers",
            ]
        )

    # Deduplicate while preserving order
    seen = set()
    out: List[str] = []
    for q in queries:
        q_norm = q.strip()
        if q_norm and q_norm not in seen:
            out.append(q_norm)
            seen.add(q_norm)

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
    haystack = " ".join(
        [
            normalize_text(entry.get("title")),
            normalize_text(entry.get("uploader")),
            normalize_text(entry.get("channel")),
            normalize_text(entry.get("description")),
        ]
    )

    oq = normalize_text(original_query)
    score = 0

    # Strongly reward correct passage/chapter relevance first
    if "daniel 1:1" in haystack or "daniel 1:1-3" in haystack:
        score += 40
    if "daniel 1" in haystack:
        score += 25
    if "chapter 1" in haystack:
        score += 10

    # Penalize obviously wrong Daniel chapters
    if "daniel 9" in haystack:
        score -= 25
    if "chapter 9" in haystack:
        score -= 20
    if "daniel 8" in haystack:
        score -= 15
    if "daniel 11" in haystack:
        score -= 15

    # Reward relevant topic words
    for token in ["jehoiakim", "nebuchadnezzar", "babylon", "exile", "deportation"]:
        if token in haystack:
            score += 5

    # Reward original query token overlap
    for token in oq.replace(":", " ").replace("-", " ").split():
        if token and token in haystack:
            score += 2

    # Trust matters, but passage relevance outranks trust
    if is_trusted_result(entry):
        score += 10

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
        return text[:1400] if text else None
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

    best_trusted_entry: Optional[Dict[str, Any]] = None
    best_trusted_query: Optional[str] = None
    best_trusted_score: Optional[int] = None

    best_any_entry: Optional[Dict[str, Any]] = None
    best_any_query: Optional[str] = None
    best_any_score: Optional[int] = None

    debug_candidates: List[Dict[str, Any]] = []

    for q in queries:
        candidates = search_youtube_candidates(q, payload.max_search_results)
        if not candidates:
            continue

        ranked = sorted(candidates, key=lambda e: score_entry(e, payload.query), reverse=True)

        for e in ranked[:3]:
            entry_score = score_entry(e, payload.query)
            debug_candidates.append(
                {
                    "query": q,
                    "title": e.get("title"),
                    "uploader": e.get("uploader"),
                    "channel": e.get("channel"),
                    "video_id": e.get("id"),
                    "trusted": is_trusted_result(e),
                    "score": entry_score,
                }
            )

        top = ranked[0]
        top_score = score_entry(top, payload.query)

        if best_any_entry is None or (best_any_score is not None and top_score > best_any_score) or best_any_score is None:
            best_any_entry = top
            best_any_query = q
            best_any_score = top_score

        trusted_ranked = [e for e in ranked if is_trusted_result(e)]
        if trusted_ranked:
            trusted_top = trusted_ranked[0]
            trusted_score = score_entry(trusted_top, payload.query)
            if (
                best_trusted_entry is None
                or best_trusted_score is None
                or trusted_score > best_trusted_score
            ):
                best_trusted_entry = trusted_top
                best_trusted_query = q
                best_trusted_score = trusted_score

    selected_entry: Optional[Dict[str, Any]] = None
    selected_query: Optional[str] = None

    if payload.trusted_only:
        selected_entry = best_trusted_entry
        selected_query = best_trusted_query
    else:
        if best_trusted_entry and best_trusted_score is not None and best_trusted_score >= (best_any_score or 0):
            selected_entry = best_trusted_entry
            selected_query = best_trusted_query
        else:
            selected_entry = best_any_entry
            selected_query = best_any_query

    if not selected_entry:
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
            "error_message": "No usable result found.",
            "debug_candidates": debug_candidates[:12],
        }

    video_id = selected_entry.get("id")
    video_url = f"https://www.youtube.com/watch?v={video_id}"
    excerpt = fetch_transcript_excerpt(video_id, payload.preferred_language)

    return {
        "success": True,
        "query": payload.query,
        "trusted_match": is_trusted_result(selected_entry),
        "source_kind": "video_transcript",
        "source_name": selected_entry.get("channel") or selected_entry.get("uploader"),
        "title": selected_entry.get("title"),
        "video_url": video_url,
        "query_used": selected_query,
        "excerpt": excerpt,
        "error_message": None,
        "debug_candidates": debug_candidates[:12],
    }
