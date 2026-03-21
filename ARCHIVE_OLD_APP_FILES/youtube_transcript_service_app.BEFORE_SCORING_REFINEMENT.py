from fastapi import FastAPI
from pydantic import BaseModel
from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path
import json
import re

from yt_dlp import YoutubeDL

try:
    from youtube_transcript_api import YouTubeTranscriptApi
except Exception:
    YouTubeTranscriptApi = None


app = FastAPI(title="YouTube Transcript Service", version="2.0.0")


# ============================================================
# CONFIG
# ============================================================

BASE_DIR = Path(__file__).resolve().parent
TRUSTED_AUTHORS_JSON = BASE_DIR / "trusted_authors.json"
TRUSTED_AUTHORS_TXT = BASE_DIR / "trusted_authors.txt"

FALLBACK_TRUSTED_AUTHORS = [
    "Renald Showers",
    "Leon Wood",
    "John Walvoord",
    "John Whitcomb",
    "Charles Feinberg",
    "Robert L. Thomas",
    "J. Dwight Pentecost",
    "David Jeremiah",
    "J. Vernon McGee",
    "John MacArthur",
    "Warren Wiersbe",
    "Chuck Swindoll",
    "D. A. Carson",
    "R. C. Sproul",
]

SEARCH_RESULTS_PER_QUERY = 4
MAX_QUERIES_TO_RUN = 18
MAX_CANDIDATES_TO_SCORE = 20
MAX_DEBUG_CANDIDATES = 12


# ============================================================
# MODELS
# ============================================================

class QueryRequest(BaseModel):
    query: str


# ============================================================
# TRUSTED AUTHORS LOADING
# ============================================================

def load_trusted_authors() -> List[str]:
    """
    Preferred:
      - trusted_authors.json
      - trusted_authors.txt

    JSON formats supported:
      1. {"trusted_authors": ["Name 1", "Name 2"]}
      2. [{"name": "Name 1"}, {"name": "Name 2"}]
      3. ["Name 1", "Name 2"]

    TXT format:
      one author per line
    """
    if TRUSTED_AUTHORS_JSON.exists():
        try:
            data = json.loads(TRUSTED_AUTHORS_JSON.read_text(encoding="utf-8"))

            if isinstance(data, dict) and isinstance(data.get("trusted_authors"), list):
                authors = [str(x).strip() for x in data["trusted_authors"] if str(x).strip()]
                if authors:
                    return dedupe_keep_order(authors)

            if isinstance(data, list):
                if all(isinstance(x, str) for x in data):
                    authors = [x.strip() for x in data if x.strip()]
                    if authors:
                        return dedupe_keep_order(authors)

                if all(isinstance(x, dict) for x in data):
                    authors = []
                    for item in data:
                        name = str(item.get("name", "")).strip()
                        if name:
                            authors.append(name)
                    if authors:
                        return dedupe_keep_order(authors)
        except Exception:
            pass

    if TRUSTED_AUTHORS_TXT.exists():
        try:
            authors = []
            for line in TRUSTED_AUTHORS_TXT.read_text(encoding="utf-8").splitlines():
                name = line.strip()
                if name and not name.startswith("#"):
                    authors.append(name)
            if authors:
                return dedupe_keep_order(authors)
        except Exception:
            pass

    return dedupe_keep_order(FALLBACK_TRUSTED_AUTHORS)


# ============================================================
# HELPERS
# ============================================================

def dedupe_keep_order(items: List[str]) -> List[str]:
    seen = set()
    out = []
    for item in items:
        key = item.casefold()
        if key not in seen:
            seen.add(key)
            out.append(item)
    return out


def normalize_spaces(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def safe_text(value: Any) -> str:
    return normalize_spaces(str(value or ""))


def simplify(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", text.casefold()).strip()


def looks_like_bible_reference(query: str) -> bool:
    q = query.strip()
    return bool(re.search(r"\b([1-3]\s*)?[A-Za-z]+\s+\d+(:\d+(-\d+)?)?\b", q))


def parse_bible_reference(query: str) -> Dict[str, Optional[str]]:
    """
    Very simple parser:
      Daniel 1:1-3
      1 John 3:16
      Romans 8
    """
    q = normalize_spaces(query)
    m = re.match(r"^((?:[1-3]\s+)?[A-Za-z]+)\s+(\d+)(?::(\d+)(?:-(\d+))?)?$", q)
    if not m:
        return {
            "book": None,
            "chapter": None,
            "verse_start": None,
            "verse_end": None,
            "normalized_ref": q,
        }

    book = m.group(1)
    chapter = m.group(2)
    verse_start = m.group(3)
    verse_end = m.group(4) or verse_start

    normalized_ref = f"{book} {chapter}"
    if verse_start:
        normalized_ref += f":{verse_start}"
        if verse_end and verse_end != verse_start:
            normalized_ref += f"-{verse_end}"

    return {
        "book": book,
        "chapter": chapter,
        "verse_start": verse_start,
        "verse_end": verse_end,
        "normalized_ref": normalized_ref,
    }


def make_search_queries(query: str, trusted_authors: List[str]) -> List[Tuple[str, str]]:
    """
    Returns a list of tuples:
      (author_name, search_query)
    """
    ref = parse_bible_reference(query)
    book = ref["book"]
    chapter = ref["chapter"]
    verse_start = ref["verse_start"]
    verse_end = ref["verse_end"]

    queries: List[Tuple[str, str]] = []

    # Search patterns
    for author in trusted_authors:
        if book and chapter and verse_start:
            queries.append((author, f'{book} {chapter}:{verse_start}-{verse_end} {author}'))
            queries.append((author, f'{book} {chapter}:{verse_start} {author}'))
            queries.append((author, f'{book} chapter {chapter} {author}'))
        elif book and chapter:
            queries.append((author, f'{book} {chapter} {author}'))
            queries.append((author, f'{book} chapter {chapter} {author}'))
        else:
            queries.append((author, f'{query} {author}'))
            queries.append((author, f'{query} commentary {author}'))

    # Add a couple of general trusted searches to widen the net
    if book and chapter:
        queries.append(("", f"{book} {chapter} commentary"))
        queries.append(("", f"{book} chapter {chapter} bible study"))
    else:
        queries.append(("", f"{query} commentary"))
        queries.append(("", f"{query} bible study"))

    # De-duplicate
    seen = set()
    final_queries = []
    for author, sq in queries:
        key = sq.casefold()
        if key not in seen:
            seen.add(key)
            final_queries.append((author, sq))

    return final_queries[:MAX_QUERIES_TO_RUN]


def search_youtube(search_query: str, max_results: int = SEARCH_RESULTS_PER_QUERY) -> List[Dict[str, Any]]:
    ydl_opts = {
        "quiet": True,
        "skip_download": True,
        "extract_flat": True,
        "noplaylist": True,
    }

    with YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(f"ytsearch{max_results}:{search_query}", download=False)

    entries = info.get("entries", []) if isinstance(info, dict) else []
    results = []

    for entry in entries:
        if not entry:
            continue
        results.append({
            "id": entry.get("id"),
            "title": safe_text(entry.get("title")),
            "uploader": safe_text(entry.get("uploader") or entry.get("channel")),
            "channel": safe_text(entry.get("channel") or entry.get("uploader")),
            "webpage_url": entry.get("url") or (f"https://www.youtube.com/watch?v={entry.get('id')}" if entry.get("id") else ""),
        })

    return results


def fetch_transcript_text(video_id: str) -> str:
    if not video_id or YouTubeTranscriptApi is None:
        return ""

    # Support both old and newer styles
    try:
        transcript_items = YouTubeTranscriptApi.get_transcript(video_id, languages=["en"])
        parts = [safe_text(item.get("text")) for item in transcript_items if item.get("text")]
        return normalize_spaces(" ".join(parts))
    except Exception:
        pass

    try:
        api = YouTubeTranscriptApi()
        fetched = api.fetch(video_id, languages=["en"])
        parts = []
        for item in fetched:
            text = ""
            if isinstance(item, dict):
                text = item.get("text", "")
            else:
                text = getattr(item, "text", "")
            if text:
                parts.append(safe_text(text))
        return normalize_spaces(" ".join(parts))
    except Exception:
        return ""


def build_excerpt(transcript_text: str, max_len: int = 1500) -> str:
    transcript_text = normalize_spaces(transcript_text)
    if len(transcript_text) <= max_len:
        return transcript_text
    return transcript_text[:max_len].rstrip() + "..."


def author_name_in_text(author: str, *texts: str) -> bool:
    if not author:
        return False
    author_simple = simplify(author)
    for text in texts:
        if author_simple and author_simple in simplify(text):
            return True
    return False


def score_candidate(
    *,
    raw_query: str,
    search_author: str,
    search_query: str,
    title: str,
    channel: str,
    uploader: str,
    transcript_text: str,
) -> Tuple[int, List[str]]:
    reasons: List[str] = []
    score = 0

    ref = parse_bible_reference(raw_query)
    book = ref["book"]
    chapter = ref["chapter"]
    verse_start = ref["verse_start"]
    verse_end = ref["verse_end"]

    title_s = simplify(title)
    channel_s = simplify(channel)
    uploader_s = simplify(uploader)
    transcript_s = simplify(transcript_text[:4000])  # limit scoring cost

    # Trusted author scoring
    if search_author:
        score += 25
        reasons.append("trusted-author-query")

        if author_name_in_text(search_author, title, channel, uploader):
            score += 15
            reasons.append("author-name-visible-in-result")

    # Book / chapter / verse scoring
    if book:
        book_s = simplify(book)
        if book_s in title_s or book_s in transcript_s:
            score += 20
            reasons.append("book-match")

    if book and chapter:
        chapter_patterns = [
            f"{simplify(book)} {chapter}",
            f"{simplify(book)} chapter {chapter}",
        ]
        if any(p in title_s for p in chapter_patterns):
            score += 30
            reasons.append("chapter-match-in-title")
        elif any(p in transcript_s for p in chapter_patterns):
            score += 20
            reasons.append("chapter-match-in-transcript")

    if book and chapter and verse_start:
        verse_patterns = [
            f"{simplify(book)} {chapter} {verse_start}",
            f"{simplify(book)} {chapter} {verse_start} {verse_end or verse_start}",
            f"{simplify(book)} {chapter}:{verse_start}",
            f"{simplify(book)} {chapter}:{verse_start}-{verse_end or verse_start}",
            f"{chapter}:{verse_start}",
            f"{chapter}:{verse_start}-{verse_end or verse_start}",
        ]
        if any(p in title_s for p in verse_patterns):
            score += 50
            reasons.append("exact-verse-match-in-title")
        elif any(p in transcript_s for p in verse_patterns):
            score += 35
            reasons.append("exact-verse-match-in-transcript")

    # Search-term relevance
    raw_terms = [t for t in simplify(raw_query).split() if len(t) > 1]
    title_hits = sum(1 for t in raw_terms if t in title_s)
    transcript_hits = sum(1 for t in raw_terms if t in transcript_s)

    if title_hits >= 2:
        score += 15
        reasons.append("title-relevance")

    if transcript_hits >= 2:
        score += 10
        reasons.append("transcript-relevance")

    # Transcript depth
    if len(transcript_text) > 300:
        score += 10
        reasons.append("substantive-transcript")

    # Penalty for overly vague title
    if book:
        book_s = simplify(book)
        if book_s not in title_s and book_s not in transcript_s:
            score -= 20
            reasons.append("vague-or-off-target")

    return score, reasons


def gather_candidates(query: str, trusted_authors: List[str]) -> List[Dict[str, Any]]:
    search_jobs = make_search_queries(query, trusted_authors)
    by_video_id: Dict[str, Dict[str, Any]] = {}

    for search_author, search_query in search_jobs:
        try:
            search_results = search_youtube(search_query)
        except Exception:
            continue

        for result in search_results:
            video_id = result.get("id")
            if not video_id:
                continue

            existing = by_video_id.get(video_id)
            if existing is None:
                by_video_id[video_id] = {
                    "video_id": video_id,
                    "title": result.get("title", ""),
                    "uploader": result.get("uploader", ""),
                    "channel": result.get("channel", ""),
                    "video_url": result.get("webpage_url", ""),
                    "search_authors": [search_author] if search_author else [],
                    "search_queries": [search_query],
                }
            else:
                if search_author and search_author not in existing["search_authors"]:
                    existing["search_authors"].append(search_author)
                if search_query not in existing["search_queries"]:
                    existing["search_queries"].append(search_query)

    candidates = list(by_video_id.values())
    return candidates[:MAX_CANDIDATES_TO_SCORE]


def enrich_and_score_candidates(query: str, candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    enriched: List[Dict[str, Any]] = []

    for candidate in candidates:
        transcript_text = fetch_transcript_text(candidate["video_id"])
        excerpt = build_excerpt(transcript_text)

        best_score = -999
        best_author = ""
        best_query = ""
        best_reasons: List[str] = []

        if candidate["search_authors"]:
            author_list = candidate["search_authors"]
        else:
            author_list = [""]

        for author in author_list:
            for search_query in candidate["search_queries"]:
                score, reasons = score_candidate(
                    raw_query=query,
                    search_author=author,
                    search_query=search_query,
                    title=candidate["title"],
                    channel=candidate["channel"],
                    uploader=candidate["uploader"],
                    transcript_text=transcript_text,
                )
                if score > best_score:
                    best_score = score
                    best_author = author
                    best_query = search_query
                    best_reasons = reasons

        enriched.append({
            "video_id": candidate["video_id"],
            "title": candidate["title"],
            "uploader": candidate["uploader"],
            "channel": candidate["channel"],
            "video_url": candidate["video_url"],
            "search_author": best_author,
            "query_used": best_query,
            "score": best_score,
            "score_reasons": best_reasons,
            "transcript_text": transcript_text,
            "excerpt": excerpt,
            "trusted": bool(best_author),
        })

    enriched.sort(key=lambda x: x["score"], reverse=True)
    return enriched


# ============================================================
# ENDPOINT
# ============================================================

@app.get("/")
def root() -> Dict[str, str]:
    return {"status": "ok", "service": "youtube-transcript-service"}


@app.post("/get_youtube_transcript")
def get_youtube_transcript(payload: QueryRequest) -> Dict[str, Any]:
    query = normalize_spaces(payload.query)

    if not query:
        return {
            "success": False,
            "query": "",
            "trusted_match": False,
            "source_kind": "",
            "source_name": "",
            "title": "",
            "video_url": "",
            "query_used": "",
            "excerpt": "",
            "error_message": "Query is required.",
            "debug_candidates": [],
            "selected_source": None,
            "other_candidates": [],
            "selection_reason": "",
        }

    trusted_authors = load_trusted_authors()
    candidates = gather_candidates(query, trusted_authors)
    scored = enrich_and_score_candidates(query, candidates)

    if not scored:
        return {
            "success": False,
            "query": query,
            "trusted_match": False,
            "source_kind": "",
            "source_name": "",
            "title": "",
            "video_url": "",
            "query_used": "",
            "excerpt": "",
            "error_message": "No matching YouTube candidates found.",
            "debug_candidates": [],
            "selected_source": None,
            "other_candidates": [],
            "selection_reason": "",
        }

    best = scored[0]

    debug_candidates = []
    for c in scored[:MAX_DEBUG_CANDIDATES]:
        debug_candidates.append({
            "query": c["query_used"],
            "title": c["title"],
            "uploader": c["uploader"],
            "channel": c["channel"],
            "video_id": c["video_id"],
            "trusted": c["trusted"],
            "score": c["score"],
            "author": c["search_author"],
            "reasons": c["score_reasons"],
        })

    other_candidates = []
    for c in scored[1:4]:
        other_candidates.append({
            "author": c["search_author"],
            "title": c["title"],
            "video_url": c["video_url"],
            "score": c["score"],
            "query_used": c["query_used"],
        })

    selection_reason = ", ".join(best["score_reasons"]) if best["score_reasons"] else "highest-ranked candidate"

    return {
        "success": True,
        "query": query,
        "trusted_match": best["trusted"],
        "source_kind": "video_transcript" if best["transcript_text"] else "video_search",
        "source_name": best["search_author"] or best["channel"] or best["uploader"],
        "title": best["title"],
        "video_url": best["video_url"],
        "query_used": best["query_used"],
        "excerpt": best["excerpt"],
        "error_message": None,
        "debug_candidates": debug_candidates,
        "selected_source": {
            "author": best["search_author"],
            "title": best["title"],
            "video_url": best["video_url"],
            "score": best["score"],
            "query_used": best["query_used"],
        },
        "other_candidates": other_candidates,
        "selection_reason": selection_reason,
    }
