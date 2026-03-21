from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

from fastapi import FastAPI
from pydantic import BaseModel

from trusted_sources import (
    ALL_TRUSTED_TERMS,
    TRUSTED_SOURCES,
    fetch_transcript,
    filter_and_rank,
    search_youtube_candidates,
)

APP_VERSION = "2026-03-20-main-app-stable-v5"

app = FastAPI(title="Bible GPT Transcript Service", version=APP_VERSION)


class QueryRequest(BaseModel):
    query: str


BIBLE_BOOKS = [
    "Genesis", "Exodus", "Leviticus", "Numbers", "Deuteronomy",
    "Joshua", "Judges", "Ruth", "1 Samuel", "2 Samuel", "1 Kings", "2 Kings",
    "1 Chronicles", "2 Chronicles", "Ezra", "Nehemiah", "Esther", "Job", "Psalms",
    "Proverbs", "Ecclesiastes", "Song of Solomon", "Isaiah", "Jeremiah",
    "Lamentations", "Ezekiel", "Daniel", "Hosea", "Joel", "Amos", "Obadiah",
    "Jonah", "Micah", "Nahum", "Habakkuk", "Zephaniah", "Haggai", "Zechariah",
    "Malachi", "Matthew", "Mark", "Luke", "John", "Acts", "Romans",
    "1 Corinthians", "2 Corinthians", "Galatians", "Ephesians", "Philippians",
    "Colossians", "1 Thessalonians", "2 Thessalonians", "1 Timothy", "2 Timothy",
    "Titus", "Philemon", "Hebrews", "James", "1 Peter", "2 Peter", "1 John",
    "2 John", "3 John", "Jude", "Revelation",
]

KNOWN_TOPICS = [
    "justification",
    "sanctification",
    "holiness",
    "second coming",
    "rapture",
    "tribulation",
    "kingdom of god",
    "atonement",
    "resurrection",
    "inspiration of scripture",
]

BOOK_AUTHOR_MAP: Dict[str, List[str]] = {
    "daniel": [
        "renald showers",
        "john walvoord",
        "leon wood",
        "charles feinberg",
        "john macarthur",
    ],
    "revelation": [
        "john walvoord",
        "j. dwight pentecost",
        "robert thomas",
        "john macarthur",
    ],
    "romans": [
        "john macarthur",
        "james montgomery boice",
        "douglas moo",
        "john stott",
    ],
    "psalms": [
        "derek kidner",
        "allen ross",
        "martin luther",
        "john calvin",
    ],
    "obadiah": [
        "john macarthur",
        "leon wood",
        "charles feinberg",
    ],
}


def normalize(text: str) -> str:
    return re.sub(r"\s+", " ", text.strip())


def normalize_lower(text: str) -> str:
    return normalize(text).lower()


def detect_book(query: str) -> Optional[str]:
    q = normalize_lower(query)
    for book in sorted(BIBLE_BOOKS, key=len, reverse=True):
        if re.search(rf"\b{re.escape(book.lower())}\b", q):
            return book
    return None


def detect_topic(query: str) -> Optional[str]:
    q = normalize_lower(query)
    for topic in sorted(KNOWN_TOPICS, key=len, reverse=True):
        if topic in q:
            return topic
    return None


def detect_author(query: str) -> Optional[str]:
    q = normalize_lower(query)
    for term in ALL_TRUSTED_TERMS:
        if term in q:
            return term
    return None


def extract_reference_parts(query: str) -> Dict[str, Optional[str]]:
    q = normalize_lower(query)

    chapter = None
    verse = None

    verse_match = re.search(r"\b\d{1,3}:(\d{1,3})\b", q)
    if verse_match:
        verse = verse_match.group(1)

    chapter_match = re.search(r"\b([1-3]?\s*[a-z]+(?:\s+[a-z]+)?)\s+(\d{1,3})(?::\d{1,3})?\b", q)
    if chapter_match:
        chapter = str(int(chapter_match.group(2)))

    return {
        "chapter": chapter,
        "verse": verse,
    }


def detect_mode(query: str) -> Dict[str, Optional[str]]:
    author = detect_author(query)
    book = detect_book(query)
    topic = detect_topic(query)
    ref = extract_reference_parts(query)

    if author:
        return {
            "routing_mode": "author",
            "book_detected": book,
            "topic_detected": topic,
            "author_requested": author,
            "chapter_detected": ref["chapter"],
            "verse_detected": ref["verse"],
        }

    if book:
        return {
            "routing_mode": "book",
            "book_detected": book,
            "topic_detected": topic,
            "author_requested": None,
            "chapter_detected": ref["chapter"],
            "verse_detected": ref["verse"],
        }

    if topic:
        return {
            "routing_mode": "topic",
            "book_detected": None,
            "topic_detected": topic,
            "author_requested": None,
            "chapter_detected": ref["chapter"],
            "verse_detected": ref["verse"],
        }

    return {
        "routing_mode": "fallback",
        "book_detected": None,
        "topic_detected": None,
        "author_requested": None,
        "chapter_detected": ref["chapter"],
        "verse_detected": ref["verse"],
    }


def preferred_authors_for_route(route: Dict[str, Optional[str]]) -> List[str]:
    if route["routing_mode"] == "author" and route["author_requested"]:
        return [str(route["author_requested"]).lower()]

    if route["routing_mode"] == "book" and route["book_detected"]:
        book_key = str(route["book_detected"]).lower()
        if book_key in BOOK_AUTHOR_MAP:
            return BOOK_AUTHOR_MAP[book_key]
        return TRUSTED_SOURCES.get("prophecy_core", [])[:5]

    if route["routing_mode"] == "topic" and route["topic_detected"]:
        topic = str(route["topic_detected"]).lower()
        if topic in {"second coming", "rapture", "tribulation"}:
            return TRUSTED_SOURCES.get("prophecy_core", [])[:6]
        return TRUSTED_SOURCES.get("evangelical_core", [])[:6]

    return TRUSTED_SOURCES.get("prophecy_core", [])[:5]


def build_search_queries(query: str, route: Dict[str, Optional[str]], preferred_authors: List[str]) -> List[str]:
    q = normalize(query)
    searches: List[str] = []

    if route["routing_mode"] == "author" and route["author_requested"]:
        author = str(route["author_requested"])
        searches.append(q)
        searches.append(f"{author} {q}")

    elif route["routing_mode"] == "book" and route["book_detected"]:
        book = str(route["book_detected"])
        chapter = route.get("chapter_detected")

        searches.append(q)

        for author in preferred_authors[:5]:
            searches.append(f"{book} {author}")
            searches.append(f"{book} study {author}")
            if chapter:
                searches.append(f"{book} {chapter} {author}")
                searches.append(f"{book} chapter {chapter} {author}")

        searches.append(f"{book} Bible study")
        searches.append(f"{book} sermon")
        if chapter:
            searches.append(f"{book} {chapter}")
            searches.append(f"{book} chapter {chapter}")

    elif route["routing_mode"] == "topic" and route["topic_detected"]:
        topic = str(route["topic_detected"])
        searches.append(q)

        for author in preferred_authors[:5]:
            searches.append(f"{topic} {author}")
            searches.append(f"{topic} teaching {author}")

        searches.append(f"{topic} Bible study")
        searches.append(f"{topic} sermon")

    else:
        searches.append(q)

    deduped: List[str] = []
    seen = set()
    for item in searches:
        key = item.lower().strip()
        if key and key not in seen:
            seen.add(key)
            deduped.append(item)

    return deduped


def extract_candidate_chapters(title: str, book: Optional[str]) -> List[str]:
    if not title:
        return []

    t = normalize_lower(title)
    found: List[str] = []
    seen = set()

    if book:
        book_l = str(book).lower()
        patterns = [
            rf"\b{re.escape(book_l)}\s+0*(\d{{1,3}})\b",
            rf"\b{re.escape(book_l)}\s*\(\s*0*(\d{{1,3}})\s*\)",
            rf"\b{re.escape(book_l)}\s+chapter\s+0*(\d{{1,3}})\b",
            rf"\bchapter\s+0*(\d{{1,3}})\b",
        ]

        for pattern in patterns:
            for match in re.finditer(pattern, t):
                chapter = str(int(match.group(1)))
                if chapter not in seen:
                    seen.add(chapter)
                    found.append(chapter)

    return found


def route_relevance_score(candidate: Dict[str, Any], route: Dict[str, Optional[str]]) -> int:
    title = normalize_lower(str(candidate.get("title", "")))
    channel = normalize_lower(str(candidate.get("channel", "")))
    author = normalize_lower(str(candidate.get("author", "")))
    combined = f"{title} {channel} {author}".strip()

    score = 0

    book = route.get("book_detected")
    topic = route.get("topic_detected")
    requested_author = route.get("author_requested")
    chapter = route.get("chapter_detected")
    verse = route.get("verse_detected")

    if requested_author and str(requested_author).lower() in combined:
        score += 40

    if book:
        book_l = str(book).lower()

        # For book queries, missing the requested book in the title is a major problem.
        if book_l in title:
            score += 70
        elif book_l in combined:
            score += 10
        else:
            score -= 140

    if topic:
        topic_l = str(topic).lower()
        if topic_l in title:
            score += 35
        elif topic_l in combined:
            score += 15

    if chapter and book:
        requested_chapter = str(int(str(chapter)))
        candidate_chapters = extract_candidate_chapters(title, book)

        if candidate_chapters:
            if requested_chapter in candidate_chapters:
                score += 80
            else:
                score -= 110

        if re.search(rf"\bchapter\s+0*{re.escape(requested_chapter)}\b", combined):
            score += 25

        if re.search(rf"\b{re.escape(str(book).lower())}\s+0*{re.escape(requested_chapter)}\b", title):
            score += 40

    if verse:
        v = str(verse)
        if re.search(rf"\b{re.escape(v)}\b", title):
            score += 5

    if route.get("routing_mode") == "book":
        if "sermon" in combined or "study" in combined or "teaching" in combined or "lecture" in combined:
            score += 8

    return score


def rerank_for_route(ranked: List[Dict[str, Any]], route: Dict[str, Optional[str]]) -> List[Dict[str, Any]]:
    rescored: List[Dict[str, Any]] = []

    for candidate in ranked:
        copy_candidate = dict(candidate)
        base_score = int(copy_candidate.get("score", 0))
        route_bonus = route_relevance_score(copy_candidate, route)
        final_score = base_score + route_bonus

        # Hard cleanup for book queries: candidates with no book in title should not survive near the top.
        book = route.get("book_detected")
        title = normalize_lower(str(copy_candidate.get("title", "")))
        if book and str(book).lower() not in title:
            final_score -= 80

        copy_candidate["base_score"] = base_score
        copy_candidate["route_bonus"] = route_bonus
        copy_candidate["final_score"] = final_score
        rescored.append(copy_candidate)

    rescored.sort(key=lambda x: int(x.get("final_score", 0)), reverse=True)
    return rescored


def build_failure(
    query: str,
    route: Dict[str, Optional[str]],
    preferred_authors: List[str],
    search_queries: List[str],
    raw_candidate_count: int,
    ranked_candidate_count: int,
    reason: str,
) -> Dict[str, Any]:
    return {
        "success": False,
        "query": query,
        "routing_mode": route["routing_mode"],
        "book_detected": route["book_detected"],
        "topic_detected": route["topic_detected"],
        "author_requested": route["author_requested"],
        "chapter_detected": route["chapter_detected"],
        "verse_detected": route["verse_detected"],
        "preferred_authors_used": preferred_authors,
        "source_name": None,
        "title": None,
        "video_url": None,
        "source_kind": None,
        "excerpt": "",
        "confidence_score": 0,
        "reason": reason,
        "app_version": APP_VERSION,
        "debug": {
            "query_used": search_queries,
            "raw_candidate_count": raw_candidate_count,
            "ranked_candidate_count": ranked_candidate_count,
            "top_candidates": [],
        },
    }


@app.get("/health")
def health() -> Dict[str, Any]:
    return {
        "ok": True,
        "app_version": APP_VERSION,
        "trusted_terms_count": len(ALL_TRUSTED_TERMS),
    }


@app.post("/get_youtube_transcript")
def get_youtube_transcript(req: QueryRequest) -> Dict[str, Any]:
    query = normalize(req.query)

    if not query:
        return build_failure(
            query="",
            route={
                "routing_mode": "fallback",
                "book_detected": None,
                "topic_detected": None,
                "author_requested": None,
                "chapter_detected": None,
                "verse_detected": None,
            },
            preferred_authors=[],
            search_queries=[],
            raw_candidate_count=0,
            ranked_candidate_count=0,
            reason="Empty query.",
        )

    route = detect_mode(query)
    preferred_authors = preferred_authors_for_route(route)
    search_queries = build_search_queries(query, route, preferred_authors)

    raw_candidates: List[Dict[str, Any]] = []
    for sq in search_queries:
        try:
            raw_candidates.extend(search_youtube_candidates(sq, max_results=8))
        except Exception:
            continue

    trusted_ranked = filter_and_rank(raw_candidates)

    if not trusted_ranked:
        return build_failure(
            query=query,
            route=route,
            preferred_authors=preferred_authors,
            search_queries=search_queries,
            raw_candidate_count=len(raw_candidates),
            ranked_candidate_count=0,
            reason="No trusted candidates found.",
        )

    final_ranked = rerank_for_route(trusted_ranked, route)
    winner = final_ranked[0]
    transcript = fetch_transcript(str(winner.get("video_url", "")))

    top_candidates = []
    for c in final_ranked[:5]:
        top_candidates.append({
            "title": c.get("title", ""),
            "video_url": c.get("video_url", ""),
            "channel": c.get("channel", ""),
            "author": c.get("author", ""),
            "base_score": c.get("base_score", c.get("score", 0)),
            "route_bonus": c.get("route_bonus", 0),
            "final_score": c.get("final_score", c.get("score", 0)),
            "source_kind": c.get("source_kind", ""),
        })

    return {
        "success": True,
        "query": query,
        "routing_mode": route["routing_mode"],
        "book_detected": route["book_detected"],
        "topic_detected": route["topic_detected"],
        "author_requested": route["author_requested"],
        "chapter_detected": route["chapter_detected"],
        "verse_detected": route["verse_detected"],
        "preferred_authors_used": preferred_authors,
        "source_name": winner.get("author") or winner.get("channel") or "",
        "title": winner.get("title", ""),
        "video_url": winner.get("video_url", ""),
        "source_kind": "video_transcript" if transcript else winner.get("source_kind", "video_search"),
        "excerpt": transcript[:800] if transcript else "",
        "confidence_score": winner.get("final_score", winner.get("score", 0)),
        "reason": "Top ranked trusted candidate selected after stricter book-aware rerank.",
        "app_version": APP_VERSION,
        "debug": {
            "query_used": search_queries,
            "raw_candidate_count": len(raw_candidates),
            "ranked_candidate_count": len(final_ranked),
            "top_candidates": top_candidates,
        },
    }
