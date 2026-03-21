from __future__ import annotations

import json
import re
import urllib.parse
import urllib.request
from typing import Dict, List, Optional


TRUSTED_SOURCES: Dict[str, List[str]] = {
    "prophecy_core": [
        "renald showers",
        "john f. walvoord",
        "john walvoord",
        "leon wood",
        "john c. whitcomb",
        "john whitcomb",
        "charles l. feinberg",
        "charles feinberg",
        "robert l. thomas",
        "robert thomas",
        "j. dwight pentecost",
        "dwight pentecost",
        "renald e. showers",
        "charles h. dyer",
        "charles dyer",
        "mark hitchcock",
        "john macarthur",
    ],
    "hermeneutics": [
        "walter c. kaiser jr",
        "walter c. kaiser",
        "walter kaiser",
        "roy b. zuck",
        "roy zuck",
        "grant r. osborne",
        "grant osborne",
        "elliot e. johnson",
        "elliot johnson",
    ],
    "biblical_languages": [
        "d. a. carson",
        "da carson",
        "don carson",
        "william d. mounce",
        "william mounce",
        "peter j. gentry",
        "peter gentry",
        "bruce k. waltke",
        "bruce waltke",
        "gleason l. archer jr",
        "gleason archer",
    ],
    "evangelical_core": [
        "martin luther",
        "john wesley",
        "smith wigglesworth",
        "w. a. criswell",
        "wa criswell",
        "e. stanley jones",
        "g. campbell morgan",
        "campbell morgan",
        "millard erickson",
        "charles spurgeon",
        "charles haddon spurgeon",
        "charles finney",
        "d. l. moody",
        "dl moody",
        "charles stanley",
        "chuck swindoll",
        "charles swindoll",
        "j. sidlow baxter",
        "sidlow baxter",
        "r. a. torrey",
        "ra torrey",
        "h. a. ironside",
        "ha ironside",
        "alan redpath",
        "warren wiersbe",
        "erwin lutzer",
        "philip miller",
        "jonathan edwards",
        "james hastings",
        "s. d. gordon",
        "jc ryle",
        "j. c. ryle",
        "i. howard marshall",
        "howard marshall",
        "gordon d. fee",
        "gordon fee",
        "alfred edersheim",
        "derek kidner",
        "allen p. ross",
        "allen ross",
        "erich zenger",
        "abraham ibn ezra",
        "ibn ezra",
        "john stott",
        "james montgomery boice",
        "boice",
        "douglas j. moo",
        "douglas moo",
        "walter c. kaiser jr",
        "walter kaiser",
        "meredith g. kline",
        "meredith kline",
        "christopher j. h. wright",
        "christopher wright",
        "gleason l. archer",
        "gleason archer",
        "michael rydelnik",
        "mitch glaser",
        "karen h. jobes",
        "karen jobes",
        "mary a. kassian",
        "mary kassian",
        "rosalie de rosset",
        "edith m. humphrey",
        "edith humphrey",
        "catherine booth",
        "lynn h. cohick",
        "lynn cohick",
        "elisabeth elliot",
        "r.c. sproul",
        "rc sproul",
        "john f. walvoord",
        "leon wood",
        "james montgomery boice",
        "john macarthur",
    ],
    "rabbinic_reference": [
        "rabbi jonathan sacks",
        "jonathan sacks",
        "rabbi abraham joshua heschel",
        "abraham joshua heschel",
        "abraham heschel",
        "rabbi joseph soloveitchik",
        "joseph soloveitchik",
        "rabbi samson raphael hirsch",
        "samson raphael hirsch",
        "samson hirsch",
        "rabbi dr. david katz",
        "rabbi david katz",
        "david katz",
        "rashi",
        "maimonides",
        "nahmanides",
        "saadia gaon",
        "david kimchi",
        "radak",
    ],
    "jewish_scholars_context": [
        "jon d. levenson",
        "jon levenson",
        "yehezkel kaufmann",
        "moshe weinfeld",
        "jacob milgrom",
    ],
    "archaeology_ane": [
        "kenneth a. kitchen",
        "kenneth kitchen",
        "james k. hoffmeier",
        "james hoffmeier",
        "richard s. hess",
        "richard hess",
        "bryant g. wood",
        "bryant wood",
        "randall price",
        "eilat mazar",
        "william f. albright",
        "william albright",
        "k. lawson younger jr",
        "lawson younger",
    ],
    "manuscript_textual": [
        "f. f. bruce",
        "ff bruce",
        "daniel b. wallace",
        "daniel wallace",
        "peter j. gentry",
        "philip wesley comfort",
        "philip comfort",
        "craig a. evans",
        "craig evans",
        "emanuel tov",
        "larry w. hurtado",
        "larry hurtado",
        "bruce m. metzger",
        "bruce metzger",
    ],
    "historical_color": [
        "william barclay",
        "alfred edersheim",
    ],
    "psalms_trusted": [
        "augustine",
        "jerome",
        "chrysostom",
        "ambrose",
        "theodoret",
        "basil",
        "eusebius",
        "origen",
        "john calvin",
        "martin luther",
        "franz delitzsch",
        "e. w. hengstenberg",
        "j. j. stewart perowne",
        "joseph addison alexander",
        "samuel horsley",
    ],
    "accessible_commentaries": [
        "bible speaks today",
        "tyndale commentaries",
        "mentor commentaries",
        "matthew henry",
        "speaker's commentary",
        "f. c. cook",
    ],
    "illustration_shelf": [
        "preaching today",
        "ct pastors",
        "encyclopedia of 7700 illustrations",
        "dictionary of 1000 illustrations",
    ],
    "salvation_army": [
        "catherine booth",
        "william booth",
        "the salvation army",
        "salvation army",
    ],
    "approved_channels": [
        "grace to you",
        "midnight call",
        "the friends of israel",
        "foi ministries",
        "moody church",
        "ligonier ministries",
        "desiring god",
        "gty",
    ],
}


def get_all_trusted_terms() -> List[str]:
    terms: List[str] = []
    for _, items in TRUSTED_SOURCES.items():
        terms.extend(items)

    cleaned: List[str] = []
    seen = set()

    for item in terms:
        key = item.strip().lower()
        if key and key not in seen:
            seen.add(key)
            cleaned.append(key)

    return cleaned


ALL_TRUSTED_TERMS = get_all_trusted_terms()


def get_approved_channels() -> List[str]:
    return [x.strip().lower() for x in TRUSTED_SOURCES.get("approved_channels", []) if x.strip()]


APPROVED_CHANNELS = get_approved_channels()


def normalize(text: str) -> str:
    return re.sub(r"\s+", " ", text.strip()).lower()


def extract_video_id(url: str) -> Optional[str]:
    match = re.search(r"(?:v=|youtu\.be/)([A-Za-z0-9_-]{11})", url)
    return match.group(1) if match else None


def _get_text(url: str, timeout: int = 12) -> str:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            )
        },
    )
    with urllib.request.urlopen(req, timeout=timeout) as response:
        return response.read().decode("utf-8", errors="ignore")


def _get_json(url: str, timeout: int = 12):
    return json.loads(_get_text(url, timeout=timeout))


def _fetch_oembed_metadata(video_url: str) -> Dict[str, str]:
    endpoint = (
        "https://www.youtube.com/oembed?url="
        + urllib.parse.quote_plus(video_url)
        + "&format=json"
    )

    try:
        data = _get_json(endpoint, timeout=12)
    except Exception:
        return {"title": "", "channel": "", "author": ""}

    if not isinstance(data, dict):
        return {"title": "", "channel": "", "author": ""}

    title = str(data.get("title", "")).strip()
    author_name = str(data.get("author_name", "")).strip()

    return {
        "title": title,
        "channel": author_name,
        "author": author_name,
    }


def search_youtube_candidates(query: str, max_results: int = 8) -> List[Dict]:
    search_url = "https://www.youtube.com/results?search_query=" + urllib.parse.quote_plus(query)

    try:
        html = _get_text(search_url, timeout=12)
    except Exception:
        return []

    ids = re.findall(r'"videoId":"([A-Za-z0-9_-]{11})"', html)

    seen = set()
    results: List[Dict] = []

    for vid in ids:
        if vid in seen:
            continue
        seen.add(vid)

        video_url = f"https://www.youtube.com/watch?v={vid}"
        meta = _fetch_oembed_metadata(video_url)

        results.append(
            {
                "title": meta.get("title", ""),
                "video_url": video_url,
                "channel": meta.get("channel", ""),
                "author": meta.get("author", ""),
                "source_kind": "video_search",
            }
        )

        if len(results) >= max_results:
            break

    return results


def fetch_transcript(video_url: str) -> str:
    vid = extract_video_id(video_url)
    if not vid:
        return ""

    endpoint = f"https://youtubetranscript.com/?server_vid2={vid}"

    try:
        data = _get_json(endpoint, timeout=12)
    except Exception:
        return ""

    if not isinstance(data, list):
        return ""

    parts: List[str] = []
    for item in data:
        if isinstance(item, dict):
            txt = str(item.get("text", "")).strip()
            if txt:
                parts.append(txt)

    return " ".join(parts).strip()


def title_has_bad_patterns(title: str) -> bool:
    t = normalize(title)
    bad_patterns = [
        r"\baudiobook\b",
        r"\bdramati[sz]ed\b",
        r"\bread[- ]?through\b",
        r"\bscripture reading\b",
        r"\bbible reading\b",
        r"\bkjv audio\b",
        r"\bniv audio\b",
        r"\bnlt audio\b",
        r"\bchapter only\b",
        r"\bcomplete audio\b",
        r"\bword for word\b",
        r"\bdownload\b",
        r"\bpdf\b",
        r"\bebook\b",
        r"\be-book\b",
        r"\bscan\b",
        r"\bfree book\b",
        r"\bcommentary pdf\b",
        r"\bdownload now\b",
        r"\blink in description\b",
    ]
    return any(re.search(p, t) for p in bad_patterns)


def is_trusted(candidate: Dict) -> bool:
    title = normalize(str(candidate.get("title", "")))
    channel = normalize(str(candidate.get("channel", "")))
    author = normalize(str(candidate.get("author", "")))
    combined = f"{title} {channel} {author}".strip()

    for term in ALL_TRUSTED_TERMS:
        if term in combined:
            return True

    for approved in APPROVED_CHANNELS:
        if approved in combined:
            return True

    return False


def score(candidate: Dict) -> int:
    title = normalize(str(candidate.get("title", "")))
    channel = normalize(str(candidate.get("channel", "")))
    author = normalize(str(candidate.get("author", "")))
    combined = f"{title} {channel} {author}".strip()

    total = 0

    for term in ALL_TRUSTED_TERMS:
        if term in title:
            total += 30
        if term in channel:
            total += 25
        if term in author:
            total += 25

    for approved in APPROVED_CHANNELS:
        if approved in combined:
            total += 20

    if re.search(r"\bsermon\b|\bteaching\b|\bstudy\b|\bmessage\b|\blecture\b|\bexposition\b", combined):
        total += 8

    if title_has_bad_patterns(title):
        total -= 120

    return total


def filter_and_rank(candidates: List[Dict]) -> List[Dict]:
    deduped: List[Dict] = []
    seen_urls = set()

    for candidate in candidates:
        url = str(candidate.get("video_url", "")).strip().lower()
        if url and url not in seen_urls:
            seen_urls.add(url)
            deduped.append(candidate)

    ranked: List[Dict] = []

    for candidate in deduped:
        if not is_trusted(candidate):
            continue

        candidate["score"] = score(candidate)

        if int(candidate.get("score", 0)) < 0:
            continue

        ranked.append(candidate)

    ranked.sort(key=lambda x: int(x.get("score", 0)), reverse=True)
    return ranked
