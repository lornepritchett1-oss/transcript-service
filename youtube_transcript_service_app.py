from fastapi import FastAPI
from pydantic import BaseModel
from youtube_transcript_api import YouTubeTranscriptApi
import re
import os
from datetime import datetime

app = FastAPI()

OUTPUT_DIR = "transcripts"
os.makedirs(OUTPUT_DIR, exist_ok=True)


class TranscriptRequest(BaseModel):
    query: str


BOOK_NAMES = [
    "Genesis", "Exodus", "Leviticus", "Numbers", "Deuteronomy",
    "Joshua", "Judges", "Ruth", "1 Samuel", "2 Samuel", "1 Kings", "2 Kings",
    "1 Chronicles", "2 Chronicles", "Ezra", "Nehemiah", "Esther", "Job",
    "Psalms", "Psalm", "Proverbs", "Ecclesiastes", "Song of Solomon", "Song of Songs",
    "Isaiah", "Jeremiah", "Lamentations", "Ezekiel", "Daniel", "Hosea", "Joel",
    "Amos", "Obadiah", "Jonah", "Micah", "Nahum", "Habakkuk", "Zephaniah",
    "Haggai", "Zechariah", "Malachi", "Matthew", "Mark", "Luke", "John", "Acts",
    "Romans", "1 Corinthians", "2 Corinthians", "Galatians", "Ephesians",
    "Philippians", "Colossians", "1 Thessalonians", "2 Thessalonians",
    "1 Timothy", "2 Timothy", "Titus", "Philemon", "Hebrews", "James",
    "1 Peter", "2 Peter", "1 John", "2 John", "3 John", "Jude", "Revelation"
]

SECTION_BREAK_WORDS = [
    "first", "second", "third", "finally", "now", "notice", "listen",
    "look at this", "let me say", "another thing", "on the other hand",
    "the point is", "here is the point", "in closing", "to conclude",
    "application", "illustration", "for example", "here's why"
]

STOPWORDS = {
    "the", "and", "that", "with", "have", "this", "from", "your", "they",
    "will", "their", "about", "there", "would", "could", "should", "what",
    "when", "where", "which", "into", "because", "then", "than", "just",
    "them", "were", "been", "being", "also", "some", "more", "very", "much",
    "said", "says", "unto", "you", "for", "are", "not", "but", "our", "his",
    "her", "had", "has", "was", "who", "how", "why", "can", "all", "out",
    "history", "earth", "planet", "world", "thing", "things", "really",
    "people", "time", "times", "something", "someone", "many", "make",
    "made", "does", "did", "doing", "here", "therefore", "again", "only",
    "over", "under", "through", "before", "after", "same", "present",
    "future", "end", "beginning", "began", "take", "place", "fact"
}

THEME_PATTERNS = [
    {
        "name": "The Sovereignty of God",
        "patterns": ["sovereign", "sovereignty", "omnipotent", "lord of the universe"],
        "priority": 4
    },
    {
        "name": "The Purpose of History",
        "patterns": ["purpose of history", "ultimate purpose", "world history"],
        "priority": 4
    },
    {
        "name": "The Kingdom of God",
        "patterns": ["kingdom", "theocratic", "rule over the earth", "kingdom rule"],
        "priority": 5
    },
    {
        "name": "The Defeat of Satan",
        "patterns": ["satan", "enemy", "crush satan", "gets rid of satan", "crush your head"],
        "priority": 4
    },
    {
        "name": "The Reign of Christ",
        "patterns": ["thousand year reign", "reign of jesus christ", "millennial kingdom", "christ will reign"],
        "priority": 5
    },
    {
        "name": "The Fall of Man",
        "patterns": ["adam", "fall", "lost through the fall", "fall of man", "man's sin"],
        "priority": 5
    },
    {
        "name": "The Glory of God",
        "patterns": ["glorify himself", "glory of god", "god to glorify himself"],
        "priority": 4
    },
    {
        "name": "The Promise of the Redeemer",
        "patterns": ["promise of the redeemer", "seed of the woman", "genesis chapter 3", "genesis 3", "first promise"],
        "priority": 7
    },
    {
        "name": "The Curse and Restoration of Creation",
        "patterns": ["creation groans", "travails in pain", "curse", "nature restored", "restored back", "whole of creation"],
        "priority": 7
    },
    {
        "name": "Judgment and Accountability",
        "patterns": ["judgment", "judge", "account", "wrath"],
        "priority": 5
    },
    {
        "name": "Salvation by Grace",
        "patterns": ["grace", "saved", "salvation", "redeemed"],
        "priority": 5
    },
    {
        "name": "Faith and Obedience",
        "patterns": ["obedience", "faith", "trust", "submit"],
        "priority": 4
    },
    {
        "name": "The Authority of Scripture",
        "patterns": ["scripture", "word of god", "bible says"],
        "priority": 4
    },
    {
        "name": "The Gospel of Christ",
        "patterns": ["cross", "resurrection", "gospel", "jesus christ"],
        "priority": 5
    },
    {
        "name": "Prayer and Dependence",
        "patterns": ["prayer", "pray", "calling on god"],
        "priority": 3
    },
    {
        "name": "Holiness and Separation",
        "patterns": ["holy", "holiness", "sanctification", "separation"],
        "priority": 3
    },
    {
        "name": "The Work of the Spirit",
        "patterns": ["holy spirit", "spirit of god", "spirit"],
        "priority": 3
    },
]

SPECIAL_SUBTHEMES = [
    {
        "label": "Satan's Strategy Against the Redeemer",
        "patterns": ["prevent that redeemer", "try to prevent", "coming redeemer", "promised redeemer from coming", "seed of the woman"],
        "base_theme": "The Defeat of Satan"
    },
    {
        "label": "The Preservation of the Messianic Line",
        "patterns": ["abel", "seth", "genealogy", "descendant of seth", "god's substitute for abel"],
        "base_theme": "The Defeat of Satan"
    },
    {
        "label": "Persecution and Apostasy",
        "patterns": ["persecute", "massacre", "apostasy", "falling away", "drifting away", "go apostate"],
        "base_theme": "The Defeat of Satan"
    },
    {
        "label": "Cain and Abel",
        "patterns": ["cain and abel", "cain", "abel", "murdered his brother", "first murder"],
        "base_theme": "The Defeat of Satan"
    },
    {
        "label": "The Days of Noah",
        "patterns": ["days of noah", "noah", "genesis chapter 6", "every imagination", "evil continually"],
        "base_theme": "The Fall of Man"
    },
    {
        "label": "The Promise in Genesis 3:15",
        "patterns": ["genesis chapter 3", "genesis 3", "verse 15", "seed of the woman", "crush your head"],
        "base_theme": "The Promise of the Redeemer"
    },
    {
        "label": "Creation Under the Curse",
        "patterns": ["creation groans", "travails in pain", "curse of man's sin", "whole of creation"],
        "base_theme": "The Curse and Restoration of Creation"
    },
    {
        "label": "Creation Restored in Christ's Reign",
        "patterns": ["nature restored", "jesus is going to do that", "authority to do that", "restored back the way it was"],
        "base_theme": "The Curse and Restoration of Creation"
    },
    {
        "label": "The Kingdom Under Attack",
        "patterns": ["members of god's kingdom", "kingdom of god's dear son", "destroy those people", "transferred out of satan's domain"],
        "base_theme": "The Kingdom of God"
    },
    {
        "label": "The Counterattack of God",
        "patterns": ["how did god counteract", "god counteract", "god had to intervene", "god would keep raising up"],
        "base_theme": "The Sovereignty of God"
    }
]


def extract_video_id(url):
    match = re.search(r"(?:v=|youtu\.be/)([\w-]+)", url)
    return match.group(1) if match else None


def clean_text(text):
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def format_timestamp(seconds):
    total_seconds = int(seconds)
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    secs = total_seconds % 60

    if hours > 0:
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"
    return f"{minutes:02d}:{secs:02d}"


def extract_scripture_references(text):
    refs = set()

    for book in sorted(BOOK_NAMES, key=len, reverse=True):
        pattern = rf"\b{re.escape(book)}\s+\d+(?::\d+(?:[-–]\d+)?)?\b"
        matches = re.findall(pattern, text, flags=re.IGNORECASE)
        for match in matches:
            refs.add(match.strip())

    return sorted(refs)


def roman_numeral(num):
    mapping = [
        (1000, "M"), (900, "CM"), (500, "D"), (400, "CD"),
        (100, "C"), (90, "XC"), (50, "L"), (40, "XL"),
        (10, "X"), (9, "IX"), (5, "V"), (4, "IV"), (1, "I")
    ]
    result = ""
    for value, symbol in mapping:
        while num >= value:
            result += symbol
            num -= value
    return result


def detect_section_boundary(text):
    lowered = text.lower()
    for marker in SECTION_BREAK_WORDS:
        if marker in lowered:
            return True
    return False


def build_sections(transcript, target_words=220, max_words=320):
    sections = []
    current_items = []
    current_words = 0
    section_start = 0.0

    for item in transcript:
        text = clean_text(item.get("text", ""))
        if not text:
            continue

        start_time = float(item.get("start", 0.0))

        if not current_items:
            section_start = start_time

        current_items.append({
            "text": text,
            "start": start_time
        })
        current_words += len(text.split())

        boundary = detect_section_boundary(text)

        should_close = False
        if current_words >= max_words:
            should_close = True
        elif current_words >= target_words and boundary:
            should_close = True

        if should_close:
            section_text = " ".join(x["text"] for x in current_items).strip()
            section_end = current_items[-1]["start"]
            sections.append({
                "start": section_start,
                "end": section_end,
                "text": section_text
            })
            current_items = []
            current_words = 0

    if current_items:
        section_text = " ".join(x["text"] for x in current_items).strip()
        section_end = current_items[-1]["start"]
        sections.append({
            "start": section_start,
            "end": section_end,
            "text": section_text
        })

    return sections


def extract_top_keywords(text, limit=8):
    words = re.findall(r"\b[a-zA-Z][a-zA-Z'-]{3,}\b", text.lower())
    counts = {}

    for word in words:
        if word in STOPWORDS:
            continue
        counts[word] = counts.get(word, 0) + 1

    ranked = sorted(counts.items(), key=lambda x: (-x[1], x[0]))
    return [word for word, _ in ranked[:limit]]


def score_theme(section_text, theme):
    lowered = section_text.lower()
    score = 0

    for pattern in theme["patterns"]:
        occurrences = lowered.count(pattern)
        if occurrences > 0:
            score += occurrences * 3

    words = re.findall(r"\b[a-zA-Z][a-zA-Z'-]{2,}\b", lowered)
    word_set = set(words)

    for pattern in theme["patterns"]:
        for token in pattern.split():
            cleaned = token.strip().lower()
            if len(cleaned) > 3 and cleaned in word_set:
                score += 1

    score += theme["priority"]
    return score


def rank_themes(section_text):
    scored = []
    for theme in THEME_PATTERNS:
        score = score_theme(section_text, theme)
        if score > theme["priority"]:
            scored.append((theme["name"], score))

    scored.sort(key=lambda x: (-x[1], x[0]))
    return scored


def detect_theme(section_text):
    ranked = rank_themes(section_text)
    return ranked[0][0] if ranked else None


def detect_special_subtheme(section_text, preferred_base_theme=None):
    lowered = section_text.lower()
    matches = []

    for item in SPECIAL_SUBTHEMES:
        if preferred_base_theme and item["base_theme"] != preferred_base_theme:
            continue

        score = 0
        for pattern in item["patterns"]:
            occurrences = lowered.count(pattern)
            if occurrences > 0:
                score += occurrences * 4

        if score > 0:
            matches.append((item["label"], score, item["base_theme"]))

    matches.sort(key=lambda x: (-x[1], x[0]))
    return matches[0] if matches else None


def choose_heading_label(section_text, recent_labels=None):
    recent_labels = recent_labels or []
    refs = extract_scripture_references(section_text)
    ranked_themes = rank_themes(section_text)

    top_theme = ranked_themes[0][0] if ranked_themes else None

    preferred_subtheme = detect_special_subtheme(section_text, preferred_base_theme=top_theme)
    any_subtheme = detect_special_subtheme(section_text)

    if preferred_subtheme:
        label = preferred_subtheme[0]
        if label not in recent_labels:
            return label

    if ranked_themes:
        for theme_name, _score in ranked_themes:
            if theme_name not in recent_labels:
                competing_subtheme = detect_special_subtheme(section_text, preferred_base_theme=theme_name)
                if competing_subtheme and competing_subtheme[0] not in recent_labels:
                    return competing_subtheme[0]
                return theme_name

        if any_subtheme:
            return any_subtheme[0]

        if len(ranked_themes) >= 2 and ranked_themes[0][1] - ranked_themes[1][1] <= 2:
            first = ranked_themes[0][0]
            second = ranked_themes[1][0]
            if first != second:
                return f"{first} / {second}"

        return ranked_themes[0][0]

    if refs:
        return refs[0]

    keywords = extract_top_keywords(section_text, limit=4)
    if keywords:
        return " / ".join(word.title() for word in keywords[:3])

    return "Main Thought"


def build_section_headings(sections):
    headings = []
    recent_labels = []

    for i, section in enumerate(sections, start=1):
        label = choose_heading_label(section["text"], recent_labels=recent_labels[-3:])
        headings.append(f"Section {i}: {label}")
        recent_labels.append(label)

    return headings


def build_preaching_outline(sections, headings):
    outline = []

    for i, (section, heading) in enumerate(zip(sections[:7], headings[:7]), start=1):
        heading_core = heading.replace(f"Section {i}: ", "").strip()
        refs = extract_scripture_references(section["text"])

        if refs and refs[0].lower() not in heading_core.lower():
            if len(heading_core) < 55:
                heading_core = f"{heading_core} ({refs[0]})"

        outline.append(f"{roman_numeral(i)}. {heading_core}")

    return outline


def extract_key_quotes_from_sections(sections, limit=5):
    quotes = []

    for section in sections:
        text = section["text"].strip()
        if 18 <= len(text.split()) <= 70:
            quotes.append(f"[{format_timestamp(section['start'])}] {text}")
        if len(quotes) >= limit:
            break

    return quotes


def generate_title_suggestions(full_text, scripture_refs, headings):
    titles = []
    detected_labels = []

    for heading in headings[:12]:
        label = heading.split(": ", 1)[1] if ": " in heading else heading
        if label not in detected_labels:
            detected_labels.append(label)

    if scripture_refs:
        titles.append(f"The Message of {scripture_refs[0]}")
        titles.append(f"Understanding {scripture_refs[0]}")

    if detected_labels:
        titles.append(detected_labels[0])
        titles.append(f"God's Word on {detected_labels[0]}")

    if len(detected_labels) >= 2:
        titles.append(f"{detected_labels[0]} and {detected_labels[1]}")

    if len(detected_labels) >= 3:
        titles.append(f"From {detected_labels[0]} to {detected_labels[2]}")

    keywords = extract_top_keywords(full_text, limit=5)
    if keywords:
        keyword_phrase = ", ".join(word.title() for word in keywords[:3])
        titles.append(f"Truth for the Heart: {keyword_phrase}")

    titles.append("A Call to Biblical Clarity")
    titles.append("When God Speaks, We Must Listen")

    seen = set()
    unique_titles = []
    for title in titles:
        if title not in seen:
            unique_titles.append(title)
            seen.add(title)

    return unique_titles[:6]


def write_section_file(path, sections, headings):
    with open(path, "w", encoding="utf-8") as f:
        f.write("TIMESTAMPED TEACHING SECTIONS\n\n")
        for section, heading in zip(sections, headings):
            start_str = format_timestamp(section["start"])
            end_str = format_timestamp(section["end"])
            f.write(f"{heading}\n")
            f.write(f"[{start_str} - {end_str}]\n\n")
            f.write(section["text"] + "\n\n")
            f.write("=" * 80 + "\n\n")


def write_titles_file(path, titles):
    with open(path, "w", encoding="utf-8") as f:
        f.write("SERMON TITLE SUGGESTIONS\n\n")
        for i, title in enumerate(titles, start=1):
            f.write(f"{i}. {title}\n")


def write_scripture_file(path, refs):
    with open(path, "w", encoding="utf-8") as f:
        f.write("SCRIPTURE REFERENCES DETECTED\n\n")
        if refs:
            for ref in refs:
                f.write(ref + "\n")
        else:
            f.write("No explicit Scripture references detected.\n")


@app.post("/get_youtube_transcript")
def get_transcript(request: TranscriptRequest):
    video_id = extract_video_id(request.query)

    if not video_id:
        return {"error": "Invalid YouTube URL"}

    fetched_transcript = YouTubeTranscriptApi().fetch(video_id)
    transcript = fetched_transcript.to_raw_data()

    raw_segments = [item.get("text", "") for item in transcript]
    clean_segments = [clean_text(t) for t in raw_segments if clean_text(t)]
    full_text = " ".join(clean_segments)

    sections = build_sections(transcript)
    headings = build_section_headings(sections)
    paragraphs = [section["text"] for section in sections]
    scripture_refs = extract_scripture_references(full_text)
    title_suggestions = generate_title_suggestions(full_text, scripture_refs, headings)
    preaching_outline = build_preaching_outline(sections, headings)
    key_quotes = extract_key_quotes_from_sections(sections, limit=5)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_filename = f"{video_id}_{timestamp}"

    raw_path = os.path.join(OUTPUT_DIR, f"{base_filename}_raw.txt")
    clean_path = os.path.join(OUTPUT_DIR, f"{base_filename}_clean.txt")
    paragraph_path = os.path.join(OUTPUT_DIR, f"{base_filename}_paragraphs.txt")
    section_path = os.path.join(OUTPUT_DIR, f"{base_filename}_sections.txt")
    outline_path = os.path.join(OUTPUT_DIR, f"{base_filename}_outline.txt")
    quotes_path = os.path.join(OUTPUT_DIR, f"{base_filename}_quotes.txt")
    titles_path = os.path.join(OUTPUT_DIR, f"{base_filename}_titles.txt")
    scripture_path = os.path.join(OUTPUT_DIR, f"{base_filename}_scriptures.txt")

    with open(raw_path, "w", encoding="utf-8") as f:
        f.write("\n".join(raw_segments))

    with open(clean_path, "w", encoding="utf-8") as f:
        f.write(full_text)

    with open(paragraph_path, "w", encoding="utf-8") as f:
        for p in paragraphs:
            f.write(p + "\n\n")

    with open(outline_path, "w", encoding="utf-8") as f:
        f.write("PREACHING OUTLINE\n\n")
        for line in preaching_outline:
            f.write(line + "\n")

    with open(quotes_path, "w", encoding="utf-8") as f:
        f.write("KEY QUOTES\n\n")
        for q in key_quotes:
            f.write(q + "\n\n")

    write_section_file(section_path, sections, headings)
    write_titles_file(titles_path, title_suggestions)
    write_scripture_file(scripture_path, scripture_refs)

    return {
        "status": "success",
        "video_id": video_id,
        "language": getattr(fetched_transcript, "language", None),
        "language_code": getattr(fetched_transcript, "language_code", None),
        "is_generated": getattr(fetched_transcript, "is_generated", None),
        "segment_count": len(transcript),
        "word_count": len(full_text.split()),
        "section_count": len(sections),
        "scripture_reference_count": len(scripture_refs),
        "files": {
            "raw": raw_path,
            "clean": clean_path,
            "paragraphs": paragraph_path,
            "sections": section_path,
            "outline": outline_path,
            "quotes": quotes_path,
            "titles": titles_path,
            "scriptures": scripture_path
        }
    }
