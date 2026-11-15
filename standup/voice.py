"""The voice rules that can be checked without a model.

`check_draft` is the deterministic half of the voice check. The other half is an LLM
judge (standup/llm.py) that catches what patterns can't: generic jokes, explained jokes,
fake-inspirational framing.
"""

import re
from pathlib import Path

VOICE_PATH = Path(__file__).resolve().parent.parent / "style" / "voice.md"

MAX_WORDS = 35
MAX_SENTENCES = 2

BANNED_PHRASES = [
    "here's the thing", "let that sink in", "this is huge", "unpopular opinion",
    "agree?", "thoughts?", "what do you think", "game-changer", "game changer",
    "revolutioniz", "revolutionis", "unlock", "delve", "landscape", "journey",
    "excited to announce", "thrilled to", "humbled", "taught me", "here's what i learned",
    "what i learned", "lesson", "takeaway", "what this means for", "hot take",
    "read that again", "full stop", "in today's", "ever-evolving", "the future of",
]

_EMOJI = re.compile(
    "[\U0001F000-\U0001FAFF\U00002600-\U000027BF\U0001F1E6-\U0001F1FF\U00002B00-\U00002BFF\U0000FE0F]"
)
_HASHTAG = re.compile(r"(?<!\w)#[A-Za-z]\w*")
_SENTENCE_END = re.compile(r"[.!?]+(?=\s|$)")
_ABBREV = re.compile(r"\b(?:e\.g|i\.e|vs|mr|mrs|ms|dr|inc|u\.s|a\.i)\.", re.I)


def load_voice() -> str:
    return VOICE_PATH.read_text(encoding="utf-8")


def word_count(text: str) -> int:
    return len(text.split())


def sentence_count(text: str) -> int:
    """Count sentences, ignoring common abbreviations, decimals, and trailing closing quotes."""
    stripped = _ABBREV.sub("", text).strip().rstrip("\"'”’)")
    stripped = re.sub(r"(?<=\d)\.(?=\d)", "", stripped)
    if not stripped:
        return 0
    ends = len(_SENTENCE_END.findall(stripped))
    return ends if stripped[-1] in ".!?" else ends + 1


def length_problems(text: str) -> list[str]:
    problems = []
    if word_count(text) > MAX_WORDS:
        problems.append(f"too long ({word_count(text)} words; limit {MAX_WORDS})")
    if sentence_count(text) > MAX_SENTENCES:
        problems.append(f"too many sentences ({sentence_count(text)}; limit {MAX_SENTENCES})")
    return problems


def banned_pattern_problems(text: str) -> list[str]:
    problems = []
    lowered = text.lower().replace("’", "'")
    for phrase in BANNED_PHRASES:
        if phrase in lowered:
            problems.append(f"banned phrase: {phrase!r}")
    if _HASHTAG.search(text):
        problems.append("hashtag")
    if "—" in text or " -- " in text:
        problems.append("em dash")
    if len(_EMOJI.findall(text)) > 1:
        problems.append("more than one emoji")
    if "\n" in text.strip():
        problems.append("line breaks (broetry)")
    if text.strip() and text.strip() == text.strip().lower() and re.search(r"[a-z]", text):
        problems.append("no capitalization")
    return problems


def check_draft(text: str) -> list[str]:
    """Every rule-based reason this draft fails the voice. An empty list means it passes."""
    if not text or not text.strip():
        return ["empty"]
    return length_problems(text) + banned_pattern_problems(text)
