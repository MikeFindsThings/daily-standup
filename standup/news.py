"""Fetch candidate headlines from free, bot-friendly sources, and dedupe them.

The fetchers do network I/O. Everything else here is a pure function so it can be tested.
"""

import html
import re
from urllib.parse import urlsplit, urlunsplit

import feedparser
import httpx

from standup.models import Story

USER_AGENT = "daily-standup/0.1"
TIMEOUT = httpx.Timeout(15.0)

RSS_FEEDS = {
    "The Verge": "https://www.theverge.com/rss/index.xml",
    "Ars Technica": "https://feeds.arstechnica.com/arstechnica/index",
    "TechCrunch": "https://techcrunch.com/feed/",
}
SOURCES = ["Hacker News", *RSS_FEEDS]

_TAG = re.compile(r"<[^>]+>")
_WS = re.compile(r"\s+")
_NON_WORD = re.compile(r"[^a-z0-9 ]+")


# --- pure helpers -----------------------------------------------------------


def clean_text(raw: str, limit: int = 300) -> str:
    """Strip HTML tags and entities, collapse whitespace, and trim to `limit` characters."""
    text = _WS.sub(" ", html.unescape(_TAG.sub(" ", raw or ""))).strip()
    return text if len(text) <= limit else text[: limit - 1].rstrip() + "…"


def normalize_url(url: str) -> str:
    """Canonical form for comparing links: no scheme, www, query string, fragment, or trailing slash."""
    parts = urlsplit((url or "").strip())
    host = parts.netloc.lower().removeprefix("www.")
    return urlunsplit(("", host, parts.path.rstrip("/"), "", "")).removeprefix("//")


def normalize_title(title: str) -> str:
    """Lowercase and drop punctuation, so the same headline from two feeds compares equal."""
    return _WS.sub(" ", _NON_WORD.sub(" ", (title or "").lower())).strip()


def dedupe(stories: list[Story]) -> list[Story]:
    """Drop stories that repeat an earlier one's link or headline. Keeps first-seen order."""
    seen_urls: set[str] = set()
    seen_titles: set[str] = set()
    unique: list[Story] = []
    for story in stories:
        url_key, title_key = normalize_url(story.url), normalize_title(story.title)
        if not title_key or url_key in seen_urls or title_key in seen_titles:
            continue
        seen_urls.add(url_key)
        seen_titles.add(title_key)
        unique.append(story)
    return unique


def matches_topic(story: Story, topic: str | None) -> bool:
    """True when every word of `topic` appears as a whole word in the story's title or summary."""
    if not topic or not topic.strip():
        return True
    words = set(normalize_title(f"{story.title} {story.summary}").split())
    return all(word in words for word in normalize_title(topic).split())


def interleave(groups: list[list[Story]]) -> list[Story]:
    """Round-robin across sources so no single feed dominates the candidate list."""
    merged: list[Story] = []
    for i in range(max((len(g) for g in groups), default=0)):
        merged.extend(g[i] for g in groups if i < len(g))
    return merged


# --- fetchers ---------------------------------------------------------------


def _client() -> httpx.Client:
    return httpx.Client(timeout=TIMEOUT, headers={"User-Agent": USER_AGENT}, follow_redirects=True)


def fetch_hacker_news(topic: str | None = None, limit: int = 30) -> list[Story]:
    """Top stories from the official HN API, or a topic search via the HN Search API."""
    with _client() as client:
        if topic and topic.strip():
            resp = client.get(
                "https://hn.algolia.com/api/v1/search",
                params={"query": topic, "tags": "story", "hitsPerPage": limit,
                        "numericFilters": "created_at_i>0"},
            )
            resp.raise_for_status()
            hits = resp.json().get("hits", [])
            return [
                Story(title=h["title"], source="Hacker News", points=h.get("points"),
                      url=h.get("url") or f"https://news.ycombinator.com/item?id={h['objectID']}")
                for h in hits if h.get("title")
            ]
        ids = client.get("https://hacker-news.firebaseio.com/v0/topstories.json")
        ids.raise_for_status()
        stories = []
        for item_id in ids.json()[:limit]:
            item = client.get(f"https://hacker-news.firebaseio.com/v0/item/{item_id}.json").json() or {}
            if item.get("type") == "story" and item.get("title"):
                stories.append(Story(
                    title=item["title"], source="Hacker News", points=item.get("score"),
                    url=item.get("url") or f"https://news.ycombinator.com/item?id={item_id}",
                ))
        return stories


def fetch_rss(source: str, limit: int = 20) -> list[Story]:
    with _client() as client:
        resp = client.get(RSS_FEEDS[source])
        resp.raise_for_status()
    feed = feedparser.parse(resp.content)
    if feed.bozo and not feed.entries:
        raise ValueError(f"{source} feed could not be parsed: {feed.bozo_exception}")
    return [
        Story(title=clean_text(e.get("title", ""), 200), url=e.get("link", ""), source=source,
              summary=clean_text(e.get("summary", "")))
        for e in feed.entries[:limit] if e.get("title") and e.get("link")
    ]


def fetch_source(source: str, topic: str | None = None) -> list[Story]:
    if source == "Hacker News":
        return fetch_hacker_news(topic)
    return [s for s in fetch_rss(source) if matches_topic(s, topic)]
