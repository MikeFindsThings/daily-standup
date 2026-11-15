from standup.models import Story
from standup.news import clean_text, dedupe, interleave, matches_topic, normalize_title, normalize_url


def story(title, url, source="X", summary=""):
    return Story(title=title, url=url, source=source, summary=summary)


def test_normalize_url_ignores_scheme_www_query_and_trailing_slash():
    assert normalize_url("https://www.example.com/a/b/?utm_source=x#top") == "example.com/a/b"
    assert normalize_url("http://example.com/a/b") == "example.com/a/b"


def test_normalize_title_ignores_case_and_punctuation():
    assert normalize_title("OpenAI's GPT: “It’s  fine”!") == normalize_title("openai s gpt it s fine")


def test_dedupe_drops_same_link_from_another_source():
    stories = [
        story("Big outage hits cloud", "https://example.com/outage?ref=hn", "Hacker News"),
        story("Cloud provider goes down", "https://www.example.com/outage/", "The Verge"),
    ]
    assert [s.source for s in dedupe(stories)] == ["Hacker News"]


def test_dedupe_drops_same_headline_with_different_link():
    stories = [
        story("Big Outage Hits Cloud!", "https://a.example/1"),
        story("big outage hits cloud", "https://b.example/2"),
        story("Something else", "https://c.example/3"),
    ]
    assert [s.url for s in dedupe(stories)] == ["https://a.example/1", "https://c.example/3"]


def test_dedupe_keeps_order_and_skips_blank_titles():
    stories = [story("B", "https://x/b"), story("  ", "https://x/blank"), story("A", "https://x/a")]
    assert [s.title for s in dedupe(stories)] == ["B", "A"]


def test_matches_topic_needs_every_word():
    s = story("AWS outage takes down half the internet", "https://x/1", summary="A DNS issue in us-east-1")
    assert matches_topic(s, "aws outage")
    assert matches_topic(s, "DNS")
    assert not matches_topic(s, "aws azure")
    assert matches_topic(s, None) and matches_topic(s, "  ")


def test_interleave_round_robins_sources():
    a = [story("a1", "u1"), story("a2", "u2"), story("a3", "u3")]
    b = [story("b1", "u4")]
    assert [s.title for s in interleave([a, b])] == ["a1", "b1", "a2", "a3"]
    assert interleave([]) == []


def test_clean_text_strips_html_and_trims():
    assert clean_text("<p>Hello &amp; <b>goodbye</b></p>") == "Hello & goodbye"
    assert len(clean_text("word " * 200, limit=50)) == 50
