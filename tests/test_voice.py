import pytest

from standup.voice import banned_pattern_problems, check_draft, length_problems, sentence_count, word_count

REAL_SAMPLES = [
    "Sitting in the airport thinking about how you should never push directly to prod.",
    "It's for my portfolio babe, I swear. It's worth the $300 charge from Anthropic.",
    "Oh look, something I wrote.",
]


@pytest.mark.parametrize("text", REAL_SAMPLES)
def test_real_samples_pass(text):
    assert check_draft(text) == []


@pytest.mark.parametrize(
    "text, expected",
    [
        ("Here's the thing: outages happen.", "here's the thing"),
        ("Let that sink in.", "let that sink in"),
        ("Unpopular opinion: staging exists.", "unpopular opinion"),
        ("Never push to prod on a Friday. Agree?", "agree?"),
        ("This is a real game-changer for airports.", "game-changer"),
        ("This will revolutionize downtime.", "revolutioniz"),
        ("My 6-year-old taught me about rollbacks.", "taught me"),
        ("The takeaway is to test your upgrades.", "takeaway"),
        ("Excited to announce I am stuck at the airport.", "excited to announce"),
    ],
)
def test_banned_phrases_are_caught(text, expected):
    assert any(expected in problem for problem in banned_pattern_problems(text))


def test_curly_apostrophes_do_not_hide_banned_phrases():
    assert banned_pattern_problems("Here’s the thing about prod.")


def test_hashtags_are_caught_but_issue_numbers_are_not():
    assert "hashtag" in banned_pattern_problems("Never push to prod. #devops")
    assert "hashtag" not in banned_pattern_problems("Closing issue #4521 as working as intended.")
    assert "hashtag" not in banned_pattern_problems("Still writing C# on a Friday.")


def test_em_dash_and_line_breaks_are_caught():
    assert "em dash" in banned_pattern_problems("The upgrade went fine — for the vendor.")
    assert "line breaks (broetry)" in banned_pattern_problems("I got stuck.\nAt the airport.")


def test_emoji_wall_is_caught_but_one_emoji_is_allowed():
    assert "more than one emoji" in banned_pattern_problems("Shipping it 🚀🔥")
    assert "more than one emoji" not in banned_pattern_problems("Shipping it 🚀")


def test_all_lowercase_is_caught():
    assert "no capitalization" in banned_pattern_problems("never push directly to prod.")


def test_sentence_count():
    assert sentence_count("One. Two.") == 2
    assert sentence_count("No terminator") == 1
    assert sentence_count("It cost $3.50, e.g. a coffee.") == 1
    assert sentence_count('He said "ship it."') == 1
    assert sentence_count("Really?! Yes.") == 2
    assert sentence_count("") == 0


def test_length_limits():
    assert length_problems("Short and fine.") == []
    assert any("too many sentences" in p for p in length_problems("One. Two. Three."))
    long_text = " ".join(["word"] * 36) + "."
    assert word_count(long_text) == 36
    assert any("too long" in p for p in length_problems(long_text))
    assert length_problems(" ".join(["word"] * 35) + ".") == []


def test_empty_draft_fails():
    assert check_draft("   ") == ["empty"]
