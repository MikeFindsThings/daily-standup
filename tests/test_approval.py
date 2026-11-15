import pytest

from standup.approval import drafts_markdown, final_markdown, resolve_final
from standup.models import Approval, Draft
from standup.store import draft_to_row, row_to_draft

STORY = dict(story_title="Airline grounds flights after software upgrade", story_url="https://example.com/s")


def drafts():
    return [
        Draft(round=1, option=1, text="Repost text.", format="repost", **STORY),
        Draft(round=1, option=2, text="One-liner text.", format="one-liner", **STORY),
        Draft(round=1, option=3, text="GIF text.", format="gif", gif_query="cat keyboard",
              gif_urls=["https://giphy.com/a", "https://giphy.com/b"], **STORY),
    ]


def test_repost_includes_link_and_no_gif():
    final = resolve_final(drafts(), Approval(choice=1))
    assert (final.text, final.format, final.story_url, final.gif_url) == (
        "Repost text.", "repost", "https://example.com/s", None)


def test_one_liner_has_no_link():
    final = resolve_final(drafts(), Approval(choice=2))
    assert final.format == "one-liner" and final.story_url is None and final.gif_url is None


def test_gif_option_defaults_to_first_candidate():
    final = resolve_final(drafts(), Approval(choice=3))
    assert final.format == "gif" and final.gif_url == "https://giphy.com/a" and final.story_url is None


def test_gif_candidate_number_and_custom_url():
    assert resolve_final(drafts(), Approval(choice=3, gif="2")).gif_url == "https://giphy.com/b"
    custom = resolve_final(drafts(), Approval(choice=1, gif="https://giphy.com/mine"))
    assert custom.format == "gif" and custom.gif_url == "https://giphy.com/mine"


def test_no_gif_on_gif_option_falls_back_to_repost():
    final = resolve_final(drafts(), Approval(choice=3, gif="none"))
    assert final.format == "repost" and final.gif_url is None and final.story_url == "https://example.com/s"


def test_edit_replaces_text_and_blank_edit_does_not():
    assert resolve_final(drafts(), Approval(choice=1, edit="  My own words.  ")).text == "My own words."
    assert resolve_final(drafts(), Approval(choice=1, edit="   ")).text == "Repost text."


def test_missing_option_is_a_clear_error():
    with pytest.raises(ValueError, match="no option 3"):
        resolve_final(drafts()[:2], Approval(choice=3))


@pytest.mark.parametrize("gif", ["5", "banana"])
def test_bad_gif_input_is_a_clear_error(gif):
    with pytest.raises(ValueError):
        resolve_final(drafts(), Approval(choice=3, gif=gif))


def test_markdown_lists_every_option_and_note():
    md = drafts_markdown(drafts(), note="There is no option 4.")
    assert "There is no option 4." in md
    assert all(f"**Option {n}**" in md for n in (1, 2, 3))
    assert "GIF 2: https://giphy.com/b" in md


def test_final_markdown_is_copy_ready():
    final = resolve_final(drafts(), Approval(choice=1))
    md = final_markdown(final, **STORY)
    assert "Repost text." in md and "https://example.com/s" in md and "Not posted anywhere" in md


def test_draft_survives_the_artifact_round_trip():
    for draft in drafts():
        assert row_to_draft(draft_to_row(draft)) == draft
