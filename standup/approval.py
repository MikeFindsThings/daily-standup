"""Pure functions around the approval step: what the human sees, and what their reply means."""

from standup.models import Approval, Draft, FinalPost

_NO_GIF = {"none", "no", "no gif", "nogif", "0"}


def drafts_markdown(drafts: list[Draft], note: str = "") -> str:
    """The message shown above the approval form in the Prefect UI."""
    story = drafts[0]
    lines = []
    if note:
        lines += [f"> **{note}**", ""]
    lines += [f"**Story:** [{story.story_title}]({story.story_url})", ""]
    for d in drafts:
        lines.append(f"**Option {d.option}** ({d.format}): {d.text}")
        for i, url in enumerate(d.gif_urls, 1):
            lines.append(f"  - GIF {i}: {url}")
        lines.append("")
    lines.append(
        "Pick an option to approve it, choose **regenerate** for drafts on the next-best story, "
        "or **skip** for no post today. Nothing is posted automatically."
    )
    return "\n".join(lines)


def resolve_final(drafts: list[Draft], approval: Approval) -> FinalPost:
    """Turn an 'approve' reply into the final post. Raises ValueError with a message the
    human can act on when the reply doesn't match the drafts on offer."""
    by_option = {d.option: d for d in drafts}
    draft = by_option.get(approval.choice)
    if draft is None:
        raise ValueError(
            f"There is no option {approval.choice}. Available: {', '.join(str(o) for o in sorted(by_option))}."
        )

    text = (approval.edit or "").strip() or draft.text
    gif_input = (approval.gif or "").strip()
    gif_url: str | None
    if gif_input.lower() in _NO_GIF:
        gif_url = None
    elif gif_input.isdigit():
        index = int(gif_input)
        if not 1 <= index <= len(draft.gif_urls):
            raise ValueError(
                f"Option {draft.option} has {len(draft.gif_urls)} GIF candidate(s); there is no GIF {index}."
            )
        gif_url = draft.gif_urls[index - 1]
    elif gif_input.lower().startswith(("http://", "https://")):
        gif_url = gif_input
    elif gif_input:
        raise ValueError("GIF must be a candidate number, a URL starting with https://, or 'none'.")
    else:
        gif_url = draft.gif_urls[0] if draft.format == "gif" and draft.gif_urls else None

    if gif_url:
        post_format = "gif"
    elif draft.format == "gif":
        post_format = "repost"  # the GIF was dropped, so fall back to the link
    else:
        post_format = draft.format

    return FinalPost(
        text=text,
        format=post_format,
        story_url=draft.story_url if post_format == "repost" else None,
        gif_url=gif_url,
    )


def final_markdown(final: FinalPost, story_title: str, story_url: str) -> str:
    """The copy-ready artifact."""
    lines = ["## Today's post", "", "```", final.text, "```", ""]
    if final.story_url:
        lines.append(f"**Link to share:** {final.story_url}")
    if final.gif_url:
        lines.append(f"**GIF:** {final.gif_url}")
    lines += ["", "---", f"Format: {final.format} · Story: [{story_title}]({story_url})",
              "Not posted anywhere. Copy it into LinkedIn yourself."]
    return "\n".join(lines)
