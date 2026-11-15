"""Data shapes shared by the flow, the store, and (in Stage 2) the MCP server."""

from typing import Literal

from prefect.input import RunInput
from pydantic import BaseModel, Field

PostFormat = Literal["repost", "one-liner", "gif"]


class Story(BaseModel):
    title: str
    url: str
    source: str
    summary: str = ""
    points: int | None = None


class Pick(BaseModel):
    rank: int
    title: str
    url: str
    source: str
    summary: str = ""
    detail: str = Field(description="The specific, concrete detail a joke can hinge on")
    reason: str = Field(description="One line on why this story has a comedic angle")


class Draft(BaseModel):
    round: int
    option: int
    text: str
    format: PostFormat
    story_title: str
    story_url: str
    gif_query: str = ""
    gif_urls: list[str] = Field(default_factory=list)


class FinalPost(BaseModel):
    text: str
    format: PostFormat
    story_url: str | None = None
    gif_url: str | None = None


class Approval(RunInput):
    """What the human sends back when the flow is waiting."""

    action: Literal["approve", "regenerate", "skip"] = Field(
        default="approve",
        description="approve = use a draft; regenerate = new drafts on the next-best story; skip = no post today",
    )
    choice: int = Field(default=1, ge=1, le=9, description="Which option to use (1, 2, or 3)")
    edit: str | None = Field(
        default=None, description="Optional. Your own wording; replaces the chosen option's text."
    )
    gif: str | None = Field(
        default=None,
        description="Optional. A GIF candidate number (1-3), a GIF URL, or 'none' for no GIF.",
    )
