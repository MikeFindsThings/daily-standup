"""GIF candidates from the Giphy search API."""

import httpx

from standup.news import TIMEOUT, USER_AGENT


def search_gifs(api_key: str, query: str, limit: int = 3) -> list[str]:
    """Return up to `limit` GIF page URLs for `query`. PG-rated results only.

    Giphy takes the API key as a URL parameter, and httpx puts the full URL in its error
    messages. Errors are re-raised without the original so the key never reaches the logs.
    """
    try:
        resp = httpx.get(
            "https://api.giphy.com/v1/gifs/search",
            params={"api_key": api_key, "q": query, "limit": limit, "rating": "pg", "lang": "en"},
            headers={"User-Agent": USER_AGENT},
            timeout=TIMEOUT,
        )
        status = resp.status_code
    except httpx.HTTPError as exc:
        raise RuntimeError(f"Giphy request failed: {type(exc).__name__}") from None
    if status != 200:
        raise RuntimeError(f"Giphy returned HTTP {status}")
    return [item["url"] for item in resp.json().get("data", []) if item.get("url")][:limit]
