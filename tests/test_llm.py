import pytest

from standup.llm import call_cost


def test_call_cost_uses_per_model_prices():
    assert call_cost("claude-haiku-5-5", 1_000_000, 1_000_000) == pytest.approx(0.60)
    assert call_cost("claude-opus-5-5", 2_000, 1_000) == pytest.approx(0.028)


def test_unknown_model_is_priced_as_the_caption_model():
    assert call_cost("some-fallback-model", 1_000, 0) == pytest.approx(0.004)
