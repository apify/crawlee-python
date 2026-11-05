from __future__ import annotations

import sys
from unittest.mock import patch

import pytest
from pydantic_ai.models.test import TestModel

from crawlee.crawlers import (
    PydanticAiCrawler,
    PydanticAiDirectExtractor,
)


def test_rejects_model_and_extractor_together() -> None:
    with pytest.raises(ValueError, match='at most one'):
        PydanticAiCrawler(model=TestModel(), extractor=PydanticAiDirectExtractor(TestModel()))


def test_default_extractor_is_direct() -> None:
    assert isinstance(PydanticAiCrawler(model=TestModel()).extractor, PydanticAiDirectExtractor)


def test_default_model_when_none_given(monkeypatch: pytest.MonkeyPatch) -> None:
    # The default model is an OpenAI one, whose client needs a key at construction.
    monkeypatch.setenv('OPENAI_API_KEY', 'test-key')

    # With neither model nor extractor, the crawler builds the default direct extractor.
    assert isinstance(PydanticAiCrawler().extractor, PydanticAiDirectExtractor)


def test_emits_experimental_warning() -> None:
    with pytest.warns(UserWarning, match='experimental'):
        PydanticAiCrawler(model=TestModel())


def test_exposes_extractor_and_usage() -> None:
    extractor = PydanticAiDirectExtractor(TestModel())
    crawler = PydanticAiCrawler(extractor=extractor)

    assert crawler.extractor is extractor
    assert crawler.ai_usage is extractor.ai_usage


def test_import_error_handled() -> None:
    # The `ai` extra is optional, so accessing the crawler without `pydantic_ai` installed must raise a clear error.
    blocked = {name: None for name in sys.modules if name == 'pydantic_ai' or name.startswith('pydantic_ai.')}
    with patch.dict('sys.modules', blocked):
        for name in list(sys.modules):
            if name.startswith('crawlee.crawlers._pydantic_ai'):
                sys.modules.pop(name, None)
        with pytest.raises(ImportError):
            from crawlee.crawlers._pydantic_ai import PydanticAiCrawler  # noqa: F401 PLC0415
