from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Literal

import pytest
from pydantic import BaseModel, Field, create_model
from pydantic_ai.exceptions import UnexpectedModelBehavior
from pydantic_ai.messages import ModelResponse, ToolCallPart
from pydantic_ai.models.function import FunctionModel
from pydantic_ai.models.test import TestModel

from crawlee.crawlers import PydanticAiDirectExtractor, PydanticAiSelectorExtractor, PydanticAiUsageStats

if TYPE_CHECKING:
    from typing import Any

    from pydantic_ai.messages import ModelMessage
    from pydantic_ai.models import Model
    from pydantic_ai.models.function import AgentInfo

    from crawlee.crawlers._pydantic_ai._types import PydanticAiHtmlExtractor


class _Item(BaseModel):
    name: str


class _Posts(BaseModel):
    posts: list[_Item]


class _Nested(BaseModel):
    title: str
    item: _Item | None = None


class _WithDefault(BaseModel):
    title: str
    tag: str = 'default'


class _NullableNote(BaseModel):
    title: str
    note: str | None


class _Aliased(BaseModel):
    name: str = Field(alias='display_name')


class _Collections(BaseModel):
    items: list[str]
    unique: set[str]


class _Status(BaseModel):
    status: Literal['in_stock', 'sold_out']


class _Mapping(BaseModel):
    data: dict[str, str]


NAME_HTML = '<div><span class="n">X</span></div>'
LIST_HTML = '<ul><li class="r"><a class="t" href="/a">A</a></li><li class="r"><a class="t" href="/b">B</a></li></ul>'

NAME_SELECTORS = {'selectors': {'name': {'selector': '.n::text'}}}
POSTS_SELECTORS = {'selectors': {'posts': {'selector': 'li.r', 'fields': {'name': {'selector': '.t::text'}}}}}


def _model(*plans: dict[str, Any]) -> FunctionModel:
    """Build a model that returns the given selector maps in order, repeating the last for any further calls."""
    state = {'index': 0}

    def respond(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        _messages = messages
        plan = plans[min(state['index'], len(plans) - 1)]
        state['index'] += 1
        return ModelResponse(parts=[ToolCallPart(tool_name=info.output_tools[0].name, args=plan)])

    return FunctionModel(respond)


def _extractor(
    model: str | Model | None = None,
    *,
    fallback: PydanticAiHtmlExtractor | None = None,
    retries: int = 3,
    max_variants: int = 5,
) -> PydanticAiSelectorExtractor:
    return PydanticAiSelectorExtractor(
        model or TestModel(),
        persistence=False,
        fallback=fallback,
        retries=retries,
        max_variants=max_variants,
    )


@pytest.mark.parametrize(
    ('annotation', 'match'),
    [
        pytest.param(dict[str, str], 'mapping', id='dict'),
        pytest.param(list[int | str], 'list of a union', id='list-of-union'),
        pytest.param(list[list[str]], 'list of lists', id='list-of-lists'),
        pytest.param(tuple[int, ...], 'unsupported annotation', id='tuple'),
        pytest.param(int | str, 'unsupported annotation', id='scalar-union'),
        pytest.param(_Posts, 'deeper than one level', id='deep-nesting'),
    ],
)
async def test_rejects_unsupported_schema(annotation: Any, match: str) -> None:
    # The schema shape is checked before any model call, so an unsupported one raises with the reason.
    schema = create_model('_Unsupported', field=(annotation, ...))

    with pytest.raises(ValueError, match=match):
        await _extractor().extract('<div></div>', schema)


async def test_extracts_scalar() -> None:
    result = await _extractor(_model(NAME_SELECTORS)).extract(NAME_HTML, _Item)

    assert result == _Item(name='X')


async def test_extracts_collection_of_scalars() -> None:
    plan = {'selectors': {'items': {'selector': '.a::text'}, 'unique': {'selector': '.b::text'}}}
    html = '<div><span class="a">x</span><span class="a">y</span><span class="b">p</span><span class="b">p</span></div>'

    result = await _extractor(_model(plan)).extract(html, _Collections)

    # The list keeps order and duplicates; the set is deduplicated.
    assert result == _Collections(items=['x', 'y'], unique={'p'})


async def test_extracts_list_of_items() -> None:
    result = await _extractor(_model(POSTS_SELECTORS)).extract(LIST_HTML, _Posts)

    assert result == _Posts(posts=[_Item(name='A'), _Item(name='B')])


async def test_extracts_literal_field() -> None:
    plan = {'selectors': {'status': {'selector': '.s::text'}}}

    result = await _extractor(_model(plan)).extract('<div><span class="s">in_stock</span></div>', _Status)

    assert result == _Status(status='in_stock')


async def test_extracts_field_with_alias() -> None:
    # The selector map is keyed by the field name, so by-name validation lets a schema with an alias accept it.
    result = await _extractor(_model(NAME_SELECTORS)).extract(NAME_HTML, _Aliased)

    assert result.name == 'X'


async def test_reuses_cached_plan() -> None:
    extractor = _extractor(_model(NAME_SELECTORS))

    assert await extractor.extract(NAME_HTML, _Item, cache_tag='test') == _Item(name='X')
    assert await extractor.extract(NAME_HTML, _Item, cache_tag='test') == _Item(name='X')
    # The second extract is served from the cache, so the model was consulted only once.
    assert extractor.ai_usage.requests == 1


async def test_concurrent_generate() -> None:
    extractor = _extractor(_model(NAME_SELECTORS))

    results = await asyncio.gather(
        extractor.extract(NAME_HTML, _Item, cache_tag='test'),
        extractor.extract(NAME_HTML, _Item, cache_tag='test'),
    )

    assert results == [_Item(name='X'), _Item(name='X')]
    assert extractor.ai_usage.requests == 1


async def test_cached_plan_for_optional_field() -> None:
    plan = {
        'selectors': {
            'title': {'selector': 'h1::text'},
            'item': {'selector': '.item', 'fields': {'name': {'selector': '.n::text'}}},
        }
    }
    extractor = _extractor(_model(plan))

    first_call = await extractor.extract(
        '<div><h1>T</h1><div class="item"><span class="n">X</span></div></div>', _Nested, cache_tag='test'
    )
    # The second page has no item, but the cached plan is still valid and returns None for the optional field.
    second_call = await extractor.extract('<div><h1>T2</h1></div>', _Nested, cache_tag='test')

    assert first_call == _Nested(title='T', item=_Item(name='X'))
    assert second_call == _Nested(title='T2', item=None)
    assert extractor.ai_usage.requests == 1


@pytest.mark.parametrize(
    ('schema', 'expected'),
    [
        pytest.param(_WithDefault, _WithDefault(title='T', tag='default'), id='with-default'),
        pytest.param(_NullableNote, _NullableNote(title='T', note=None), id='nullable'),
    ],
)
async def test_omits_selector_for_optional_field(schema: type[BaseModel], expected: BaseModel) -> None:
    plan = {'selectors': {'title': {'selector': 'h1::text'}}}
    extractor = _extractor(_model(plan))

    result = await extractor.extract('<div><h1>T</h1></div>', schema, cache_tag='test')

    assert result == expected
    assert extractor.ai_usage.requests == 1


async def test_cache_key_ignores_field_description() -> None:
    schema_a = create_model('_DescA', name=(str, Field(description='description')))
    schema_b = create_model('_DescB', name=(str, Field(description='no description')))
    extractor = _extractor(_model(NAME_SELECTORS))

    await extractor.extract(NAME_HTML, schema_a, cache_tag='test')
    await extractor.extract(NAME_HTML, schema_b, cache_tag='test')

    # Same shape and tag, so the second extract reuses the cached plan with no model call.
    assert extractor.ai_usage.requests == 1


async def test_caches_different_tags() -> None:
    extractor = _extractor(_model(NAME_SELECTORS))

    await extractor.extract(NAME_HTML, _Item, cache_tag='a')
    await extractor.extract(NAME_HTML, _Item, cache_tag='b')  # different bucket, generated again
    await extractor.extract(NAME_HTML, _Item, cache_tag='a')  # first bucket still cached

    # One generation per tag. A shared bucket would have served 'b' from cache, leaving the count at one.
    assert extractor.ai_usage.requests == 2


async def test_eviction_of_oldest_variant() -> None:
    pages = {
        'a': '<div><span class="a">A</span></div>',
        'b': '<div><span class="b">B</span></div>',
        'c': '<div><span class="c">C</span></div>',
    }
    extractor = _extractor(
        _model(
            {'selectors': {'name': {'selector': '.a::text'}}},
            {'selectors': {'name': {'selector': '.b::text'}}},
            {'selectors': {'name': {'selector': '.c::text'}}},  # 'c' generated and evicted 'a'
            {'selectors': {'name': {'selector': '.a::text'}}},  # 'a' regenerated after eviction
        ),
        max_variants=2,
    )

    for key in ('a', 'b', 'c'):
        await extractor.extract(pages[key], _Item, cache_tag='test')

    # With max_variants=2 the 'a' plan was evicted, so extracting 'a' again generates a fourth time.
    await extractor.extract(pages['a'], _Item, cache_tag='test')

    assert extractor.ai_usage.requests == 4


@pytest.mark.parametrize(
    'invalid_plan',
    [
        pytest.param({'selectors': {}}, id='missing-field'),
        pytest.param(
            {'selectors': {'posts': {'selector': 'li.r::text', 'fields': {'name': {'selector': '.t::text'}}}}},
            id='container-with-value-form',
        ),
        pytest.param(
            {'selectors': {'posts': {'selector': 'li[', 'fields': {'name': {'selector': '.t::text'}}}}},
            id='invalid-css',
        ),
        pytest.param(
            {'selectors': {'posts': {'selector': 'li.r', 'fields': {'name': {'selector': '.t'}}}}},
            id='sub-without-value-form',
        ),
        pytest.param(
            {'selectors': {'posts': {'selector': 'li.r', 'fields': {'name': {'selector': '.absent::text'}}}}},
            id='sub-matches-nothing',
        ),
    ],
)
async def test_retries_with_invalid_plan(invalid_plan: dict[str, Any]) -> None:
    # Each plan trips a different validation guard. The model is asked to fix it, then returns a valid plan.
    extractor = _extractor(_model(invalid_plan, POSTS_SELECTORS))

    result = await extractor.extract(LIST_HTML, _Posts, cache_tag='test')

    assert result == _Posts(posts=[_Item(name='A'), _Item(name='B')])
    assert extractor.ai_usage.requests == 2  # the first plan failed validation, the second one succeeded


async def test_retries_with_invalid_data() -> None:
    # The selectors are well-formed and match, but the extracted value fails schema validation, so the plan is retried.
    html = '<div><span class="bad">WRONG</span><span class="good">in_stock</span></div>'
    extractor = _extractor(
        _model(
            {'selectors': {'status': {'selector': '.bad::text'}}},  # 'WRONG' is not a valid Literal value
            {'selectors': {'status': {'selector': '.good::text'}}},
        )
    )

    result = await extractor.extract(html, _Status, cache_tag='test')

    assert result == _Status(status='in_stock')
    assert extractor.ai_usage.requests == 2


async def test_unsupported_schema_delegates_to_fallback() -> None:
    fallback = PydanticAiDirectExtractor(TestModel(custom_output_args={'data': {'k': 'v'}}))

    result = await _extractor(fallback=fallback).extract('<div></div>', _Mapping)

    assert result == _Mapping(data={'k': 'v'})


async def test_generation_failure_delegates_to_fallback() -> None:
    # The selector matches nothing, so generation fails and the extractor degrades to the fallback.
    bad_plan = {'selectors': {'name': {'selector': '.absent::text'}}}
    fallback = PydanticAiDirectExtractor(TestModel(custom_output_args={'name': 'from-fallback'}))

    result = await _extractor(_model(bad_plan), fallback=fallback, retries=0).extract(
        NAME_HTML, _Item, cache_tag='test'
    )

    assert result == _Item(name='from-fallback')


async def test_generation_failure_raises() -> None:
    bad_plan = {'selectors': {'name': {'selector': '.absent::text'}}}

    with pytest.raises(UnexpectedModelBehavior):
        await _extractor(_model(bad_plan), retries=0).extract(NAME_HTML, _Item, cache_tag='test')


async def test_scope_raises() -> None:
    with pytest.raises(ValueError, match='matched nothing'):
        await _extractor().extract('<div>x</div>', _Item, scope='.missing')


async def test_fallback_shares_usage_accumulator() -> None:
    fallback = PydanticAiDirectExtractor(TestModel())
    extractor = _extractor(fallback=fallback)

    assert fallback.ai_usage is extractor.ai_usage


def test_set_ai_usage_reshares_with_fallback() -> None:
    fallback = PydanticAiDirectExtractor(TestModel())
    extractor = _extractor(fallback=fallback)
    new_usage = PydanticAiUsageStats()

    extractor.set_ai_usage(new_usage)

    assert extractor.ai_usage is new_usage
    assert fallback.ai_usage is new_usage


async def test_active_state() -> None:
    extractor = _extractor()

    assert extractor.active is False
    async with extractor:
        assert extractor.active is True
    assert extractor.active is False


async def test_enter_propagates_to_fallback() -> None:
    inner = _extractor()
    outer = _extractor(fallback=inner)

    async with outer:
        assert inner.active is True
    assert inner.active is False


async def test_double_enter_raises() -> None:
    extractor = _extractor()

    async with extractor:
        with pytest.raises(RuntimeError, match='already active'):
            await extractor.__aenter__()


async def test_exit_without_enter_raises() -> None:
    extractor = _extractor()

    with pytest.raises(RuntimeError, match='not active'):
        await extractor.__aexit__(None, None, None)


async def test_cache_persists_across_instances() -> None:
    async with PydanticAiSelectorExtractor(_model(NAME_SELECTORS), kvs_cache_key='shared-cache') as first:
        assert await first.extract(NAME_HTML, _Item, cache_tag='test') == _Item(name='X')
        assert first.ai_usage.requests == 1

    # A fresh instance loads the cache from the KeyValueStore and serves without calling the model.
    async with PydanticAiSelectorExtractor(_model(NAME_SELECTORS), kvs_cache_key='shared-cache') as second:
        assert await second.extract(NAME_HTML, _Item, cache_tag='test') == _Item(name='X')
        assert second.ai_usage.requests == 0
