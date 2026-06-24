from __future__ import annotations

import asyncio
import hashlib
import json
import re
import types
from collections import defaultdict
from enum import Enum
from logging import getLogger
from typing import TYPE_CHECKING, Literal, Union, cast, get_args, get_origin

from cssselect import SelectorError
from pydantic import BaseModel, Field, ValidationError
from pydantic_ai import Agent, ModelRetry
from pydantic_ai.exceptions import UnexpectedModelBehavior
from pydantic_ai.usage import RunUsage

from crawlee._utils.docs import docs_group
from crawlee._utils.recoverable_state import RecoverableState

from ._base_extractor import BasePydanticAiHtmlExtractor
from ._prompts import _SELECTOR_INSTRUCTIONS
from ._skeleton_distiller import PydanticAiSkeletonDistiller

if TYPE_CHECKING:
    from typing import Any

    from parsel import Selector
    from pydantic_ai.models import Model
    from pydantic_ai.usage import UsageLimits

    from ._types import PydanticAiHtmlDistiller, PydanticAiHtmlExtractor, PydanticAiUsageStats, TSchema

logger = getLogger(__name__)

# Matches a value pseudo-element (`::text` or `::attr(name)`) at the end of a selector.
_LEAF_PSEUDO_RE = re.compile(r'::(?:text|attr\([^()]*\))\s*$')


class FieldSelector(BaseModel):
    """One node of a selector map. It is a leaf selector or an item group. It mirrors the user schema shape."""

    selector: str = Field(
        description=(
            'Parsel CSS selector. For data fields it must end with ::text or ::attr(...). '
            'For item-group fields it is a container selector matching every item element, '
            'without ::text or ::attr.'
        )
    )
    fields: dict[str, FieldSelector] | None = Field(
        default=None,
        description=(
            'Sub-selectors for the item fields. Set only for item-group fields. '
            'Each one is written relative to one item container.'
        ),
    )


class SelectorMap(BaseModel):
    """LLM output for `PydanticAiSelectorExtractor`. A tree of Parsel CSS selectors that mirrors the user schema."""

    selectors: dict[str, FieldSelector] = Field(
        description=(
            'Maps each schema field name to its selector. A leaf field maps to one selector. '
            'An item-group field maps to a container selector with sub-selectors.'
        )
    )


class SelectorCacheState(BaseModel):
    """Persisted selector cache of one `PydanticAiSelectorExtractor`.

    Each key is a `(schema, scope, cache_tag)` digest. Each value is the list
    of selector maps learned for that bucket, one per markup variant.
    """

    selectors: dict[str, list[SelectorMap]] = Field(default_factory=dict)


class _FieldKind(Enum):
    """The selector-mapping shape of a single schema field."""

    LEAF = 'leaf'
    """A scalar value extracted by one leaf selector."""

    LIST_SCALAR = 'list_scalar'
    """A list or set of scalars extracted by one leaf selector matching many nodes."""

    LIST_MODEL = 'list_model'
    """A list of items. Maps to a container selector plus relative sub-selectors."""

    NESTED_MODEL = 'nested_model'
    """A single nested model. Maps to a container selector plus relative sub-selectors."""

    LIST_UNION = 'list_union'
    """Unsupported: a list of a union type (a match cannot pick a member)."""

    LIST_OF_LISTS = 'list_of_lists'
    """Unsupported: a list nested inside a list."""

    MAPPING = 'mapping'
    """Unsupported: a `dict`-typed field."""

    UNSUPPORTED = 'unsupported'
    """Unsupported: any other parametrized annotation (tuple, set, ...)."""


@docs_group('Other')
class PydanticAiSelectorExtractor(BasePydanticAiHtmlExtractor):
    """Extractor that learns reusable CSS selectors and reuses them for free.

    On each call it first tries the cached selector maps and extracts with no LLM call when one fits. On a miss it
    asks the model for a new map, validates it against the live page, and caches it. A bucket keeps several maps,
    so A/B-tested markup variants can coexist.

    The cache is a `RecoverableState` persisted to a `KeyValueStore`. As an async context manager it loads at
    startup and saves at shutdown. Used standalone, it initializes lazily.

    With a `fallback` extractor, unsupported schemas and generation failures degrade to it. Infrastructure errors
    such as credentials, HTTP, and usage limits propagate.

    See the `PydanticAiHtmlExtractor` protocol for the common extractor interface, and `PydanticAiDirectExtractor`
    for a per-page variant with no selector cache.

    ### Usage

    ```python
    from pydantic import BaseModel
    from pydantic_ai.models.openai import OpenAIChatModel
    from pydantic_ai.providers.openai import OpenAIProvider

    from crawlee.crawlers import PydanticAiDirectExtractor, PydanticAiSelectorExtractor


    class Product(BaseModel):
        name: str
        price: str | None


    model = OpenAIChatModel('gpt-5.4-nano', provider=OpenAIProvider(api_key='...'))
    extractor = PydanticAiSelectorExtractor(model=model, fallback=PydanticAiDirectExtractor(model=model))
    product = await extractor.extract('<html>...</html>', Product, cache_tag='product')
    ```
    """

    _MAX_RENDER_DEPTH = 5
    """Hard cap on `_format_fields` recursion depth."""

    def __init__(
        self,
        model: str | Model,
        *,
        kvs_cache_key: str | None = None,
        distiller: PydanticAiHtmlDistiller | None = None,
        instructions: str = _SELECTOR_INSTRUCTIONS,
        retries: int = 3,
        max_variants: int = 5,
        fallback: PydanticAiHtmlExtractor | None = None,
        usage_limits: UsageLimits | None = None,
        persistence: bool = True,
    ) -> None:
        """Initialize a new instance.

        Args:
            model: A provider-prefixed name (e.g. `'openai:gpt-5.4-nano'`) or a pydantic-ai `Model`.
            kvs_cache_key: Name of the `KeyValueStore` record holding the selector cache. Defaults to `'AI-SELECTORS'`.
            distiller: The HTML distiller shaping the LLM input. Defaults to `PydanticAiSkeletonDistiller`.
            instructions: Base selector-generation instructions. The distiller's prompt notes are appended
                automatically.
            retries: How many times the model may fix failing selectors within one generation.
            max_variants: Cap on cached selector maps per bucket.
            fallback: Extractor to degrade to when generation fails or the schema shape is unsupported.
            usage_limits: Optional pydantic-ai `UsageLimits` applied to every generation run.
            persistence: Whether the selector cache is persisted. Disable for ephemeral runs or tests.
        """
        if max_variants < 1:
            raise ValueError('max_variants must be at least 1, so each bucket can keep one selector map.')

        super().__init__(
            model,
            distiller=distiller or PydanticAiSkeletonDistiller(),
            instructions=instructions,
            usage_limits=usage_limits,
        )
        self._retries = retries
        self._max_variants = max_variants
        self._fallback = fallback
        self._persistence = persistence
        self._share_usage_with_fallback()

        self._selector_cache: RecoverableState[SelectorCacheState] = RecoverableState(
            default_state=SelectorCacheState(),
            persist_state_key=kvs_cache_key or 'AI-SELECTORS',
            persistence_enabled=persistence,
            logger=logger,
        )
        self._locks: defaultdict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._init_lock = asyncio.Lock()
        self._active = False

    def set_ai_usage(self, value: PydanticAiUsageStats) -> None:
        """Adopt `value` and re-share it with the fallback chain."""
        super().set_ai_usage(value)
        self._share_usage_with_fallback()

    def _share_usage_with_fallback(self) -> None:
        """Make the fallback chain accumulate into this extractor's `ai_usage`."""
        if self._fallback is not None:
            self._fallback.set_ai_usage(self._ai_usage)

    @property
    def active(self) -> bool:
        """Whether the extractor is in its async context-manager scope."""
        return self._active

    async def __aenter__(self) -> PydanticAiSelectorExtractor:
        """Initialize the selector cache eagerly."""
        if self._active:
            raise RuntimeError(f'The {type(self).__name__} is already active.')

        if not self._selector_cache.is_initialized:
            await self._selector_cache.initialize()

        self._active = True
        return self

    async def __aexit__(self, exc_type: object, exc_value: object, exc_traceback: object) -> None:
        """Persist the selector cache one final time and detach from events."""
        if not self._active:
            raise RuntimeError(f'The {type(self).__name__} is not active.')

        await self._selector_cache.teardown()
        self._active = False

    async def extract(
        self,
        content: str | Selector,
        schema: type[TSchema],
        *,
        scope: str | None = None,
        cache_tag: str | None = None,
        additional_instructions: str | None = None,
    ) -> TSchema:
        """Extract `schema` from `content` using cached or freshly generated selectors.

        Args:
            content: Raw HTML or a parsed Parsel `Selector`.
            schema: The Pydantic model describing the desired output.
            scope: Optional CSS selector restricting extraction to the first matching subtree.
            cache_tag: Optional tag identifying the page kind. Selectors are cached per tag.
            additional_instructions: Extra instructions appended for this call only.

        Raises:
            ValueError: When the schema shape is unsupported and no fallback is configured, or when `scope` matches
                nothing.
        """
        selector = self._as_selector(content)

        if scope is not None:
            # Everything below runs against this subtree, so generated selectors cannot match content outside the scope.
            selector = self._resolve_scope(selector, scope)

        # Reject unsupported schema shapes before any cache or LLM work.
        reason = self._unsupported_schema_reason(schema)
        if reason is not None:
            if self._fallback is not None:
                logger.info(
                    f'Schema {schema.__name__} is not supported by cached selectors ({reason}). '
                    f'Delegating to the fallback extractor.'
                )
                return await self._delegate_to_fallback(selector, schema, cache_tag, additional_instructions)
            raise ValueError(
                f'PydanticAiSelectorExtractor does not support this schema shape: {reason}. '
                'Configure a fallback extractor or use PydanticAiDirectExtractor for it.'
            )

        if not self._selector_cache.is_initialized:
            async with self._init_lock:
                if not self._selector_cache.is_initialized:
                    # Lazy init for standalone use. Under `PydanticAiCrawler` the context manager inits it at startup.
                    await self._selector_cache.initialize()

        cache_digest = self._build_cache_digest(schema, scope, cache_tag)
        variants = self._selector_cache.current_value.selectors.setdefault(cache_digest, [])

        extracted = self._try_cached_variants(variants, selector, schema)
        if extracted is not None:
            logger.debug(f'Cache hit for {schema.__name__} (tag={cache_tag!r}); extracted with no model call.')
            return extracted

        async with self._locks[cache_digest]:
            # A concurrent miss may have generated selectors while we waited for the lock, so check the cache again.
            extracted = self._try_cached_variants(variants, selector, schema)
            if extracted is not None:
                logger.debug(f'Cache hit for {schema.__name__} (tag={cache_tag!r}) after waiting on the lock.')
                return extracted

            logger.debug(f'Cache miss for {schema.__name__} (tag={cache_tag!r}); generating selectors.')
            try:
                selector_map = await self._generate_selectors(selector, schema, additional_instructions)
            except UnexpectedModelBehavior:
                if self._fallback is not None:
                    logger.info(
                        f'Selector generation failed for {schema.__name__} (tag={cache_tag!r}). '
                        'Delegating to the fallback extractor.'
                    )
                    return await self._delegate_to_fallback(selector, schema, cache_tag, additional_instructions)
                raise

            variants.insert(0, selector_map)
            # `variants` is a live reference into the cached state, so trim it in place. Reassigning would shadow it
            # instead of updating the cache.
            del variants[self._max_variants :]

            logger.debug(
                f'Cached new selectors for {schema.__name__} (tag={cache_tag!r}); {len(variants)} variant(s) in bucket.'
            )
            return self._apply_selectors(selector_map, selector, schema)

    async def _delegate_to_fallback(
        self,
        selector: Selector,
        schema: type[TSchema],
        cache_tag: str | None,
        additional_instructions: str | None,
    ) -> TSchema:
        if self._fallback is None:
            raise RuntimeError('Cannot delegate to a fallback extractor because none is configured.')
        # The scope was already applied to `selector`, so the fallback gets the subtree and no scope.
        return await self._fallback.extract(
            selector,
            schema,
            cache_tag=cache_tag,
            additional_instructions=additional_instructions,
        )

    def _try_cached_variants(
        self,
        variants: list[SelectorMap],
        selector: Selector,
        schema: type[TSchema],
    ) -> TSchema | None:
        for index, selector_map in enumerate(variants):
            try:
                extracted = self._apply_selectors(selector_map, selector, schema)
            except (ValidationError, ValueError, SelectorError):
                continue  # belongs to a different template variant or is malformed
            if index > 0:
                # Move-to-front so this variant is tried first next time.
                variants.insert(0, variants.pop(index))
            return extracted
        return None

    async def _generate_selectors(
        self,
        selector: Selector,
        schema: type[TSchema],
        additional_instructions: str | None,
    ) -> SelectorMap:
        agent: Agent[None, SelectorMap] = cast(
            'Agent[None, SelectorMap]',
            Agent(
                self._model,
                output_type=SelectorMap,
                instructions=self._base_instructions,
                retries=self._retries,
            ),
        )

        @agent.output_validator
        def _validate(plan: SelectorMap) -> SelectorMap:
            self._check_fields_covered(plan, schema)
            self._check_selectors_compile_and_match(plan, selector)
            self._check_apply_succeeds(plan, selector, schema)
            return plan

        skeleton = await asyncio.to_thread(self._distiller.distill, selector.get())
        # The output type is `SelectorMap`, so the user schema never reaches the model through a tool. The prompt
        # spells it out instead, paid once per markup variant.
        prompt = f'Fields to extract:\n{self._format_fields(schema)}\n\nPage skeleton:\n{skeleton}'

        run_usage = RunUsage()
        try:
            result = await agent.run(
                prompt,
                instructions=additional_instructions,
                usage_limits=self._usage_limits,
                usage=run_usage,
            )
        finally:
            self._ai_usage.add(run_usage)

        logger.debug(
            f'Selector generation for {schema.__name__} used {run_usage.requests} request(s), '
            f'{run_usage.input_tokens} input + {run_usage.output_tokens} output tokens.'
        )
        return result.output

    @staticmethod
    def _check_fields_covered(plan: SelectorMap, schema: type[BaseModel]) -> None:
        """Raise `ModelRetry` when the plan misses required schema fields."""
        missing = [name for name in schema.model_fields if name not in plan.selectors]
        if missing:
            raise ModelRetry(f'No selector provided for fields: {missing}')

    @staticmethod
    def _is_leaf_selector_form(selector: str) -> bool:
        """Whether a selector targets a value (ends with `::text` or `::attr(...)`)."""
        return _LEAF_PSEUDO_RE.search(selector) is not None

    def _check_selectors_compile_and_match(self, plan: SelectorMap, selector: Selector) -> None:
        """Raise `ModelRetry` on invalid CSS, wrong selector form, or no matches.

        Generation-time strictness: a selector matching nothing yields `[]` for list fields and `None` for optional
        ones. Both are schema-valid, so without this check a useless selector map would be accepted and cached,
        silently returning empty data from then on. At apply time empty matches stay legal (other pages of the
        template may genuinely lack the content).
        """
        empty: list[str] = []
        for name, field_selector in plan.selectors.items():
            is_container = field_selector.fields is not None
            is_leaf_form = self._is_leaf_selector_form(field_selector.selector)
            # A container with ::text/::attr yields text nodes instead of elements, breaking `_apply_fields` on item
            # groups. A leaf without that form yields whole elements instead of a value. Flag both so the model
            # fixes the form, not a downstream symptom.
            if is_container and is_leaf_form:
                raise ModelRetry(
                    f'The container selector for field {name!r} must not end with ::text or ::attr(...); '
                    'that form is only for leaf fields. A container selects item ELEMENTS, and sub-selectors '
                    'extract leaves relative to each item.'
                )
            if not is_container and not is_leaf_form:
                raise ModelRetry(
                    f'The selector for leaf field {name!r} must end with ::text or ::attr(...) so it yields a '
                    'value, not an element. Append ::text for the element text or ::attr(name) for an attribute.'
                )
            try:
                matched = selector.css(field_selector.selector)
            except SelectorError as exc:
                raise ModelRetry(
                    f'The selector for field {name!r} is not valid Parsel CSS ({exc}). '
                    'Use plain CSS ending with ::text or ::attr(...).'
                ) from exc
            if not matched:
                empty.append(name)
                continue
            # Check sub-selectors against the first matched item. Cheap, and turns a vague "field required" error
            # into a targeted "this relative selector matches nothing in an item".
            for sub_name, sub in (field_selector.fields or {}).items():
                if not self._is_leaf_selector_form(sub.selector):
                    raise ModelRetry(
                        f'The selector for field {name!r}[].{sub_name!r} must end with ::text or ::attr(...); '
                        'sub-selectors extract leaf values relative to one item container.'
                    )
                try:
                    sub_matched = matched[0].css(sub.selector)
                except SelectorError as exc:
                    raise ModelRetry(
                        f'The selector for field {name!r}[].{sub_name!r} is not valid Parsel CSS ({exc}). '
                        'Use plain CSS ending with ::text or ::attr(...). '
                        'Sub-selectors must be RELATIVE to one item container.'
                    ) from exc
                if not sub_matched:
                    empty.append(f'{name}[].{sub_name}')
        if empty:
            raise ModelRetry(
                f'Selectors matched no elements on this page for fields: {empty}. '
                'Anchor them to elements that actually exist in the document.'
            )

    def _check_apply_succeeds(
        self,
        plan: SelectorMap,
        selector: Selector,
        schema: type[BaseModel],
    ) -> None:
        """Raise `ModelRetry` when applying the plan produces schema-invalid data."""
        try:
            self._apply_selectors(plan, selector, cast('type[TSchema]', schema))
        except ValidationError as exc:
            failures = '; '.join(f'{".".join(map(str, error["loc"]))}: {error["msg"]}' for error in exc.errors())
            raise ModelRetry(
                f'Applying the selectors to the live page produced invalid data: {failures}. '
                'Adjust the failing selectors.'
            ) from exc

    def _apply_selectors(
        self,
        plan: SelectorMap,
        selector: Selector,
        schema: type[TSchema],
    ) -> TSchema:
        """Run `plan` against `selector` and build a validated `schema` instance."""
        return schema.model_validate(self._apply_fields(plan.selectors, selector, schema))

    def _apply_fields(
        self,
        fields: dict[str, FieldSelector],
        scope: Selector,
        schema: type[BaseModel],
    ) -> dict[str, Any]:
        """Apply one level of the selector tree relative to `scope`.

        Item-group fields recurse. The container selector enumerates item elements. Sub-selectors run relative to
        each item (native Parsel behavior of `element.css(...)`).
        """
        raw: dict[str, Any] = {}
        for name, info in schema.model_fields.items():
            field_selector = fields.get(name)
            if field_selector is None:
                continue
            kind, inner = self._classify_field(info.annotation)

            if kind is _FieldKind.LIST_MODEL and inner is not None:
                raw[name] = [
                    self._apply_fields(field_selector.fields or {}, element, inner)
                    for element in scope.css(field_selector.selector)
                ]
            elif kind is _FieldKind.NESTED_MODEL and inner is not None:
                matched = scope.css(field_selector.selector)
                raw[name] = self._apply_fields(field_selector.fields or {}, matched[0], inner) if matched else None
            elif kind is _FieldKind.LIST_SCALAR:
                raw[name] = [value.strip() for value in scope.css(field_selector.selector).getall()]
            else:
                value = scope.css(field_selector.selector).get()
                raw[name] = value.strip() if isinstance(value, str) else value
        return raw

    @staticmethod
    def _build_cache_digest(schema: type[BaseModel], scope: str | None, cache_tag: str | None) -> str:
        """Build the digest identifying a `(schema, scope, cache_tag)` bucket.

        Scope and tag are part of the identity. The same schema extracted from a different region or page kind gets
        its own selector bucket.
        """
        return hashlib.sha256(
            json.dumps(schema.model_json_schema(), sort_keys=True).encode()
            + b'\x00'
            + (scope or '').encode()
            + b'\x00'
            + (cache_tag or '').encode()
        ).hexdigest()[:16]

    @staticmethod
    def _unwrap_optional(annotation: Any) -> Any:
        """Return `X` for `X | None`, the annotation unchanged otherwise."""
        origin = get_origin(annotation)
        if origin is Union or origin is types.UnionType:
            args = [a for a in get_args(annotation) if a is not type(None)]
            if len(args) == 1:
                return args[0]
        return annotation

    @staticmethod
    def _is_union(annotation: Any) -> bool:
        """Return whether `annotation` is a non-Optional union."""
        origin = get_origin(annotation)
        return origin is Union or origin is types.UnionType

    def _classify_field(self, annotation: Any) -> tuple[_FieldKind, type[BaseModel] | None]:
        """Classify a field annotation into its selector-mapping shape.

        Single source of truth for the field-shape introspection shared by the capability gate, the prompt renderer
        and the selector applier. Optional wrappers (`X | None`) are stripped first, so `str | None` is a leaf and
        `list[Item] | None` is a list of models.

        Args:
            annotation: The raw field annotation, possibly `Optional`.
        """
        annotation = self._unwrap_optional(annotation)
        origin = get_origin(annotation)

        if origin in (list, set):
            args = get_args(annotation)
            item = self._unwrap_optional(args[0]) if args else str
            # `list[A | B]` is ambiguous: a match can't be tied to a specific union member, so treat it as unsupported.
            if self._is_union(item):
                return _FieldKind.LIST_UNION, None
            if isinstance(item, type) and issubclass(item, BaseModel):
                return _FieldKind.LIST_MODEL, item
            if get_origin(item) in (list, set):
                return _FieldKind.LIST_OF_LISTS, None
            return _FieldKind.LIST_SCALAR, None
        if isinstance(annotation, type) and issubclass(annotation, BaseModel):
            return _FieldKind.NESTED_MODEL, annotation
        if origin is dict:
            return _FieldKind.MAPPING, None
        # A `Literal` constrains a value to a fixed set. It is extracted as a leaf string and validated by Pydantic.
        if origin is Literal:
            return _FieldKind.LEAF, None
        if origin is not None:
            return _FieldKind.UNSUPPORTED, None
        return _FieldKind.LEAF, None

    def _unsupported_schema_reason(self, schema: type[BaseModel], *, depth: int = 0) -> str | None:
        """Return why `schema` cannot be served by cached selectors, or `None`.

        Supported shapes are scalar leaves, lists of scalars, lists of models with leaf-only fields, and single
        nested models with leaf-only fields. Run before generation to avoid spending LLM retries on an impossible
        schema.

        Args:
            schema: The schema to check.
            depth: Current recursion depth.
        """
        for name, info in schema.model_fields.items():
            kind, model = self._classify_field(info.annotation)

            if kind is _FieldKind.LIST_UNION:
                return f'field {name!r} is a list of a union type'
            if kind is _FieldKind.LIST_OF_LISTS:
                return f'field {name!r} is a list of lists'
            if kind is _FieldKind.MAPPING:
                return f'field {name!r} is a mapping'
            if kind is _FieldKind.UNSUPPORTED:
                return f'field {name!r} has an unsupported annotation {info.annotation!r}'

            if kind in (_FieldKind.LIST_MODEL, _FieldKind.NESTED_MODEL) and model is not None:
                if depth >= 1:
                    noun = 'item lists' if kind is _FieldKind.LIST_MODEL else 'models'
                    return f'field {name!r} nests {noun} deeper than one level'
                reason = self._unsupported_schema_reason(model, depth=depth + 1)
                if reason is not None:
                    return reason
        return None

    def _format_fields(self, schema: type[BaseModel]) -> str:
        """Render the schema fields as an indented text block for the prompt.

        The selector agent's output type is `SelectorMap`, so the user schema is invisible to the model. This spells
        out the field names, types, item-group structure, and descriptions.

        Args:
            schema: The Pydantic model whose fields to render.

        Raises:
            ValueError: When recursion exceeds `_MAX_RENDER_DEPTH`.
        """

        def render(model: type[BaseModel], indent: int) -> list[str]:
            if indent > self._MAX_RENDER_DEPTH:
                raise ValueError(f'Schema rendering exceeded depth {self._MAX_RENDER_DEPTH}')
            pad = '  ' * indent
            lines: list[str] = []
            for name, info in model.model_fields.items():
                description = f': {info.description}' if info.description else ''
                kind, inner = self._classify_field(info.annotation)
                if kind is _FieldKind.LIST_MODEL and inner is not None:
                    lines.append(f'{pad}- {name} (list of items, each with:){description}')
                    lines.extend(render(inner, indent + 1))
                elif kind is _FieldKind.NESTED_MODEL and inner is not None:
                    lines.append(f'{pad}- {name} (item with:){description}')
                    lines.extend(render(inner, indent + 1))
                else:
                    annotation = self._unwrap_optional(info.annotation)
                    type_name = annotation.__name__ if isinstance(annotation, type) else str(annotation)
                    lines.append(f'{pad}- {name} ({type_name}){description}')
            return lines

        return '\n'.join(render(schema, 0))
