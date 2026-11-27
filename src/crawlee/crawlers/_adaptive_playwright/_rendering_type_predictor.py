from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from itertools import zip_longest
from logging import getLogger
from statistics import mean
from typing import TYPE_CHECKING, Annotated, Literal
from urllib.parse import urlparse

from pydantic import BaseModel, ConfigDict, Field, PlainSerializer, PlainValidator
from rapidfuzz.distance import JaroWinkler
from sklearn.linear_model import LogisticRegression
from typing_extensions import override

from crawlee._utils.docs import docs_group
from crawlee._utils.recoverable_state import RecoverableState

from ._utils import sklearn_model_serializer, sklearn_model_validator

if TYPE_CHECKING:
    from types import TracebackType

    from crawlee import Request

logger = getLogger(__name__)

UrlComponents = list[str]
RenderingType = Literal['static', 'client only']
FeatureVector = tuple[float, float]


class RenderingTypePredictorState(BaseModel):
    model_config = ConfigDict(validate_by_name=True, validate_by_alias=True)

    model: Annotated[
        LogisticRegression,
        Field(LogisticRegression),
        PlainValidator(sklearn_model_validator),
        PlainSerializer(sklearn_model_serializer),
    ]

    detection_results: Annotated[
        dict[RenderingType, dict[str, list[UrlComponents]]], Field(alias='detectionResults', default_factory=dict)
    ]
    """URL components of the detected requests by rendering type and label, which the model is trained on."""


@docs_group('Other')
@dataclass(frozen=True)
class RenderingTypePrediction:
    """Rendering type recommendation with detection probability recommendation."""

    rendering_type: RenderingType
    """Recommended rendering type."""
    detection_probability_recommendation: float
    """Recommended rendering detection probability. Expected values between 0-1.

    Zero represents absolute confidence in `rendering_type` recommendation.
    One represents no confidence in `rendering_type` recommendation."""


@docs_group('Other')
class RenderingTypePredictor(ABC):
    """Stores rendering type for previously crawled URLs and predicts the rendering type for unvisited urls."""

    def __init__(self) -> None:
        """Initialize a new instance."""
        # Flag to indicate the state.
        self._active = False

    @abstractmethod
    def predict(self, request: Request) -> RenderingTypePrediction:
        """Get `RenderingTypePrediction` based on the input request.

        Args:
            request: `Request` instance for which the prediction is made.
        """

    @abstractmethod
    def store_result(self, request: Request, rendering_type: RenderingType) -> None:
        """Store prediction results and retrain the model.

        Args:
            request: Used request.
            rendering_type: Known suitable `RenderingType`.
        """

    async def initialize(self) -> None:
        """Initialize additional resources required for the predictor operation."""
        if self._active:
            raise RuntimeError(f'The {self.__class__.__name__} is already active.')
        self._active = True

    async def clear(self) -> None:
        """Clear and release additional resources used by the predictor."""
        if not self._active:
            raise RuntimeError(f'The {self.__class__.__name__} is not active.')
        self._active = False

    async def __aenter__(self) -> RenderingTypePredictor:
        """Initialize the predictor upon entering the context manager."""
        await self.initialize()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        """Clear the predictor upon exiting the context manager."""
        await self.clear()


@docs_group('Other')
class DefaultRenderingTypePredictor(RenderingTypePredictor):
    """Stores rendering type for previously crawled URLs and predicts the rendering type for unvisited urls.

    `RenderingTypePredictor` implementation based on logistic regression:
    https://scikit-learn.org/stable/modules/generated/sklearn.linear_model.LogisticRegression.html
    """

    def __init__(
        self,
        detection_ratio: float = 0.1,
        *,
        persistence_enabled: bool = False,
        persist_state_key: str = 'rendering-type-predictor-state',
    ) -> None:
        """Initialize a new instance.

        Args:
            detection_ratio: A number between 0 and 1 that determines the desired ratio of rendering type detections.
            persist_state_key: Key in the key-value storage where the predictor state will be saved.
            If None, defaults to 'rendering-type-predictor-state'.
            persistence_enabled: Whether to persist the trained model and its detection results for reuse.

        """
        super().__init__()

        self._detection_ratio = max(0, min(1, detection_ratio))

        self._state = RecoverableState(
            default_state=RenderingTypePredictorState(model=LogisticRegression(max_iter=1000)),
            persist_state_key=persist_state_key,
            persistence_enabled=persistence_enabled,
            logger=logger,
        )

    @override
    async def initialize(self) -> None:
        """Get current state of the predictor."""
        await super().initialize()

        if not self._state.is_initialized:
            await self._state.initialize()

    @override
    async def clear(self) -> None:
        """Clear the predictor state."""
        await super().clear()

        if self._state.is_initialized:
            await self._state.teardown()

    @override
    def predict(self, request: Request) -> RenderingTypePrediction:
        """Get `RenderingTypePrediction` based on the input request.

        Args:
            request: `Request` instance for which the prediction is made.
        """
        similarity_threshold = 0.1  #  Prediction probability difference threshold to consider prediction unreliable.
        label = request.label or ''

        # Check that the model has already been fitted.
        if hasattr(self._state.current_value.model, 'coef_'):
            url_feature = self._calculate_feature_vector(get_url_components(request.url), label)
            # Are both calls expensive?
            prediction = self._state.current_value.model.predict([url_feature])[0]
            probability = self._state.current_value.model.predict_proba([url_feature])[0]

            if abs(probability[0] - probability[1]) < similarity_threshold:
                # Prediction not reliable.
                detection_probability_recommendation = 1.0
            else:
                detection_probability_recommendation = self._detection_ratio
                # Increase recommendation for labels with fewer than four detection results.
                detection_probability_recommendation *= max(1, 5 - self._count_detection_results(label))

            return RenderingTypePrediction(
                rendering_type=('client only', 'static')[int(prediction)],
                detection_probability_recommendation=detection_probability_recommendation,
            )
        # No data available yet.
        return RenderingTypePrediction(rendering_type='client only', detection_probability_recommendation=1)

    @override
    def store_result(self, request: Request, rendering_type: RenderingType) -> None:
        """Store prediction results and retrain the model.

        Args:
            request: Used `Request` instance.
            rendering_type: Known suitable `RenderingType` for the used `Request` instance.
        """
        label = request.label or ''
        urls_by_label = self._state.current_value.detection_results.setdefault(rendering_type, {})
        urls_by_label.setdefault(label, []).append(get_url_components(request.url))
        self._retrain()

    def _count_detection_results(self, label: str) -> int:
        detection_results = self._state.current_value.detection_results
        return sum(len(urls_by_label.get(label, [])) for urls_by_label in detection_results.values())

    def _retrain(self) -> None:
        x: list[FeatureVector] = [(0, 1), (1, 0)]
        y: list[float] = [0, 1]

        for rendering_type, urls_by_label in self._state.current_value.detection_results.items():
            encoded_rendering_type = 1 if rendering_type == 'static' else 0
            for label, urls in urls_by_label.items():
                for url_components in urls:
                    x.append(self._calculate_feature_vector(url_components, label))
                    y.append(encoded_rendering_type)

        self._state.current_value.model.fit(x, y)

    def _calculate_mean_similarity(self, url: UrlComponents, label: str, rendering_type: RenderingType) -> float:
        known_urls = self._state.current_value.detection_results.get(rendering_type, {}).get(label, [])
        if not known_urls:
            return 0
        return mean(calculate_url_similarity(url, known_url_components) for known_url_components in known_urls)

    def _calculate_feature_vector(self, url: UrlComponents, label: str) -> tuple[float, float]:
        return (
            self._calculate_mean_similarity(url, label, 'static'),
            self._calculate_mean_similarity(url, label, 'client only'),
        )


def get_url_components(url: str) -> UrlComponents:
    """Get list of url components where first component is host name."""
    parsed_url = urlparse(url)
    if parsed_url.path:
        return [parsed_url.netloc, *parsed_url.path.strip('/').split('/')]
    return [parsed_url.netloc]


def calculate_url_similarity(url_1: UrlComponents, url_2: UrlComponents) -> float:
    """Calculate url similarity based on host name and path components similarity.

    Return 0 if different host names.
    Compare path components using Jaro-Winkler method and assign 1 or 0 value based on similarity_cutoff for each
    path component. Return their weighted average.
    """
    # Anything with Jaro-Winkler similarity less than this value is considered completely different,
    # otherwise considered the same.
    similarity_cutoff = 0.8

    if (url_1[0] != url_2[0]) or not url_1 or not url_2:
        return 0
    if url_1 == url_2:
        return 1

    # Each additional path component from longer path is compared to empty string.
    return mean(
        1 if JaroWinkler.similarity(path_1, path_2) > similarity_cutoff else 0
        for path_1, path_2 in zip_longest(url_1[1:], url_2[1:], fillvalue='')
    )
