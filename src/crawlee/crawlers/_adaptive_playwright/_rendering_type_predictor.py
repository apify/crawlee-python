from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass
from itertools import zip_longest
from statistics import mean
from typing import Literal
from urllib.parse import urlparse

from jaro import jaro_winkler_metric
from sklearn.linear_model import LogisticRegression
from typing_extensions import override

from crawlee import Request
from crawlee._utils.docs import docs_group

UrlComponents = list[str]
RenderingType = Literal['static', 'client only']
FeatureVector = tuple[float, float]


@docs_group('Data structures')
@dataclass(frozen=True)
class RenderingTypePrediction:
    """Rendering type recommendation with detection probability recommendation."""

    rendering_type: RenderingType
    """Recommended rendering type."""
    detection_probability_recommendation: float
    """Recommended rendering detection probability. Expected values between 0-1.

    Zero represents absolute confidence in `rendering_type` recommendation.
    One represents no confidence in `rendering_type` recommendation."""


@docs_group('Classes')
class RenderingTypePredictor(ABC):
    """Stores rendering type for previously crawled URLs and predicts the rendering type for unvisited urls."""

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


@docs_group('Classes')
class DefaultRenderingTypePredictor(RenderingTypePredictor):
    """Stores rendering type for previously crawled URLs and predicts the rendering type for unvisited urls.

    `RenderingTypePredictor` implementation based on logistic regression:
    https://scikit-learn.org/stable/modules/generated/sklearn.linear_model.LogisticRegression.html
    """

    def __init__(self, detection_ratio: float = 0.1) -> None:
        """A default constructor.

        Args:
            detection_ratio: A number between 0 and 1 that determines the desired ratio of rendering type detections.
        """
        self._rendering_type_detection_results: dict[RenderingType, dict[str, list[UrlComponents]]] = {
            'static': defaultdict(list),
            'client only': defaultdict(list),
        }
        self._model = LogisticRegression(max_iter=1000)
        self._detection_ratio = max(0, min(1, detection_ratio))

        # Used to increase detection probability recommendation for initial recommendations of each label.
        # Reaches 1 (no additional increase) after n samples of specific label is already present in
        # `self._rendering_type_detection_results`.
        n = 3
        self._labels_coefficients: dict[str, float] = defaultdict(lambda: n + 2)

    @override
    def predict(self, request: Request) -> RenderingTypePrediction:
        """Get `RenderingTypePrediction` based on the input request.

        Args:
            request: `Request` instance for which the prediction is made.
        """
        similarity_threshold = 0.1  #  Prediction probability difference threshold to consider prediction unreliable.
        label = request.label or ''

        if self._rendering_type_detection_results['static'] or self._rendering_type_detection_results['client only']:
            url_feature = self._calculate_feature_vector(get_url_components(request.url), label)
            # Are both calls expensive?
            prediction = self._model.predict([url_feature])[0]
            probability = self._model.predict_proba([url_feature])[0]

            if abs(probability[0] - probability[1]) < similarity_threshold:
                # Prediction not reliable.
                detection_probability_recommendation = 1.0
            else:
                detection_probability_recommendation = self._detection_ratio
                # Increase recommendation for uncommon labels.
                detection_probability_recommendation *= self._labels_coefficients[label]

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
        self._rendering_type_detection_results[rendering_type][label].append(get_url_components(request.url))
        if self._labels_coefficients[label] > 1:
            self._labels_coefficients[label] -= 1
        self._retrain()

    def _retrain(self) -> None:
        x: list[FeatureVector] = [(0, 1), (1, 0)]
        y: list[float] = [0, 1]

        for rendering_type, urls_by_label in self._rendering_type_detection_results.items():
            encoded_rendering_type = 1 if rendering_type == 'static' else 0
            for label, urls in urls_by_label.items():
                for url_components in urls:
                    x.append(self._calculate_feature_vector(url_components, label))
                    y.append(encoded_rendering_type)

        self._model.fit(x, y)

    def _calculate_mean_similarity(self, url: UrlComponents, label: str, rendering_type: RenderingType) -> float:
        if not self._rendering_type_detection_results[rendering_type][label]:
            return 0
        return mean(
            calculate_url_similarity(url, known_url_components)
            for known_url_components in self._rendering_type_detection_results[rendering_type][label]
        )

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
    Compare path components using jaro-wrinkler method and assign 1 or 0 value based on similarity_cutoff for each
    path component. Return their weighted average.
    """
    # Anything with jaro_winkler_metric less than this value is considered completely different,
    # otherwise considered the same.
    similarity_cutoff = 0.8

    if (url_1[0] != url_2[0]) or not url_1 or not url_2:
        return 0
    if url_1 == url_2:
        return 1

    # Each additional path component from longer path is compared to empty string.
    return mean(
        1 if jaro_winkler_metric(path_1, path_2) > similarity_cutoff else 0
        for path_1, path_2 in zip_longest(url_1[1:], url_2[1:], fillvalue='')
    )
