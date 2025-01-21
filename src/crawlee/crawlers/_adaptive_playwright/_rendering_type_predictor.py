from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass
from itertools import zip_longest
from statistics import mean
from typing import Literal, override
from urllib.parse import urlparse

import jaro
from jaro import jaro_winkler_metric
from sklearn.linear_model import LogisticRegression

jaro.jaro_winkler_metric('SHACKLEFORD', 'SHACKELFORD')

from crawlee import Request

"""
X = [ [f"{i}"] for i in range(0,100)]
y = ["a"] *50 + ["b"] *50
clf = LogisticRegression(random_state=0).fit(X, y)
point = [[49],]
print(clf.predict(point))
print(clf.predict_proba(point))
"""
UrlComponents = list[str]
RenderingType = Literal['static', 'client only']


@dataclass(frozen=True)
class RenderingTypePrediction:
    rendering_type: RenderingType
    detection_probability_recommendation: float




class RenderingTypePredictor(ABC):
    @abstractmethod
    def predict(self, request: Request) -> RenderingTypePrediction: ...

    @abstractmethod
    def store_result(self, request: Request, crawl_type: RenderingType) -> None: ...


class LogisticalRegressionPredictor(RenderingTypePredictor):

    def __init__(self):
        self._rendering_type_detection_results: dict[RenderingType, dict[str, UrlComponents]] = {
            'static': defaultdict(list[str]),
            'client only': defaultdict(list[str]),
        }
        self._inputs = []
        self._outputs = []
        self._model = LogisticRegression(random_state=0)

    @override
    def predict(self, request: Request) -> RenderingTypePrediction:
        url_feature =''
        #self._model.predict([request.url, request.label])

    @override
    def store_result(self, request: Request, rendering_type: RenderingType) -> None:
        self._rendering_type_detection_results[rendering_type][request.label].append(get_url_components(request.url))
        #self._model.fit(self._inputs, self._outputs)

    def _get_mean_similarity(self, url: str, label: str, rendering_type: RenderingType) -> float:
        return mean(
            calculate_url_similarity(get_url_components(url), known_url_components)
            for known_url_components in self._rendering_type_detection_results[rendering_type][label]
        )

    def _calculate_feature_vector(self, request: Request) -> (float, float):
        return (
            self._get_mean_similarity(request.url, request.label, 'static'),
            self._get_mean_similarity(request.url, request.label, 'client only')
        )





def get_url_components(url: str) -> UrlComponents:
    """Get list of url components where first component is host name."""
    parsed_url = urlparse(url)
    return [parsed_url.netloc, *parsed_url.path.strip('/').split('/')]

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

    # Each additional path component from longer path is replaced by empty string in the shorter path.
    return mean(1 if jaro_winkler_metric(path_1, path_2) > similarity_cutoff else 0 for
         path_1, path_2 in zip_longest(url_1[1:], url_2[1:], fillvalue=''))
