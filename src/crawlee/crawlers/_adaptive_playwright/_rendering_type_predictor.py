from abc import ABC, abstractmethod
from dataclasses import dataclass
from itertools import zip_longest
from typing import Literal, override

from jaro import jaro_winkler_metric
from sklearn.linear_model import LogisticRegression
from urllib.parse import urlparse

import jaro
jaro.jaro_winkler_metric(u'SHACKLEFORD', u'SHACKELFORD')

from crawlee import Request


"""
X = [ [f"{i}"] for i in range(0,100)]
y = ["a"] *50 + ["b"] *50
clf = LogisticRegression(random_state=0).fit(X, y)
point = [[49],]
print(clf.predict(point))
print(clf.predict_proba(point))
"""

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
        self._inputs = []
        self._outputs = []
        self._model = LogisticRegression(random_state=0)

    @override
    def predict(self, request: Request) -> RenderingTypePrediction:
        self._model.predict([request.url, request.label])

    @override
    def store_result(self, request: Request, rendering_type: RenderingType) -> None:
        self._inputs.append([request.url, request.label])
        self._outputs.append(rendering_type)
        self._model.fit(self._inputs, self._outputs)


def calculate_similarity(url_1: str, url_2: str) -> float:
    """Calculate url similarity.

    Return 0 if different host names
    Compare path components using jaro-wrinkler method and assign 1 or 0 value based on similarity_cutoff for each
    path component. Return their weighted average."""

    similarity_cutoff = 0.8 # Anything not at least similar with this value is considered completely different.
    parsed_url_1 = urlparse(url_1)
    parsed_url_2 = urlparse(url_2)


    if parsed_url_1.netloc != parsed_url_2.netloc:
        return 0

    url_1_paths = parsed_url_1.path.strip("/").split("/")
    url_2_paths = parsed_url_2.path.strip("/").split("/")

    similarity_sum = 0

    for path_1, path_2 in zip_longest(url_1_paths, url_2_paths, fillvalue=""):
        similarity_sum += 1 if jaro_winkler_metric(path_1, path_2) > similarity_cutoff else 0

    return similarity_sum / max(len(url_1_paths), len(url_2_paths))


print(calculate_similarity("https://docs.python.org/3/library/itertools.html#itertools.zip_longest",
                           "https://docs.python.org/3.7/library/itertools.html#itertools.zip_longest"))
