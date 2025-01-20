from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Literal, override

from sklearn.linear_model import LogisticRegression
from urllib.parse import urlparse

import jaro

from crawlee import Request

X = [ [f"{i}"] for i in range(0,100)]
y = ["a"] *50 + ["b"] *50
clf = LogisticRegression(random_state=0).fit(X, y)
point = [[49],]
print(clf.predict(point))
print(clf.predict_proba(point))


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


def get_url_components(url: str) -> tuple:
    urlparse
