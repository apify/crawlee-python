from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from random import choice
from typing import Literal

from typing_extensions import override

RenderingType = Literal['static', 'client only']

@dataclass(frozen=True)
class RenderingTypePrediction:
    rendering_type: RenderingType
    detection_probability_recommendation: float



class RenderingTypePredictor(ABC):

    @abstractmethod
    def predict(self, url: str, label: str | None) -> RenderingTypePrediction:
        ...

    @abstractmethod
    def store_result(self, url: str, label: str | None, crawl_type: RenderingType) -> None:
        ...


class DefaultRenderingTypePredictor(RenderingTypePredictor):
    #Dummy version of predictor. Proper version will be implemented in another change.

    @override
    def predict(self, url: str, label: str | None) -> RenderingTypePrediction: # Will be implemented later
        return RenderingTypePrediction(choice(['static', 'client only']), 0.1)

    @override
    def store_result(self, url: str, label: str | None, crawl_type: RenderingType) -> None:
        pass
