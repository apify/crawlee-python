from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from random import choice
from typing import TYPE_CHECKING, Literal

from typing_extensions import override

if TYPE_CHECKING:
    from crawlee import Request

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


class RandomRenderingTypePredictor(RenderingTypePredictor):
    # Dummy version of predictor. Proper version will be implemented in another change.

    @override
    def predict(self, request: Request) -> RenderingTypePrediction:  # Will be implemented later
        return RenderingTypePrediction(choice(['static', 'client only']), 0.1)

    @override
    def store_result(self, request: Request, crawl_type: RenderingType) -> None:
        pass
