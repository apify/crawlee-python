from dataclasses import dataclass
from random import choice
from typing import Literal

CrawlType = Literal['primary', 'secondary']

@dataclass(frozen=True)
class CrawlTypePrediction:
    crawl_type: CrawlType
    detection_probability_recommendation: float


class CrawlTypePredictor:
    #Dummy version of predictor. Proper version will be implemented in another change.

    def predict(self, url: str, label: str | None) -> CrawlTypePrediction:
        return CrawlTypePrediction(choice(['primary', 'secondary']), 0.1)

    def store_result(self, url: str, label: str | None, crawl_type: CrawlType):
        pass
