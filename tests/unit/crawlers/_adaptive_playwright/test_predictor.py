from dataclasses import dataclass

import pytest

from crawlee import Request
from crawlee.crawlers._adaptive_playwright._rendering_type_predictor import LogisticalRegressionPredictor, \
    calculate_similarity


def test_predictor():
    # store some results and get predictions
    predictor = LogisticalRegressionPredictor()

    label_relevant_learning_data = (
        # Label "static for sure" should be enough for the predictor to predict "static"
        ("http://www.aaa.com", "static for sure", "static"),
        ("http://www.aab.com", "static for sure", "static"),
        ("http://www.aac.com", "static for sure", "static"),
        # Label "browser for sure" should be enough for the predictor to predict "static"
        ("http://www.ddd.com", "browser for sure", "browser"),
        ("http://www.dde.com", "browser for sure", "browser"),
        ("http://www.ddf.com", "browser for sure", "browser"),
    )

    for learning_data in label_relevant_learning_data:
        predictor.store_result(Request.from_url(url="http://www.aaa.com", label="static for sure"), rendering_type=learning_data[2])

    assert predictor.predict("http://www.aaa.com", "static for sure") == "static"
    assert predictor.predict("http://www.ijk.com", "static for sure") == "static"
    assert predictor.predict("http://www.ddd.com", "browser for sure") == "browser"
    assert predictor.predict("http://www.ijk.com", "browser for sure") == "browser"


@pytest.mark.parametrize(("url_1", "url_2", "expected_rounded_metric"), [
    ("https://docs.python.org/3/library/itertools.html#itertools.zip_longest",
     "https://docs.python.org/3.7/library/itertools.html#itertools.zip_longest", 0.67),
    ("https://differente.com/same", "https://differenta.com/same", 0),
    ("https://different.com/almost_the_same", "https://different.com/almost_the_sama", 1)
])
def test_url_similarity(url_1: str, url_2: str, expected_rounded_metric: float):
    assert round(calculate_similarity(url_1=url_1, url_2=url_2),2) == expected_rounded_metric
