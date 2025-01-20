from dataclasses import dataclass

from crawlee import Request
from crawlee.crawlers._adaptive_playwright._rendering_type_predictor import LogisticalRegressionPredictor


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
