from crawlee import Request
from crawlee._types import RequestHandlerRunResult
from crawlee.crawlers import (
    AdaptivePlaywrightCrawler,
    RenderingType,
    RenderingTypePrediction,
    RenderingTypePredictor,
)


class CustomRenderingTypePredictor(RenderingTypePredictor):
    def __init__(self) -> None:
        self._learning_data = list[tuple[Request, RenderingType]]()

    def predict(self, request: Request) -> RenderingTypePrediction:
        # Some custom logic that produces some `RenderingTypePrediction`
        # based on the `request` input.
        rendering_type: RenderingType = (
            'static' if 'abc' in request.url else 'client only'
        )

        return RenderingTypePrediction(
            #  Recommends `static` rendering type -> HTTP-based sub crawler will be used.
            rendering_type=rendering_type,
            # Recommends that both sub crawlers should run with 20% chance. When both sub
            # crawlers are running, the predictor can compare results and learn.
            # High number means that predictor is not very confident about the
            # `rendering_type`, low number means that predictor is very confident.
            detection_probability_recommendation=0.2,
        )

    def store_result(self, request: Request, rendering_type: RenderingType) -> None:
        # This function allows predictor to store new learning data and retrain itself
        # if needed. `request` is input for prediction and `rendering_type` is the correct
        # prediction.
        self._learning_data.append((request, rendering_type))
        # retrain


def result_checker(result: RequestHandlerRunResult) -> bool:
    # Some function that inspects produced `result` and returns `True` if the result
    # is correct.
    return bool(result)  # Check something on result


def result_comparator(
    result_1: RequestHandlerRunResult, result_2: RequestHandlerRunResult
) -> bool:
    # Some function that inspects two results and returns `True` if they are
    # considered equivalent. It is used when comparing results produced by HTTP-based
    # sub crawler and playwright based sub crawler.
    return (
        result_1.push_data_calls == result_2.push_data_calls
    )  #  For example compare `push_data` calls.


crawler = AdaptivePlaywrightCrawler.with_parsel_static_parser(
    rendering_type_predictor=CustomRenderingTypePredictor(),
    result_checker=result_checker,
    result_comparator=result_comparator,
)
