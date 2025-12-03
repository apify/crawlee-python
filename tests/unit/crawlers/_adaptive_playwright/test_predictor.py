from __future__ import annotations

import pytest

from crawlee import Request
from crawlee.crawlers._adaptive_playwright._rendering_type_predictor import (
    DefaultRenderingTypePredictor,
    RenderingType,
    calculate_url_similarity,
    get_url_components,
)
from crawlee.storages import KeyValueStore


@pytest.mark.parametrize('label', ['some label', None])
@pytest.mark.parametrize(
    ('url', 'expected_prediction'),
    [
        ('http://www.aaa.com/some/stuff/extra', 'static'),
        ('http://www.aab.com/some/otherstuff', 'static'),
        ('http://www.aac.com/some', 'static'),
        ('http://www.ddd.com/some/stuff/extra', 'client only'),
        ('http://www.dde.com/some/otherstuff', 'client only'),
        ('http://www.ddf.com/some', 'client only'),
    ],
)
async def test_predictor_same_label(url: str, expected_prediction: RenderingType, label: str | None) -> None:
    async with DefaultRenderingTypePredictor() as predictor:
        learning_inputs: tuple[tuple[str, RenderingType], ...] = (
            ('http://www.aaa.com/some/stuff', 'static'),
            ('http://www.aab.com/some/stuff', 'static'),
            ('http://www.aac.com/some/stuff', 'static'),
            ('http://www.ddd.com/some/stuff', 'client only'),
            ('http://www.dde.com/some/stuff', 'client only'),
            ('http://www.ddf.com/some/stuff', 'client only'),
        )

        # Learn from small set
        for learned_url, rendering_type in learning_inputs:
            predictor.store_result(Request.from_url(url=learned_url, label=label), rendering_type=rendering_type)

        assert predictor.predict(Request.from_url(url=url, label=label)).rendering_type == expected_prediction


async def test_predictor_new_label_increased_detection_probability_recommendation() -> None:
    """Test that urls of uncommon labels have increased detection recommendation.

    This increase should gradually drop as the predictor learns more data with this label."""
    detection_ratio = 0.01
    label = 'some label'
    async with DefaultRenderingTypePredictor(detection_ratio=detection_ratio) as predictor:
        # Learn first prediction of this label
        predictor.store_result(
            Request.from_url(url='http://www.aaa.com/some/stuff', label=label), rendering_type='static'
        )
        # Increased detection_probability_recommendation
        prediction = predictor.predict(Request.from_url(url='http://www.aaa.com/some/stuffa', label=label))
        assert prediction.rendering_type == 'static'
        assert prediction.detection_probability_recommendation == detection_ratio * 4

        # Learn second prediction of this label
        predictor.store_result(
            Request.from_url(url='http://www.aaa.com/some/stuffe', label=label), rendering_type='static'
        )
        # Increased detection_probability_recommendation
        prediction = predictor.predict(Request.from_url(url='http://www.aaa.com/some/stuffa', label=label))
        assert prediction.rendering_type == 'static'
        assert prediction.detection_probability_recommendation == detection_ratio * 3

        # Learn third prediction of this label
        predictor.store_result(
            Request.from_url(url='http://www.aaa.com/some/stuffi', label=label), rendering_type='static'
        )
        # Increased detection_probability_recommendation
        prediction = predictor.predict(Request.from_url(url='http://www.aaa.com/some/stuffa', label=label))
        assert prediction.rendering_type == 'static'
        assert prediction.detection_probability_recommendation == detection_ratio * 2

        # Learn fourth prediction of this label.
        predictor.store_result(
            Request.from_url(url='http://www.aaa.com/some/stuffo', label=label), rendering_type='static'
        )
        # Label considered stable now. There should be no increase of detection_probability_recommendation.
        prediction = predictor.predict(Request.from_url(url='http://www.aaa.com/some/stuffa', label=label))
        assert prediction.rendering_type == 'static'
        assert prediction.detection_probability_recommendation == detection_ratio


async def test_unreliable_prediction() -> None:
    """Test that detection_probability_recommendation for unreliable predictions is 1.

    Create situation where no learning data of new label is available for the predictor.
    It's first prediction is not reliable as both options have 50% chance, so it should set maximum
    detection_probability_recommendation."""
    learnt_label = 'some label'

    async with DefaultRenderingTypePredictor() as predictor:
        # Learn two predictions of some label. One of each to make predictor very uncertain.
        predictor.store_result(
            Request.from_url(url='http://www.aaa.com/some/stuff', label=learnt_label), rendering_type='static'
        )
        predictor.store_result(
            Request.from_url(url='http://www.aaa.com/some/otherstuff', label=learnt_label), rendering_type='client only'
        )

        # Predict for new label. Predictor does not have enough information to give any reliable guess and should make
        # it clear by setting detection_probability_recommendation=1
        probability = predictor.predict(
            Request.from_url(url='http://www.unknown.com', label='new label')
        ).detection_probability_recommendation
        assert probability == 1


async def test_no_learning_data_prediction() -> None:
    """Test that predictor can predict even if it never learnt anything before.

    It should give some prediction, but it has to set detection_probability_recommendation=1"""
    async with DefaultRenderingTypePredictor() as predictor:
        probability = predictor.predict(
            Request.from_url(url='http://www.unknown.com', label='new label')
        ).detection_probability_recommendation

        assert probability == 1


async def test_persistent_no_learning_data_prediction() -> None:
    """Test that the model is saved after initialisation in KeyValueStore."""
    persist_key = 'test-no_learning-state'
    async with DefaultRenderingTypePredictor(persistence_enabled=True, persist_state_key=persist_key) as _predictor:
        pass

    kvs = await KeyValueStore.open()

    persisted_data = await kvs.get_value(persist_key)

    assert persisted_data is not None
    assert persisted_data['model']['is_fitted'] is False


async def test_persistent_prediction() -> None:
    """Test that the model and resources is saved after train in KeyValueStore."""
    persist_key = 'test-persistent-state'
    async with DefaultRenderingTypePredictor(persistence_enabled=True, persist_state_key=persist_key) as predictor:
        # Learn some data
        predictor.store_result(
            Request.from_url(url='http://www.aaa.com/some/stuff', label='some label'), rendering_type='static'
        )

    kvs = await KeyValueStore.open()

    persisted_data = await kvs.get_value(persist_key)

    assert persisted_data is not None
    assert persisted_data['model']['is_fitted'] is True


@pytest.mark.parametrize(
    ('persistence_enabled', 'same_result'),
    [
        pytest.param(True, True, id='with persistence'),
        pytest.param(False, False, id='without persistence'),
    ],
)
async def test_persistent_prediction_recovery(*, persistence_enabled: bool, same_result: bool) -> None:
    """Test that the model and resources is recovered from KeyValueStore."""
    persist_key = 'test-persistent-state-recovery'

    async with DefaultRenderingTypePredictor(
        detection_ratio=0.01, persistence_enabled=persistence_enabled, persist_state_key=persist_key
    ) as predictor:
        # Learn some data
        predictor.store_result(
            Request.from_url(url='http://www.aaa.com/some/stuff', label='some label'), rendering_type='static'
        )
        before_recover_prediction = predictor.predict(
            Request.from_url(url='http://www.aaa.com/some/stuff', label='some label')
        )

    # Recover predictor
    async with DefaultRenderingTypePredictor(
        detection_ratio=0.01, persistence_enabled=True, persist_state_key=persist_key
    ) as recover_predictor:
        after_recover_prediction = recover_predictor.predict(
            Request.from_url(url='http://www.aaa.com/some/stuff', label='some label')
        )

    # If persistence is enabled, the predicted results must be the same.
    if same_result:
        assert (
            before_recover_prediction.detection_probability_recommendation
            == after_recover_prediction.detection_probability_recommendation
        )
    else:
        assert (
            before_recover_prediction.detection_probability_recommendation
            != after_recover_prediction.detection_probability_recommendation
        )


@pytest.mark.parametrize(
    ('url_1', 'url_2', 'expected_rounded_similarity'),
    [
        (
            'https://docs.python.org/3/library/itertools.html#itertools.zip_longest',
            'https://docs.python.org/3.7/library/itertools.html#itertools.zip_longest',
            0.67,
        ),
        ('https://differente.com/same', 'https://differenta.com/same', 0),
        ('https://same.com/almost_the_same', 'https://same.com/almost_the_sama', 1),
        ('https://same.com/same/extra', 'https://same.com/same', 0.5),
    ],
)
def test_url_similarity(url_1: str, url_2: str, expected_rounded_similarity: float) -> None:
    assert (
        round(calculate_url_similarity(url_1=get_url_components(url_1), url_2=get_url_components(url_2)), 2)
        == expected_rounded_similarity
    )
