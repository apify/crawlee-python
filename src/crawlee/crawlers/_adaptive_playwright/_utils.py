from typing import Any

import numpy as np
from sklearn.linear_model import LogisticRegression


def sklearn_model_validator(v: LogisticRegression | dict[str, Any]) -> LogisticRegression:
    if isinstance(v, LogisticRegression):
        return v

    model = LogisticRegression(max_iter=1000)
    if v.get('is_fitted', False):
        model.coef_ = np.array(v['coef'])
        model.intercept_ = np.array(v['intercept'])
        model.classes_ = np.array(v['classes'])
        model.n_iter_ = np.array(v.get('n_iter', [1000]))

    return model


def sklearn_model_serializer(model: LogisticRegression) -> dict[str, Any]:
    if hasattr(model, 'coef_'):
        return {
            'coef': model.coef_.tolist(),
            'intercept': model.intercept_.tolist(),
            'classes': model.classes_.tolist(),
            'n_iter': model.n_iter_.tolist() if hasattr(model, 'n_iter_') else [1000],
            'is_fitted': True,
            'max_iter': model.max_iter,
            'solver': model.solver,
        }
    return {'is_fitted': False, 'max_iter': model.max_iter, 'solver': model.solver}
