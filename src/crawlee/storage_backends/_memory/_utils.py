from __future__ import annotations

import json
from typing import Any


def json_snapshot(value: Any) -> Any:
    """Return an independent copy of `value`, round-tripped through JSON as the file-system backend serializes it.

    A value the file-system backend can't serialize, such as one with a circular reference or a tuple as a dict key,
    raises the same error here. Other objects JSON doesn't support become `str(value)`, and dict keys that aren't
    strings are converted to strings, matching what reading the value back from the file-system backend returns.
    """
    return json.loads(json.dumps(value, ensure_ascii=False, default=str))
