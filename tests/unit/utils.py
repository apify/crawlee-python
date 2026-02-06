import sys

import pytest

run_alone_on_mac = pytest.mark.run_alone if sys.platform == 'darwin' else lambda x: x
