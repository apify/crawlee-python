import pytest

from crawlee.fingerprint_suite._browserforge_adapter import FingerprintGenerator as bf_FingerprintGenerator
from crawlee.fingerprint_suite._fingerprint_generator import AbstractFingerprintGenerator


@pytest.mark.skip(reason="Injector not implemented yet so we use browserforge injector.")
@pytest.mark.parametrize("fingerprint_generator",[
                pytest.param(bf_FingerprintGenerator, id="Browserforge"),
])
def test_fingerprint_generator_has_default(fingerprint_generator: AbstractFingerprintGenerator):
    """Test that header generator can work without any options."""
    assert fingerprint_generator.generate()

