# ruff: noqa
def patch_browserforge() -> None:
    """Patches `browserforge` to use data from `apify_fingerprint_datapoints`.

    This avoids import time or runtime file downloads."""

    # Temporary fix until https://github.com/daijro/browserforge/pull/29 is merged
    from pathlib import Path
    from typing import Dict
    import apify_fingerprint_datapoints  # type:ignore[import-untyped]
    from browserforge import download
    from browserforge.download import DATA_FILES

    # Needed to be done before the import of code that does import time download
    download.DATA_DIRS: Dict[str, Path] = {  # type:ignore[misc]
        'headers': apify_fingerprint_datapoints.get_header_network().parent,
        'fingerprints': apify_fingerprint_datapoints.get_fingerprint_network().parent,
    }
    import browserforge.bayesian_network

    class BayesianNetwork(browserforge.bayesian_network.BayesianNetwork):
        def __init__(self, path: Path) -> None:
            if path.name in DATA_FILES['headers']:
                path = download.DATA_DIRS['headers'] / path.name
            else:
                path = download.DATA_DIRS['fingerprints'] / path.name
            super().__init__(path)

    browserforge.bayesian_network.BayesianNetwork = BayesianNetwork  # type:ignore[misc]
    import browserforge.headers.generator

    browserforge.headers.generator.DATA_DIR = download.DATA_DIRS['headers']
    import browserforge.fingerprints.generator

    browserforge.headers.generator.DATA_DIR = download.DATA_DIRS['fingerprints']
