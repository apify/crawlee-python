# ruff: noqa: N802


def patch_browserforge() -> None:
    """Patches `browserforge` to use data from `apify_fingerprint_datapoints`.

    This avoids import time or runtime file downloads.
    """
    # Temporary fix until https://github.com/daijro/browserforge/pull/29 is merged
    from pathlib import Path

    import apify_fingerprint_datapoints
    from browserforge import download

    download.DATA_DIRS: dict[str, Path] = {  # type:ignore[misc]
        'headers': apify_fingerprint_datapoints.get_header_network().parent,
        'fingerprints': apify_fingerprint_datapoints.get_fingerprint_network().parent,
    }

    def DownloadIfNotExists(**flags: bool) -> None:
        pass

    download.DownloadIfNotExists = DownloadIfNotExists

    import browserforge.bayesian_network

    class BayesianNetwork(browserforge.bayesian_network.BayesianNetwork):
        def __init__(self, path: Path) -> None:
            """Inverted mapping as browserforge expects somewhat renamed file names."""
            if path.name in download.DATA_FILES['headers']:
                path = download.DATA_DIRS['headers'] / download.DATA_FILES['headers'][path.name]
            else:
                path = download.DATA_DIRS['fingerprints'] / download.DATA_FILES['fingerprints'][path.name]
            super().__init__(path)

    browserforge.bayesian_network.BayesianNetwork = BayesianNetwork  # type:ignore[misc]
    import browserforge.headers.generator

    browserforge.headers.generator.DATA_DIR = download.DATA_DIRS['headers']
    import browserforge.fingerprints.generator

    browserforge.headers.generator.DATA_DIR = download.DATA_DIRS['fingerprints']
