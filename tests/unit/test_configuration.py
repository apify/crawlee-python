from crawlee.configuration import Configuration


def test_global_configuration_works() -> None:
    assert Configuration.get_global_configuration() is Configuration.get_global_configuration()
