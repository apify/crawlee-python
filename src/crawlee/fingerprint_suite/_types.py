from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel

SupportedOperatingSystems = Literal['windows', 'macos', 'linux', 'android', 'ios']
SupportedDevices = Literal['desktop', 'mobile']
SupportedHttpVersion = Literal['1', '2']
SupportedBrowserType = Literal['chrome', 'firefox', 'safari', 'edge']


class ScreenOptions(BaseModel):
    model_config = ConfigDict(extra='forbid', validate_by_name=True, validate_by_alias=True, alias_generator=to_camel)

    """Defines the screen constrains for the fingerprint generator."""

    min_width: float | None = None
    """Minimal screen width constraint for the fingerprint generator."""

    max_width: float | None = None
    """Maximal screen width constraint for the fingerprint generator."""

    min_height: float | None = None
    """Minimal screen height constraint for the fingerprint generator."""

    max_height: float | None = None
    """Maximal screen height constraint for the fingerprint generator."""


class HeaderGeneratorOptions(BaseModel):
    """Collection of header related attributes that can be used by the fingerprint generator."""

    model_config = ConfigDict(extra='forbid', validate_by_name=True, validate_by_alias=True, alias_generator=to_camel)

    browsers: list[SupportedBrowserType] | None = None
    """List of BrowserSpecifications to generate the headers for."""

    operating_systems: list[SupportedOperatingSystems] | None = None
    """List of operating systems to generate the headers for."""

    devices: list[SupportedDevices] | None = None
    """List of devices to generate the headers for."""

    locales: list[str] | None = None
    """List of at most 10 languages to include in the [Accept-Language]
    (https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Accept-Language) request header
    in the language format accepted by that header, for example `en`, `en-US` or `de`."""

    http_version: SupportedHttpVersion | None = None
    """HTTP version to be used for header generation (the headers differ depending on the version)."""

    strict: bool | None = None
    """If true, the generator will throw an error if it cannot generate headers based on the input."""
