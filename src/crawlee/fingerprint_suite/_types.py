from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field

from crawlee._utils.docs import docs_group

SupportedOperatingSystems = Literal['windows', 'macos', 'linux', 'android', 'ios']
SupportedDevices = Literal['desktop', 'mobile']
SupportedHttpVersion = Literal['1', '2']
SupportedBrowserType = Literal['chrome', 'firefox', 'safari', 'edge']


@docs_group('Data structures')
class ScreenOptions(BaseModel):
    model_config = ConfigDict(extra='forbid', populate_by_name=True)

    """Defines the screen constrains for the fingerprint generator."""

    min_width: Annotated[float | None, Field(alias='minWidth')] = None
    """Minimal screen width constraint for the fingerprint generator."""

    max_width: Annotated[float | None, Field(alias='maxWidth')] = None
    """Maximal screen width constraint for the fingerprint generator."""

    min_height: Annotated[float | None, Field(alias='minHeight')] = None
    """Minimal screen height constraint for the fingerprint generator."""

    max_height: Annotated[float | None, Field(alias='maxHeight')] = None
    """Maximal screen height constraint for the fingerprint generator."""


@docs_group('Data structures')
class HeaderGeneratorOptions(BaseModel):
    """Collection of header related attributes that can be used by the fingerprint generator."""

    model_config = ConfigDict(extra='forbid', populate_by_name=True)

    browsers: list[SupportedBrowserType] | None = None
    """List of BrowserSpecifications to generate the headers for."""

    operating_systems: Annotated[list[SupportedOperatingSystems] | None, Field(alias='operatingSystems')] = None
    """List of operating systems to generate the headers for."""

    devices: list[SupportedDevices] | None = None
    """List of devices to generate the headers for."""

    locales: list[str] | None = None
    """List of at most 10 languages to include in the [Accept-Language]
    (https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Accept-Language) request header
    in the language format accepted by that header, for example `en`, `en-US` or `de`."""

    http_version: Annotated[SupportedHttpVersion | None, Field(alias='httpVersion')] = None
    """HTTP version to be used for header generation (the headers differ depending on the version)."""

    strict: bool | None = None
    """If true, the generator will throw an error if it cannot generate headers based on the input."""
