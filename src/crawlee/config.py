# ruff: noqa: TCH003 TCH002
from __future__ import annotations

from datetime import timedelta
from typing import Annotated

from pydantic import Field
from pydantic_settings import BaseSettings


class Configuration(BaseSettings):
    """Global Crawlee configuration based on environment variables."""

    internal_timeout: Annotated[timedelta | None, Field(alias='crawlee_internal_timeout')] = None
    verbose_log: Annotated[bool, Field(alias='crawlee_verbose_log')] = False
