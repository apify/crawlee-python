from __future__ import annotations

from typing import Generic

from typing_extensions import TypeVar

from crawlee._types import BasicCrawlingContext

__all__ = [
    'ContextPipelineFinalizationError',
    'ContextPipelineInitializationError',
    'ContextPipelineInterruptedError',
    'HttpStatusCodeError',
    'ProxyError',
    'RequestHandlerError',
    'SessionError',
    'UserDefinedErrorHandlerError',
    'ServiceConflictError',
]

TCrawlingContext = TypeVar('TCrawlingContext', bound=BasicCrawlingContext, default=BasicCrawlingContext)


class UserDefinedErrorHandlerError(Exception):
    """Wraps an exception thrown from an user-defined error handler."""


class SessionError(Exception):
    """Errors of `SessionError` type will trigger a session rotation.

    This error doesn't respect the `max_request_retries` option and has a separate limit of `max_session_rotations`.
    """


class ProxyError(SessionError):
    """Raised when a proxy is being blocked or malfunctions."""


class HttpStatusCodeError(Exception):
    """Raised when the response status code indicates an error."""


class RequestHandlerError(Exception, Generic[TCrawlingContext]):
    """Wraps an exception thrown from a request handler (router) and extends it with crawling context."""

    def __init__(self, wrapped_exception: Exception, crawling_context: TCrawlingContext) -> None:
        self.wrapped_exception = wrapped_exception
        self.crawling_context = crawling_context


class ContextPipelineInitializationError(Exception):
    """Wraps an exception thrown in the initialization step of a context pipeline middleware.

    We may not have the complete context at this point, so only `BasicCrawlingContext` is provided.
    """

    def __init__(self, wrapped_exception: Exception, crawling_context: BasicCrawlingContext) -> None:
        self.wrapped_exception = wrapped_exception
        self.crawling_context = crawling_context


class ContextPipelineFinalizationError(Exception):
    """Wraps an exception thrown in the finalization step of a context pipeline middleware.

    We may not have the complete context at this point, so only `BasicCrawlingContext` is provided.
    """

    def __init__(self, wrapped_exception: Exception, crawling_context: BasicCrawlingContext) -> None:
        self.wrapped_exception = wrapped_exception
        self.crawling_context = crawling_context


class ContextPipelineInterruptedError(Exception):
    """May be thrown in the initialization phase of a middleware to signal that the request should not be processed."""


class ServiceConflictError(RuntimeError):
    """Thrown when a service container is getting reconfigured."""

    def __init__(self, service_name: str, new_value: object, old_value: object) -> None:
        super().__init__(
            f"Service '{service_name}' was already set (existing value is '{old_value}', new value is '{new_value}')."
        )
