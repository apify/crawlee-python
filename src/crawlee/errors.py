from __future__ import annotations

from typing import Generic

from typing_extensions import TypeVar

from crawlee._types import BasicCrawlingContext
from crawlee._utils.docs import docs_group

__all__ = [
    'ContextPipelineFinalizationError',
    'ContextPipelineInitializationError',
    'ContextPipelineInterruptedError',
    'HttpStatusCodeError',
    'ProxyError',
    'RequestHandlerError',
    'ServiceConflictError',
    'SessionError',
    'UserDefinedErrorHandlerError',
]

TCrawlingContext = TypeVar('TCrawlingContext', bound=BasicCrawlingContext, default=BasicCrawlingContext)


@docs_group('Errors')
class UserDefinedErrorHandlerError(Exception):
    """Wraps an exception thrown from an user-defined error handler."""


@docs_group('Errors')
class SessionError(Exception):
    """Errors of `SessionError` type will trigger a session rotation.

    This error doesn't respect the `max_request_retries` option and has a separate limit of `max_session_rotations`.
    """


@docs_group('Errors')
class ProxyError(SessionError):
    """Raised when a proxy is being blocked or malfunctions."""


@docs_group('Errors')
class HttpStatusCodeError(Exception):
    """Raised when the response status code indicates an error."""

    def __init__(self, message: str, status_code: int) -> None:
        super().__init__(f'{message} (status code: {status_code}).')
        self.status_code = status_code
        self.message = message


@docs_group('Errors')
class RequestHandlerError(Exception, Generic[TCrawlingContext]):
    """Wraps an exception thrown from a request handler (router) and extends it with crawling context."""

    def __init__(self, wrapped_exception: Exception, crawling_context: TCrawlingContext) -> None:
        super().__init__()
        self.wrapped_exception = wrapped_exception
        self.crawling_context = crawling_context


@docs_group('Errors')
class ContextPipelineInitializationError(Exception):
    """Wraps an exception thrown in the initialization step of a context pipeline middleware.

    We may not have the complete context at this point, so only `BasicCrawlingContext` is provided.
    """

    def __init__(self, wrapped_exception: Exception, crawling_context: BasicCrawlingContext) -> None:
        super().__init__()
        self.wrapped_exception = wrapped_exception
        self.crawling_context = crawling_context


@docs_group('Errors')
class ContextPipelineFinalizationError(Exception):
    """Wraps an exception thrown in the finalization step of a context pipeline middleware.

    We may not have the complete context at this point, so only `BasicCrawlingContext` is provided.
    """

    def __init__(self, wrapped_exception: Exception, crawling_context: BasicCrawlingContext) -> None:
        super().__init__()
        self.wrapped_exception = wrapped_exception
        self.crawling_context = crawling_context


@docs_group('Errors')
class ContextPipelineInterruptedError(Exception):
    """May be thrown in the initialization phase of a middleware to signal that the request should not be processed."""


@docs_group('Errors')
class ServiceConflictError(RuntimeError):
    """Thrown when a service container is getting reconfigured."""

    def __init__(self, service_name: str, new_value: object, old_value: object) -> None:
        super().__init__(
            f"Service '{service_name}' was already set (existing value is '{old_value}', new value is '{new_value}')."
        )
