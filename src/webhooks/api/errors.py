"""HTTP error contract: every failure is ``{"error": {"code", "message", "details"}}``."""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from webhooks.service import ConflictError, NotFoundError, ServiceError, ValidationError

# 422 as a literal: starlette renamed HTTP_422_UNPROCESSABLE_ENTITY, and the old
# name warns while the new one is missing on older versions.
HTTP_422_UNPROCESSABLE_CONTENT = 422


class ApiError(Exception):
    """Raised by route handlers for domain failures."""

    def __init__(
        self,
        status_code: int,
        code: str,
        message: str,
        details: Any | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message
        self.details = details


def _error_response(
    status_code: int, code: str, message: str, details: Any | None = None
) -> JSONResponse:
    payload: dict[str, Any] = {"error": {"code": code, "message": message}}
    if details is not None:
        payload["error"]["details"] = details
    return JSONResponse(status_code=status_code, content=payload)


# Domain errors -> HTTP status codes.
_SERVICE_STATUS: dict[type[ServiceError], tuple[int, str]] = {
    NotFoundError: (status.HTTP_404_NOT_FOUND, "not_found"),
    ConflictError: (status.HTTP_409_CONFLICT, "conflict"),
    ValidationError: (status.HTTP_400_BAD_REQUEST, "invalid_request"),
}


def install_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(ApiError)
    async def _api_error(_: Request, exc: ApiError) -> JSONResponse:
        return _error_response(exc.status_code, exc.code, exc.message, exc.details)

    @app.exception_handler(ServiceError)
    async def _service_error(_: Request, exc: ServiceError) -> JSONResponse:
        for error_type, (code, name) in _SERVICE_STATUS.items():
            if isinstance(exc, error_type):
                return _error_response(code, name, str(exc))
        return _error_response(
            status.HTTP_500_INTERNAL_SERVER_ERROR, "internal_error", "internal error"
        )

    @app.exception_handler(RequestValidationError)
    async def _validation_error(_: Request, exc: RequestValidationError) -> JSONResponse:
        return _error_response(
            HTTP_422_UNPROCESSABLE_CONTENT,
            "validation_error",
            "request validation failed",
            details=[
                {"location": list(error.get("loc", [])), "message": error.get("msg", "")}
                for error in exc.errors()
            ],
        )

    @app.exception_handler(StarletteHTTPException)
    async def _http_error(_: Request, exc: StarletteHTTPException) -> JSONResponse:
        return _error_response(exc.status_code, "http_error", str(exc.detail), None)
