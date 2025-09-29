from __future__ import annotations

from http import HTTPStatus
from typing import Any, Dict, Optional, Union

from pydantic import BaseModel, Field, model_validator


class StatusCode(BaseModel):
    """Structured HTTP status code representation."""

    code: int = Field(
        ..., ge=100, le=599, description="Numeric HTTP status code value",
    )
    phrase: str = Field(
        ..., description="Associated reason phrase for the status code",
    )
    description: Optional[str] = Field(
        default=None,
        description="Optional human-readable elaboration of the status",
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce(cls, value: Any) -> Dict[str, Any]:
        if isinstance(value, cls):
            return value.model_dump()

        if isinstance(value, HTTPStatus):
            return {"code": value.value, "phrase": value.phrase}

        if isinstance(value, int):
            try:
                status = HTTPStatus(value)
            except ValueError as exc:
                raise ValueError(f"Invalid HTTP status code: {value}") from exc
            return {"code": status.value, "phrase": status.phrase}

        if isinstance(value, dict):
            data = value.copy()
            code = data.get("code")
            if code is None:
                raise ValueError("StatusCode requires a 'code' field")

            try:
                status = HTTPStatus(code)
            except ValueError as exc:
                raise ValueError(f"Invalid HTTP status code: {code}") from exc

            data.setdefault("phrase", status.phrase)
            return data

        raise TypeError(
            f"StatusCode cannot be constructed from value of type {type(value).__name__}",
        )


StatusCodeInput = Union[StatusCode, HTTPStatus, int, Dict[str, Any]]


class APIResponse(BaseModel):
    """Minimal response envelope used by the API."""

    success: bool = Field(..., description="Indicates whether the request succeeded")
    status_code: StatusCode = Field(
        ..., description="Structured HTTP status information returned to the client",
    )
    message: str = Field(..., description="Short description of the outcome")
    context: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Optional payload containing additional response data",
    )

    @classmethod
    def ok(
        cls,
        status_code: StatusCodeInput = HTTPStatus.OK,
        message: str = "success",
        context: Optional[Dict[str, Any]] = None,
    ) -> "APIResponse":
        status = StatusCode.model_validate(status_code)
        return cls(success=True, status_code=status, message=message, context=context)

    @classmethod
    def fail(
        cls,
        status_code: StatusCodeInput,
        message: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> "APIResponse":
        status = StatusCode.model_validate(status_code)
        return cls(success=False, status_code=status, message=message, context=context)
