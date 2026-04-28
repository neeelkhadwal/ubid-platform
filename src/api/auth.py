"""
API-key authentication and role-based authorization.

API keys are loaded from the API_KEYS env var (JSON object), e.g.:

    API_KEYS={"abc123":{"name":"alice","role":"admin"}}

Roles (least → most privileged):
    viewer    — read non-PII fields; PAN/GSTIN are masked.
    reviewer  — viewer rights + see unmasked PII + submit review decisions.
    admin     — reviewer rights + merge/unmerge and anchor mutations.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from fastapi import Depends, Header, HTTPException, status

from src.config import settings

ROLE_VIEWER = "viewer"
ROLE_REVIEWER = "reviewer"
ROLE_ADMIN = "admin"

_LEVEL = {ROLE_VIEWER: 0, ROLE_REVIEWER: 1, ROLE_ADMIN: 2}


@dataclass(frozen=True)
class Principal:
    name: str
    role: str

    @property
    def can_view_pii(self) -> bool:
        return _LEVEL.get(self.role, -1) >= _LEVEL[ROLE_REVIEWER]


def authenticate(
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
) -> Principal:
    keys = settings.api_keys or {}
    if not keys:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="API authentication is not configured (set API_KEYS).",
        )
    if not x_api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing X-API-Key header",
            headers={"WWW-Authenticate": "ApiKey"},
        )
    info = keys.get(x_api_key)
    if not isinstance(info, dict) or "role" not in info:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
        )
    role = info["role"]
    if role not in _LEVEL:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"API key has unknown role {role!r}",
        )
    return Principal(name=str(info.get("name") or "unknown"), role=role)


def require_role(minimum: str):
    if minimum not in _LEVEL:
        raise ValueError(f"Unknown role: {minimum}")
    minimum_level = _LEVEL[minimum]

    def dep(principal: Principal = Depends(authenticate)) -> Principal:
        if _LEVEL[principal.role] < minimum_level:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"This action requires '{minimum}' role or higher",
            )
        return principal

    return dep


def mask_identifier(value: Optional[str]) -> Optional[str]:
    """Mask a PAN/GSTIN-style identifier, preserving first 2 and last 2 chars."""
    if not value:
        return value
    if len(value) <= 4:
        return "*" * len(value)
    return f"{value[:2]}{'*' * (len(value) - 4)}{value[-2:]}"
