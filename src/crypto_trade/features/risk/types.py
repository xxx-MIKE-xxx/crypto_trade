"""Dataclasses describing the standardized risk report."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

FeatureRow = Dict[str, Any]


@dataclass
class ReportConfig:
    """Runtime configuration for report generation."""

    timeout: int = 15
    include_raw: bool = True
    defade_api_key: Optional[str] = field(default_factory=lambda: os.getenv("DEFADE_API_KEY"))
    goplus_api_key: Optional[str] = field(default_factory=lambda: os.getenv("GOPLUS_API_KEY"))
    goplus_api_secret: Optional[str] = field(default_factory=lambda: os.getenv("GOPLUS_API_SECRET"))
    jupiter_api_key: Optional[str] = field(default_factory=lambda: os.getenv("JUPITER_API_KEY"))
    rugcheck_api_key: Optional[str] = field(default_factory=lambda: os.getenv("RUGCHECK_API_KEY"))

    @classmethod
    def from_env(cls, timeout: int = 15, include_raw: bool = True) -> "ReportConfig":
        return cls(timeout=timeout, include_raw=include_raw)


@dataclass
class SourceResult:
    """Result wrapper for a single data source fetch."""

    source: str
    attempted: bool
    success: bool
    available: bool
    requires_key: bool
    latency_ms: Optional[float] = None
    http_status: Optional[int] = None
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    data: Any = None
    raw: Any = None

    def to_status_dict(self) -> Dict[str, Any]:
        return {
            "attempted": self.attempted,
            "success": self.success,
            "available": self.available,
            "requires_key": self.requires_key,
            "latency_ms": self.latency_ms,
            "http_status": self.http_status,
            "error_type": self.error_type,
            "error_message": self.error_message,
        }


@dataclass
class StandardRiskReport:
    """Standardized machine-readable risk report."""

    schema_version: str
    generated_at_utc: str
    token: Dict[str, Any]
    overall: Dict[str, Any]
    source_status: Dict[str, Dict[str, Any]]
    categories: Dict[str, Dict[str, Any]]
    feature_row: FeatureRow
    warnings: List[Dict[str, Any]]
    raw: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "generated_at_utc": self.generated_at_utc,
            "token": self.token,
            "overall": self.overall,
            "source_status": self.source_status,
            "categories": self.categories,
            "feature_row": self.feature_row,
            "warnings": self.warnings,
            "raw": self.raw,
        }
