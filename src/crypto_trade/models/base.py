"""Base model interfaces for M1-M4.

Keep this small until M1 is working end-to-end.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any


class BaseModel(ABC):
    """Minimal interface expected from research models."""

    @abstractmethod
    def fit(self, data: Any) -> "BaseModel":
        raise NotImplementedError

    @abstractmethod
    def predict(self, features: Any) -> Any:
        raise NotImplementedError

    @abstractmethod
    def save(self, path: Path) -> None:
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def load(cls, path: Path) -> "BaseModel":
        raise NotImplementedError