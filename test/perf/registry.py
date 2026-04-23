"""Workload registry. Workloads register themselves at import time."""
from __future__ import annotations

from typing import Type

from .workloads.base import Workload

_REGISTRY: dict[str, Type[Workload]] = {}


def register(cls: Type[Workload]) -> Type[Workload]:
    if not cls.name:
        raise ValueError(f"workload class {cls.__name__} has no name")
    if cls.name in _REGISTRY:
        raise ValueError(f"duplicate workload name: {cls.name}")
    _REGISTRY[cls.name] = cls
    return cls


def get(name: str) -> Type[Workload]:
    if name not in _REGISTRY:
        raise KeyError(
            f"unknown workload {name!r}; known: {sorted(_REGISTRY)}"
        )
    return _REGISTRY[name]


def all_names() -> list[str]:
    return sorted(_REGISTRY)


def _bootstrap() -> None:
    # Import workload modules so their @register decorators fire.
    from .workloads import cold_pull  # noqa: F401


_bootstrap()
