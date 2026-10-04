"""Provider-independent metadata contracts and an offline fake reader."""

from .reader import FakeMetadataReader, MetadataReader, TableNotFoundError
from .types import TableRef, TableSnapshot

__all__ = [
    "FakeMetadataReader",
    "MetadataReader",
    "TableNotFoundError",
    "TableRef",
    "TableSnapshot",
]
