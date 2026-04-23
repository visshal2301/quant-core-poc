"""Shared model definitions for repo-style pipeline decomposition."""

from dataclasses import dataclass
from typing import List, Optional


@dataclass(frozen=True)
class BronzeTableSpec:
    dataset: str
    table_name: str
    expected_grain: str
    is_dimension: bool = False


@dataclass(frozen=True)
class DimensionSpec:
    table_name: str
    business_key: str
    surrogate_key: str
    tracked_columns: List[str]
    source_columns: List[str]
    scd2_enabled: bool = True


@dataclass(frozen=True)
class FactSpec:
    table_name: str
    event_date_column: str
    partition_column: str
    natural_keys: List[str]
    numeric_columns: List[str]
    reference_keys: List[str]
    extra_columns: Optional[List[str]] = None

