from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Union


@dataclass
class Condition:
    lhs: str
    op: str
    rhs: str


@dataclass
class ConditionOr:
    conditions: List[Union[Condition, ConditionOr, ConditionAnd]]


@dataclass
class ConditionAnd:
    conditions: List[Union[Condition, ConditionOr, ConditionAnd]]


@dataclass
class SelectQuery:
    columns: List[str]
    tables: List[str]
    where: Optional[ConditionAnd] = None
    group_by: Optional[List[str]] = None
    having: Optional[ConditionAnd] = None
    limit: Optional[int] = None

    # order_by is not in the scope
    # hence not parsing
    # order_by: Optional[str] = None
