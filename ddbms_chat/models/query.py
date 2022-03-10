from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Union


@dataclass
class Condition:
    lhs: str
    op: str
    rhs: str

    def __hash__(self) -> int:
        return hash((self.__class__.__name__, self.lhs, self.op, self.rhs))

    def __str__(self) -> str:
        html_op_map = {
            "<": "&lt;",
            ">": "&gt;",
            "<=": "&lte;=",
            ">=": "&gte;=",
        }
        return f"({self.lhs} {html_op_map.get(self.op, self.op)} {self.rhs})"


@dataclass
class ConditionOr:
    conditions: List[Union[Condition, ConditionOr, ConditionAnd]]

    def __hash__(self) -> int:
        return hash((self.__class__.__name__, *self.conditions))

    def __str__(self) -> str:
        return f"({' || '.join(map(str, self.conditions))})"


@dataclass
class ConditionAnd:
    conditions: List[Union[Condition, ConditionOr, ConditionAnd]]

    def __hash__(self) -> int:
        return hash(tuple(self.__class__.__name__, *self.conditions))

    def __str__(self) -> str:
        return f"({' && '.join(map(str, self.conditions))})"


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

    def __hash__(self) -> int:
        return hash(
            (
                self.__class__.__name__,
                tuple(self.columns),
                tuple(self.tables),
                self.where,
                None if self.group_by is None else tuple(self.group_by),
                self.having,
                self.limit,
            )
        )
