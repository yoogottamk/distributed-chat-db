from __future__ import annotations

from abc import ABC
from typing import List, Union

from ddbms_chat.models.query import Condition, ConditionAnd, ConditionOr


class TreeNode(ABC):
    def __init__(self):
        # kept it in case some common init is needed
        pass


class SelectionNode(TreeNode):
    def __init__(self, condition: Union[Condition, ConditionAnd, ConditionOr]):
        super().__init__()
        self.condition = condition

    def __str__(self) -> str:
        return f"<Select {self.condition}>"

    def __hash__(self) -> int:
        return hash((self.__class__.__name__, self.condition))


class ProjectionNode(TreeNode):
    def __init__(self, columns: List[str]):
        super().__init__()
        self.columns = columns

    def __str__(self) -> str:
        return f"<Project {self.columns}>"

    def __hash__(self) -> int:
        return hash(tuple(self.__class__.__name__, *self.columns))


class JoinNode(TreeNode):
    def __init__(self, join_condition: Condition):
        super().__init__()
        self.join_condition = join_condition

    def __str__(self) -> str:
        return f"<Join {self.join_condition}>"

    def __hash__(self) -> int:
        return hash((self.__class__.__name__, self.join_condition))


class UnionNode(TreeNode):
    def __init__(self):
        super().__init__()

    def __str__(self) -> str:
        return "<Union>"

    def __hash__(self) -> int:
        return super().__hash__()


class RelationNode(TreeNode):
    def __init__(self, name: str):
        super().__init__()
        self.name = name
        self.is_localized: bool = False

    def __str__(self) -> str:
        return f"<{self.name}{' *' if not self.is_localized else ''}>"

    def __hash__(self) -> int:
        return hash((self.__class__.__name__, self.name, self.is_localized))
