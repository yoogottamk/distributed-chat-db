from __future__ import annotations

from abc import ABC
from typing import List, Optional, Union
from uuid import uuid4

from ddbms_chat.models.query import Condition, ConditionAnd, ConditionOr


class TreeNode(ABC):
    def __init__(self):
        # for unique hasing
        self._uuid: int = uuid4().int

    def __repr__(self):
        return str(self)


class SelectionNode(TreeNode):
    def __init__(self, condition: Union[Condition, ConditionAnd, ConditionOr]):
        super().__init__()
        self.condition = condition

    def __str__(self) -> str:
        return f"<Select {self.condition}>"

    def __hash__(self) -> int:
        return hash((self._uuid, self.__class__.__name__, self.condition))


class ProjectionNode(TreeNode):
    def __init__(self, columns: List[str]):
        super().__init__()
        self.columns = columns

    def __str__(self) -> str:
        return f"<Project {self.columns}>"

    def __hash__(self) -> int:
        return hash((self._uuid, self.__class__.__name__, *self.columns))


class JoinNode(TreeNode):
    def __init__(self, condition: Optional[Condition] = None):
        super().__init__()
        self.condition = condition

    def __str__(self) -> str:
        return f"<Join {self.condition if self.condition else '(X)'}>"

    def __hash__(self) -> int:
        return hash((self._uuid, self.__class__.__name__, self.condition))


class UnionNode(TreeNode):
    def __init__(self):
        super().__init__()

    def __str__(self) -> str:
        return "<Union>"

    def __hash__(self) -> int:
        return hash(self._uuid)


class RelationNode(TreeNode):
    def __init__(self, name: str):
        super().__init__()
        self.name = name
        self.is_localized: bool = False
        self.site_id: int = -1

    def __str__(self) -> str:
        return f"<{self.name}{' *' if not self.is_localized else ' @ site ' + str(self.site_id)}>"

    def __hash__(self) -> int:
        return hash(
            (
                self._uuid,
                self.__class__.__name__,
                self.name,
                self.is_localized,
                self.site_id,
            )
        )
