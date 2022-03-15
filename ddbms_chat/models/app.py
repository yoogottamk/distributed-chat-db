from dataclasses import dataclass
from datetime import datetime


@dataclass
class User:
    id: int
    name: str
    username: str
    last_seen: datetime
    status: str
    phone: str
    email: str

    def __eq__(self, o):
        return (type(self) == type(o)) and (self.id == o.id)


@dataclass
class Group:
    id: int
    name: str
    created_by: User

    def __eq__(self, o):
        return (type(self) == type(o)) and (self.id == o.id)


@dataclass
class Message:
    id: int
    group: Group
    author: User
    content: str
    sent_at: datetime

    def __eq__(self, o):
        return (type(self) == type(o)) and (self.id == o.id)


@dataclass
class GroupMember:
    group: Group
    user: User

    def __eq__(self, o):
        return (
            (type(self) == type(o))
            and (self.group == o.group)
            and (self.user == o.message)
        )
