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


@dataclass
class Group:
    id: int
    name: str
    created_by: User


@dataclass
class Message:
    id: int
    group: Group
    author: User
    content: str
    sent_at: datetime
