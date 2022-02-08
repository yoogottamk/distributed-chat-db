from __future__ import annotations

from dataclasses import dataclass


@dataclass
class Site:
    id: int
    name: str
    ip: str
    user: str
    password: str

    def __eq__(self, o):
        return (type(self) == type(o)) and (self.id == o.id)


@dataclass
class Table:
    id: int
    name: str
    fragment_type: str

    def __eq__(self, o):
        return (type(self) == type(o)) and (self.id == o.id)


@dataclass
class Column:
    id: int
    name: str
    table: Table
    type: str
    pk: int
    notnull: int
    unique: int

    def __eq__(self, o):
        return (type(self) == type(o)) and (self.id == o.id)


@dataclass
class Fragment:
    id: int
    name: str
    logic: str
    parent: int
    table: Table

    def __eq__(self, o):
        return (type(self) == type(o)) and (self.id == o.id)


@dataclass
class Allocation:
    fragment: Fragment
    site: Site

    def __eq__(self, o):
        return (
            (type(self) == type(o))
            and (self.fragment == o.fragment)
            and (self.site == o.site)
        )
