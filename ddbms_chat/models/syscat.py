from __future__ import annotations

from dataclasses import dataclass


@dataclass
class Site:
    id: int
    name: str
    ip: str
    user: str
    password: str


@dataclass
class Table:
    id: int
    name: str


@dataclass
class Column:
    id: int
    name: str
    table: Table
    type: str
    pk: int
    notnull: int
    unique: int


@dataclass
class Fragment:
    id: int
    name: str
    type: str
    logic: str
    parent: int
    table: Table


@dataclass
class Allocation:
    fragment: Fragment
    site: Site
