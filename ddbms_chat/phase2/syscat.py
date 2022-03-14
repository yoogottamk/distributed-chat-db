from typing import Optional, Tuple

from ddbms_chat.models.syscat import Allocation, Column, Fragment, Site, Table
from ddbms_chat.syscat.sites import SITES
from ddbms_chat.utils import DBConnection, PyQL


def read_syscat(site: Optional[Site] = None) -> Tuple[PyQL, PyQL, PyQL, PyQL, PyQL]:
    if site is None:
        site = SITES[0]

    assert site

    with DBConnection(site) as cursor:
        ret = []
        for col_name, col_cls in [
            ("allocation", Allocation),("column", Column),
            ("fragment", Fragment),
            ("site", Site),
            ("table", Table),
        ]:
            cursor.execute(f"select * from `{col_name}`")
            rows = cursor.fetchall()
            ret.append(PyQL([col_cls(**row) for row in rows]))

    return tuple(ret)
