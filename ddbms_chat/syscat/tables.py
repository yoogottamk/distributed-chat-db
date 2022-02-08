from ddbms_chat.models.syscat import Table
from ddbms_chat.utils import PyQL


TABLES = PyQL(
    [
        Table(id=1, name="user"),
        Table(id=2, name="group"),
        Table(id=3, name="message"),
        Table(id=4, name="group_member"),
    ]
)
