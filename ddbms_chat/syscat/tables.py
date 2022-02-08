from ddbms_chat.models.syscat import Table
from ddbms_chat.utils import PyQL


TABLES = PyQL(
    [
        Table(id=1, name="user", fragment_type="V"),
        Table(id=2, name="group", fragment_type="H"),
        Table(id=3, name="message", fragment_type="DH"),
        Table(id=4, name="group_member", fragment_type="-"),
    ]
)
