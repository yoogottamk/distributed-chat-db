from ddbms_chat.models.syscat import Fragment
from ddbms_chat.syscat.tables import TABLES
from ddbms_chat.utils import PyQL


FRAGMENTS = PyQL(
    [
        Fragment(
            id=1, name="user_1", logic="username,last_seen", parent=1, table=TABLES[0]
        ),
        Fragment(id=2, name="user_2", logic="name,status", parent=2, table=TABLES[0]),
        Fragment(id=3, name="user_3", logic="phone,email", parent=3, table=TABLES[0]),
        Fragment(id=4, name="group_1", logic="id%4==0", parent=4, table=TABLES[1]),
        Fragment(id=5, name="group_2", logic="id%4==1", parent=5, table=TABLES[1]),
        Fragment(id=6, name="group_3", logic="id%4==2", parent=6, table=TABLES[1]),
        Fragment(id=7, name="group_4", logic="id%4==3", parent=7, table=TABLES[1]),
        Fragment(
            id=8, name="message_1", logic="group|><message_1", parent=4, table=TABLES[2]
        ),
        Fragment(
            id=9, name="message_2", logic="group|><message_2", parent=5, table=TABLES[2]
        ),
        Fragment(
            id=10,
            name="message_3",
            logic="group|><message_3",
            parent=6,
            table=TABLES[2],
        ),
        Fragment(
            id=11,
            name="message_4",
            logic="group|><message_4",
            parent=7,
            table=TABLES[2],
        ),
        Fragment(id=12, name="group_members", logic="", parent=12, table=TABLES[3]),
    ]
)
