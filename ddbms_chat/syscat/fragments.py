from ddbms_chat.models.syscat import Fragment
from ddbms_chat.syscat.tables import TABLES
from ddbms_chat.utils import PyQL


FRAGMENTS = PyQL(
    [
        Fragment(
            id=1, name="group_1", type="H", logic="id%4==0", parent=1, table=TABLES[1]
        ),
        Fragment(
            id=2, name="group_2", type="H", logic="id%4==1", parent=2, table=TABLES[1]
        ),
        Fragment(
            id=3, name="group_3", type="H", logic="id%4==2", parent=3, table=TABLES[1]
        ),
        Fragment(
            id=4, name="group_4", type="H", logic="id%4==3", parent=4, table=TABLES[1]
        ),
        Fragment(
            id=5,
            name="message_1",
            type="DH",
            logic="group|><message_1",
            parent=1,
            table=TABLES[2],
        ),
        Fragment(
            id=6,
            name="message_2",
            type="DH",
            logic="group|><message_2",
            parent=2,
            table=TABLES[2],
        ),
        Fragment(
            id=7,
            name="message_3",
            type="DH",
            logic="group|><message_3",
            parent=3,
            table=TABLES[2],
        ),
        Fragment(
            id=8,
            name="message_4",
            type="DH",
            logic="group|><message_4",
            parent=4,
            table=TABLES[2],
        ),
        Fragment(
            id=9,
            name="user_1",
            type="V",
            logic="username,last_seen",
            parent=9,
            table=TABLES[0],
        ),
        Fragment(
            id=10,
            name="user_2",
            type="V",
            logic="name,status",
            parent=10,
            table=TABLES[0],
        ),
        Fragment(
            id=11,
            name="user_3",
            type="V",
            logic="phone,email",
            parent=11,
            table=TABLES[0],
        ),
        Fragment(
            id=12, name="group_members", type="-", logic="", parent=12, table=TABLES[3]
        ),
    ]
)
