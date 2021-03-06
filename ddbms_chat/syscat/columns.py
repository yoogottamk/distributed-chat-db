from ddbms_chat.models.syscat import Column
from ddbms_chat.syscat.tables import TABLES
from ddbms_chat.utils import PyQL

# user table columns

COLUMNS = PyQL(
    [
        Column(
            id=1,
            name="id",
            table=TABLES[0],
            type="int",
            pk=1,
            notnull=1,
            unique=1,
        ),
        Column(
            id=2,
            name="name",
            table=TABLES[0],
            type="str",
            pk=0,
            notnull=1,
            unique=0,
        ),
        Column(
            id=3,
            name="username",
            table=TABLES[0],
            type="str",
            pk=0,
            notnull=1,
            unique=1,
        ),
        Column(
            id=4,
            name="phone",
            table=TABLES[0],
            type="str",
            pk=0,
            notnull=1,
            unique=1,
        ),
        Column(
            id=5,
            name="email",
            table=TABLES[0],
            type="str",
            pk=0,
            notnull=1,
            unique=1,
        ),
        Column(
            id=6,
            name="status",
            table=TABLES[0],
            type="str",
            pk=0,
            notnull=1,
            unique=0,
        ),
        Column(
            id=7,
            name="last_seen",
            table=TABLES[0],
            type="datetime",
            pk=0,
            notnull=1,
            unique=0,
        ),
    ]
    + [  # group table columns
        Column(
            id=8,
            name="id",
            table=TABLES[1],
            type="int",
            pk=1,
            notnull=1,
            unique=1,
        ),
        Column(
            id=9,
            name="gname",
            table=TABLES[1],
            type="str",
            pk=0,
            notnull=1,
            unique=0,
        ),
        Column(
            id=10,
            name="created_by",
            table=TABLES[1],
            type="User",
            pk=0,
            notnull=1,
            unique=0,
        ),
    ]
    + [  # message table columns
        Column(
            id=11,
            name="id",
            table=TABLES[2],
            type="int",
            pk=1,
            notnull=1,
            unique=1,
        ),
        Column(
            id=12,
            name="mgroup",
            table=TABLES[2],
            type="Group",
            pk=0,
            notnull=1,
            unique=0,
        ),
        Column(
            id=13,
            name="author",
            table=TABLES[2],
            type="User",
            pk=0,
            notnull=1,
            unique=0,
        ),
        Column(
            id=14,
            name="content",
            table=TABLES[2],
            type="str",
            pk=0,
            notnull=1,
            unique=0,
        ),
        Column(
            id=15,
            name="sent_at",
            table=TABLES[2],
            type="datetime",
            pk=0,
            notnull=1,
            unique=0,
        ),
    ]
    + [  # group member table columns
        Column(
            id=16,
            name="group",
            table=TABLES[3],
            type="Group",
            pk=1,
            notnull=1,
            unique=0,
        ),
        Column(
            id=17,
            name="user",
            table=TABLES[3],
            type="User",
            pk=1,
            notnull=1,
            unique=0,
        ),
    ]
)
