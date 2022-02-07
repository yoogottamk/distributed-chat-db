from ddbms_chat.models.syscat import Table


TABLES = [
    Table(id=1, name="User", key="id"),
    Table(id=2, name="Group", key="id"),
    Table(id=3, name="Message", key="id"),
    Table(id=4, name="GroupMember", key="(group,user)"),
]
