from ddbms_chat.models.syscat import Site
from ddbms_chat.utils import PyQL

SITES = PyQL(
    [
        Site(id=1, name="n1", ip="172.20.0.2", user="user", password="password"),
        Site(id=2, name="n2", ip="172.20.0.3", user="user", password="password"),
        Site(id=3, name="n3", ip="172.20.0.4", user="user", password="password"),
        Site(id=4, name="n4", ip="172.20.0.5", user="user", password="password"),
    ]
)
