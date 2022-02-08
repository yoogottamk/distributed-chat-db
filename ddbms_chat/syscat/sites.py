from ddbms_chat.models.syscat import Site
from ddbms_chat.utils import PyQL


SITES = PyQL(
    [
        Site(id=1, name="cp5", ip="10.3.5.211", user="root", password="iiit123"),
        Site(id=2, name="cp6", ip="10.3.5.208", user="root", password="iiit123"),
        Site(id=3, name="cp7", ip="10.3.5.204", user="root", password="iiit123"),
        Site(id=4, name="cp8", ip="10.3.5.205", user="root", password="iiit123"),
    ]
)
