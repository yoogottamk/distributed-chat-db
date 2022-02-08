from ddbms_chat.phase1.db import recreate_db
from ddbms_chat.phase1.tables import setup_tables, fill_tables
from ddbms_chat.syscat.sites import SITES
from ddbms_chat.utils import DBConnection

message_function_map = [
    ("Creating database", recreate_db, False),
    ("Creating system catalog tables", setup_tables, True),
    ("Filling system catalog tables", fill_tables, True),
]

for message, func, connect_db in message_function_map:
    print("-" * (len(message) + 4))
    print(f"| {message} |")
    print("-" * (len(message) + 4))
    for site in SITES:
        print(f"Working at site {site.id}")
        try:
            with DBConnection(site, connect_db=connect_db) as cursor:
                func(cursor)
            print(f"Successful at site {site.id}")
        except Exception as e:
            print(f"Failed at site {site.id}, {e}")
