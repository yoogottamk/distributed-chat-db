import atexit
import readline
from secrets import token_hex
from traceback import print_exc

from rich.console import Console
from rich.pretty import pprint
from rich.table import Table

from ddbms_chat.config import HOSTNAME, PROJECT_ROOT
from ddbms_chat.phase2.parser import parse_select, parse_sql
from ddbms_chat.phase2.query_tree import build_query_tree
from ddbms_chat.phase2.syscat import read_syscat
from ddbms_chat.phase3.execution_planner import execute_plan, plan_execution
from ddbms_chat.phase4.utils import tx_2pc

history_file = PROJECT_ROOT / ".history"
history_file.touch()


_, _, _, syscat_sites, _ = read_syscat()
sites = syscat_sites.where(name=HOSTNAME)

if len(sites) > 0:
    CURRENT_SITE = sites[0]
else:
    raise ValueError("Running on a node not in system catalog")

readline.read_history_file(history_file)

while True:
    try:
        qid = f"q{token_hex(3)}s{CURRENT_SITE.id}"
        query_str = input("Enter query: ")
        cmd = query_str.strip().lower().split()[0]
        if cmd == "select":
            parsed_query = parse_sql(query_str)
            select_query = parse_select(parsed_query)
            pprint(select_query, expand_all=True)
            qt = build_query_tree(select_query)

            execution_plan = plan_execution(qt, qid)
            rows = execute_plan(execution_plan, qid, CURRENT_SITE, select_query)
            print(f"{len(rows)} rows fetched")
            if len(rows) > 0:
                table = Table(title=f"Query {qid}")
                for k in rows[0]:
                    table.add_column(k)
                for row in rows:
                    table.add_row(*list(map(str, row.values())))
                console = Console()
                console.print(table)
        elif cmd == "update":
            tx_2pc(query_str)
    except EOFError:
        break
    except Exception as e:
        print_exc()

atexit.register(readline.write_history_file, history_file)
