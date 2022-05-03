import atexit
import readline

from rich.pretty import pprint

from ddbms_chat.config import HOSTNAME, PROJECT_ROOT
from ddbms_chat.phase2.parser import parse_select, parse_sql
from ddbms_chat.phase2.query_tree import build_query_tree
from ddbms_chat.phase2.syscat import read_syscat
from ddbms_chat.phase3.execution_planner import execute_plan, plan_execution

history_file = PROJECT_ROOT / ".history"
history_file.touch()

newline = "\n"
qid = f"q{history_file.read_text().count(newline)}"

_, _, _, syscat_sites, _ = read_syscat()
sites = syscat_sites.where(name=HOSTNAME)

if len(sites) > 0:
    CURRENT_SITE = sites[0]
    qid += f"s{CURRENT_SITE.id}"

readline.read_history_file(history_file)

while True:
    try:
        query_str = input("Enter select query: ")
        parsed_query = parse_sql(query_str)
        select_query = parse_select(parsed_query)
        pprint(select_query, expand_all=True)
        qt = build_query_tree(select_query)

        execution_plan = plan_execution(qt, qid)
        execute_plan(execution_plan, qid)
    except EOFError:
        break
    except Exception as e:
        print(e)

atexit.register(readline.write_history_file, history_file)
