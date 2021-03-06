from ddbms_chat.phase2.syscat import read_syscat
from ddbms_chat.phase3.utils import send_request_to_site
from ddbms_chat.utils import debug_log

syscat_allocation, _, syscat_fragment, _, syscat_table = read_syscat()

tx_log_file = open("tx-coordinator.log", "w+")


def tx_2pc(update_sql: str, query_id: str):
    split_sql = update_sql.strip().split()
    relation_name = split_sql[1].strip("`")
    table_id = syscat_table.where(name=relation_name)[0].id
    fragments = syscat_fragment.where(table=table_id)

    debug_log("Found %s fragments: %s", len(fragments), fragments.items)

    tx_log_file.write(f"{query_id}: begin_commit\n")

    responses = []
    for frag in fragments:
        sql = " ".join([split_sql[0], f"`{frag.name}`", *split_sql[2:]])
        site = syscat_allocation.where(fragment=frag.id)[0].site

        try:
            r = send_request_to_site(
                site, "post", "/2pc/prepare", json={"sql": sql, "txid": query_id}
            )
            if not r.ok:
                debug_log("Failed in prepare\n%s", (r.reason,))
                raise ValueError("Request failed")
            debug_log("Got response %s", (r.text,))
            responses.append(r.text)
        except Exception as e:
            print(e)
            tx_log_file.write(f"{query_id}: abort\n")
            tx_log_file.write(f"{query_id}: end_of_transaction\n")
            return

    for frag in fragments:
        site = syscat_allocation.where(fragment=frag.id)[0].site

        if not all(x == "vote-commit" for x in responses):
            debug_log("Global abort, not all did vote-commit")
            try:
                r = send_request_to_site(
                    site, "post", "/2pc/global-abort", json={"txid": query_id}
                )
                if not r.ok:
                    debug_log("global-abort failed\n%s", (r.reason,))
                    raise ValueError("Request failed")
            except Exception as e:
                debug_log("aborted")
                tx_log_file.write(f"{query_id}: abort\n")
                tx_log_file.write(f"{query_id}: end_of_transaction\n")
                print(e)
                return
        else:
            debug_log("Global commit")
            try:
                r = send_request_to_site(
                    site, "post", "/2pc/global-commit", json={"txid": query_id}
                )
                if not r.ok:
                    raise ValueError("Request failed")
            except Exception as e:
                tx_log_file.write(f"{query_id}: failed\n")
                tx_log_file.write(f"{query_id}: end_of_transaction\n")
                print(e)
                return
