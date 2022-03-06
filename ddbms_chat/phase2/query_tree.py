from rich.pretty import pprint
from sqlparse.sql import Token

from ddbms_chat.phase2.parser import parse_select, parse_sql
from ddbms_chat.utils import inspect_object


def build_naive_query_tree(sql_query: str):
    parsed_query = parse_sql(sql_query)
    print(parsed_query)

    pprint(parse_select(parsed_query))


if __name__ == "__main__":
    # test_query = (
    #     "SELECT P.Pnumber, P.Dnum, E.Lname, E.Address, E.Bdate "
    #     "FROM PROJECT P, DEPARTMENT D, EMPLOYEE E "
    #     "WHERE P.Dnum=D.Dnumber AND D.Mgr_ssn=E.Ssn AND P.Plocation = 'Stafford'"
    # )
    test_query = (
        "SELECT P.Pnumber, P.Dnum, E.Lname, E.Address, E.Bdate "
        "FROM PROJECT P, EMPLOYEE E "
        "INNER JOIN DEPARTMENT D ON P.Dnum = D.Dnumber "
        "WHERE (D.Mgr_ssn = E.Ssn OR D.mgr_ssn % 3 = 1) AND P.Plocation = 'Stafford' "
        "GROUP BY P.Pnumber, P.Dnum "
        "HAVING P.Pnumber > 5 OR P.Dnum < 3 "
        "LIMIT 10"
    )
    build_naive_query_tree(test_query)
