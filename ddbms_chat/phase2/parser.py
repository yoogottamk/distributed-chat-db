from typing import Dict, List, Tuple, Union

import sqlparse
from rich.pretty import pprint
from sqlparse.sql import (
    Comparison,
    Function,
    Identifier,
    IdentifierList,
    Parenthesis,
    Statement,
    Token,
    Where,
)
from sqlparse.tokens import Punctuation

from ddbms_chat.models.query import Condition, ConditionAnd, ConditionOr, SelectQuery
from ddbms_chat.phase2.syscat import read_syscat
from ddbms_chat.utils import debug_log, inspect_object

(
    syscat_allocation,
    syscat_columns,
    syscat_fragments,
    syscat_sites,
    syscat_tables,
) = read_syscat()


def extract_names_from_func_col(func_col: str):
    """
    Extract function name and column name from string

    Example:
        "avg(col)" -> avg, col
    """
    if "(" not in func_col:
        return None, func_col

    return func_col.replace("(", "|").replace(")", "").split("|")[:2]


def add_function_to_column(col_func, col_name):
    """
    add function to column name if function is defined
    """
    return f"{col_func}({col_name})" if col_func is not None else col_name


def parse_sql(sql: str) -> Statement:
    parsed_query = sqlparse.parse(
        sqlparse.format(
            sql,
            keyword_case="upper",
            identifier_case="lower",
            strip_comments=True,
            reindent_aligned=True,
        )
    )[0]

    # if not sqlvalidator.parse(str(parsed_query)).is_valid():
    #     # TODO: raise warning or something
    #     pass

    return parsed_query


def _fill_table_from_syscat(column_names: List[str]) -> List[str]:
    """
    read columns from system catalog and prepend table to column name
    """
    resolved_column_names = []

    for column_name in column_names:
        column_func = None

        if "(" in column_name:
            column_func, column_name = extract_names_from_func_col(column_name)

        if "." in column_name:
            resolved_column_names.append(
                add_function_to_column(
                    column_func,
                    ".".join([name.strip("`") for name in column_name.split(".")]),
                )
            )
            continue

        possible_cols = syscat_columns.where(name=column_name.strip("`"))
        if len(possible_cols) != 1:
            raise ValueError(f"Couldn't identify the relation for column {column_name}")

        resolved_column_names.append(
            add_function_to_column(
                column_func, syscat_tables.where(id=possible_cols[0].id)[0].name
            )
        )

    return resolved_column_names


def _find_relation_for_column(column_name: str, tables: List[str]):
    candidate_tables = []
    for table in tables:
        s_table = syscat_tables.where(name=table)[0]
        if len(syscat_columns.where(name=column_name, table=s_table.id)) == 1:
            candidate_tables.append(table)

    if len(candidate_tables) == 1:
        return candidate_tables[0]

    if len(candidate_tables) > 1:
        raise ValueError(
            f"Available tables {candidate_tables} for column {column_name}"
        )

    raise ValueError(f"No table found for column {column_name}")


def _resolve_column_alias(column_name: str, table_alias_map: Dict[str, str]):
    """
    expand column alias name to their fullname

    select p.b from project p;
    p.b -> project.b
    """
    func_name = None

    if "(" in column_name:
        func_name, column_name = extract_names_from_func_col(column_name)

    if "." not in column_name:
        column_name = column_name.strip("`")
        return add_function_to_column(
            func_name,
            f"{_find_relation_for_column(column_name, list(table_alias_map.values()))}.{column_name}",
        )

    table_alias, column_name = column_name.split(".", 2)

    return add_function_to_column(
        func_name, f"{table_alias_map[table_alias]}.{column_name.strip('`')}"
    )


def _resolve_column_aliases(
    tables: List[Identifier], columns: List[str]
) -> Tuple[Dict[str, str], List[str], List[str]]:
    """
    generate table name alias mapping and expand select column names
    """
    table_alias_map = {}

    # extract table aliases
    for table in tables:
        alias = table.get_alias()
        name = table.get_real_name().strip("`")
        if alias is None:
            table_alias_map[name] = name
        else:
            table_alias_map[alias.strip("`")] = name

    table_names = list(set(table_alias_map.values()))

    # resolve column names
    for i, column in enumerate(columns):
        try:
            columns[i] = _resolve_column_alias(column, table_alias_map)
        except:
            raise ValueError("Unknown table referenced in columns")

    return table_alias_map, table_names, list(set(columns))


def _parse_comparison(token: Comparison, table_alias_map: Dict[str, str]) -> Condition:
    """
    Convert a Comparison (sqlparse) to Condition (own)
    """
    operation_token = list(
        filter(lambda x: x._get_repr_name() == "Comparison", token.tokens)
    )[0]

    try:
        lhs = _resolve_column_alias(token.left.value, table_alias_map).strip("`")
    except:
        lhs = token.left.value

    try:
        rhs = _resolve_column_alias(token.right.value, table_alias_map).strip("`")
    except:
        rhs = token.right.value

    return Condition(lhs, operation_token.value, rhs)


def _parse_condition_list(
    condition_tokens: Union[Parenthesis, Where, List[Token]],
    table_alias_map: Dict[str, str],
) -> Union[Condition, ConditionOr, ConditionAnd]:
    """
    parse conditions listed in where clause

    it is given that the conditions are in CNF
    """
    if type(condition_tokens) is list:
        condition_iter = condition_tokens
    elif condition_tokens._get_repr_name() == "Comparison":
        return _parse_comparison(condition_tokens, table_alias_map)
    else:
        condition_iter = condition_tokens.tokens

    conditions = []
    combiner = Condition

    for token in condition_iter:
        if token.is_whitespace or type(token) is Punctuation:
            continue

        if type(token) is Parenthesis:
            conditions.append(_parse_condition_list(token, table_alias_map))

        if token.value == "AND":
            combiner = ConditionAnd
        elif token.value == "OR":
            combiner = ConditionOr

        if type(token) is Comparison:
            conditions.append(_parse_comparison(token, table_alias_map))

    # single condition
    if combiner is Condition:
        return conditions[0]

    return combiner(conditions)


def _reduce_condition(condition: Union[Condition, ConditionAnd, ConditionOr]):
    """
    reduce ConditionAnd and ConditionOr logically

    eg:
        ConditionAnd([Condition, ConditionAnd, ConditionAnd]) ->
            ConditionAnd([Condition, *ConditionAnd.conditions, *ConditionAnd.conditions])

    basically, unpack conditions of same type within that type

    A && ((B && C) && D) -> A && B && C && D
    """
    # already reduced
    if type(condition) is Condition:
        return condition

    # now only ConditionOr and ConditionAnd left
    # add assert to make type checker happy
    assert (type(condition) is ConditionAnd) or (type(condition) is ConditionOr)

    parent_type = type(condition)
    final_conditions = []

    for child_condition in condition.conditions:
        if type(child_condition) is Condition:
            final_conditions.append(child_condition)
        elif type(child_condition) is parent_type:
            for grandchild_condition in child_condition.conditions:
                final_conditions.append(_reduce_condition(grandchild_condition))
        else:
            final_conditions.append(_reduce_condition(condition))

    return parent_type(final_conditions)


def parse_select(sql: Statement) -> SelectQuery:
    """
    parse select statement

    also convert equijoins to select+where

    Equijoins can be implemented using select and where
    Since this is only supposed to work with equijoins, removing "INNER JOIN ON ..."
    from the query will simplify the internal structure
    """
    if sql.get_type() != "SELECT":
        raise ValueError("Can only translate for select statements")

    # tokens[0] is "SELECT" anyways
    prev_keyword = sql.tokens[0]

    token: Token
    columns = []
    tables = []
    conditions = []
    where_clause = None
    group_by = []
    having_clause = []
    limit = None

    debug_log("parsing token: %r", sql.tokens[0])
    for token in sql.tokens[1:]:
        # ignore whitespace
        if token.is_whitespace:
            continue

        debug_log("parsing token: %r", token)

        # keep track of last encountered keyword
        if token.is_keyword:
            # sqlparse didn't implement groupby/having properly
            # have to add edge cases :(
            if (not (prev_keyword and prev_keyword.value == "HAVING")) or (
                token.value == "LIMIT"
            ):
                prev_keyword = token
                debug_log("keyword^")
                continue

        # special handling for where
        if type(token) is Where:
            where_clause = token
            prev_keyword = token
            continue

        # error?
        if prev_keyword is None:
            raise ValueError(f"Unexpected token {token}")

        # expecting column list
        if prev_keyword.value == "SELECT":
            if token.value == "*":
                columns = ["*"]
            else:
                token: IdentifierList
                assert token.is_group, "Invalid query?"

                def __parse_select_tokens(token):
                    if type(token) is Identifier:
                        return token.value.strip("`")
                    elif type(token) is Function:
                        func_name = token.get_name()
                        arg = list(token.get_parameters())[0].value.strip("`")
                        return f"{func_name}({arg})"

                    return None

                columns = [__parse_select_tokens(token)]
                if columns[0] is None:
                    columns = list(
                        map(__parse_select_tokens, (token.get_identifiers()))
                    )

            prev_keyword = None
            continue

        # expecting list of tables
        if prev_keyword.value == "FROM":
            token: IdentifierList

            if type(token) is Identifier:
                tables.append(token)
            else:
                tables += list(token.get_identifiers())

            prev_keyword = None
            continue

        # joins -> select+where
        if prev_keyword.value in ["INNER JOIN", "JOIN"]:
            tables.append(token)
            prev_keyword = None
            continue

        if prev_keyword.value == "ON":
            assert type(token) is Comparison, "Error in `JOIN ON` condition?"
            conditions.append(token)
            prev_keyword = None
            continue

        # group by and having
        if prev_keyword.value == "GROUP BY":
            group_by += list(token.get_identifiers())
            prev_keyword = None
            continue

        if prev_keyword.value == "HAVING":
            having_clause.append(token)
            continue

        # limit
        if prev_keyword.value == "LIMIT":
            try:
                limit = int(token.value)
            except:
                raise ValueError("LIMIT should be an integer")

    if columns == ["*"]:
        for table in tables:
            s_table = syscat_tables.where(name=table.value.strip("`"))[0]
            columns = [
                f"{s_table.name}.{c.name}"
                for c in syscat_columns.where(table=s_table.id)
            ]

    table_alias_map, table_names, column_names = _resolve_column_aliases(
        tables, columns
    )
    if where_clause:
        conditions.append(where_clause)

    # convert group by clause to compatible SelectQuery
    if len(group_by) > 0:
        group_by = list(
            map(
                lambda col_name: _resolve_column_alias(col_name.value, table_alias_map),
                group_by,
            )
        )
    else:
        group_by = None

    # convert having clause to compatible SelectQuery
    if having_clause:
        having_clause = _parse_condition_list(having_clause, table_alias_map)

        if type(having_clause) is not ConditionAnd:
            having_clause = ConditionAnd([having_clause])
    else:
        having_clause = None

    return SelectQuery(
        _fill_table_from_syscat(column_names),
        table_names,
        _reduce_condition(
            ConditionAnd(
                [_parse_condition_list(cond, table_alias_map) for cond in conditions]
            )
        ),
        group_by,
        having_clause,
        limit,
    )
