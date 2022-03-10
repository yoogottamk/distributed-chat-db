from typing import Dict, List, Tuple, Union

import sqlparse
import sqlvalidator
from sqlparse.sql import (
    Comparison,
    Identifier,
    IdentifierList,
    Parenthesis,
    Statement,
    Token,
    Where,
)
from sqlparse.tokens import Punctuation

from ddbms_chat.models.query import Condition, ConditionAnd, ConditionOr, SelectQuery
from ddbms_chat.utils import debug_log


def parse_sql(sql: str) -> Statement:
    parsed_query = sqlparse.parse(
        sqlparse.format(
            sql,
            keyword_case="upper",
            strip_comments=True,
            reindent_aligned=True,
        )
    )[0]

    # if not sqlvalidator.parse(str(parsed_query)).is_valid():
    #     # TODO: raise warning or something
    #     pass

    return parsed_query


def _resolve_column_alias(column_name: str, table_alias_map: Dict[str, str]):
    """
    expand column alias name to their fullname

    select p.b from project p;
    p.b -> project.b
    """
    if "." not in column_name:
        return column_name

    table_alias, column_name = column_name.split(".", 2)

    return f"{table_alias_map[table_alias]}.{column_name.strip('`')}"


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
    return Condition(
        _resolve_column_alias(token.left.value, table_alias_map).strip("`"),
        operation_token.value,
        _resolve_column_alias(token.right.value, table_alias_map).strip("`"),
    )


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
            token: IdentifierList
            assert token.is_group, "Invalid query?"

            columns += list(
                map(lambda x: x.value.strip("`"), (token.get_identifiers()))
            )
            prev_keyword = None
            continue

        # expecting list of tables
        if prev_keyword.value == "FROM":
            token: IdentifierList

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
        column_names,
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
