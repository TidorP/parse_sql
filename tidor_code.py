import json
from typing import Any, Union
import unittest
from run_sql import query_bigquery


####################################################################################################
# HELPER FUNCTIONS
def _parse_date_trunc(field_name: str):
    """
    Checks if the field_name includes date truncation syntax such as __week, __month, or __year.
    Returns (base_field_name, date_grain) if found, else (field_name, None).
    """
    if field_name.endswith("__week"):
        return field_name.replace("__week", ""), "WEEK"
    elif field_name.endswith("__month"):
        return field_name.replace("__month", ""), "MONTH"
    elif field_name.endswith("__year"):
        return field_name.replace("__year", ""), "YEAR"
    else:
        return field_name, None


def _get_definition(
    name: str, definitions: list[dict[str, Any]]
) -> Union[dict[str, Any], None]:
    for definition in definitions:
        if definition["name"] == name:
            return definition
    return None


def _find_join_definition(
    table_a: str, table_b: str, join_defs: list[dict[str, Any]]
) -> Union[dict[str, Any], None]:
    """
    Finds a join definition in the semantic layer that connects table_a and table_b.
    Expects each join def to look like:
      {
         "one": "orders",
         "many": "order_items",
         "join": "order_items.order_id = orders.order_id"
      }
    Returns None if nothing is found.
    """
    for jd in join_defs:
        if jd["one"] == table_a and jd["many"] == table_b:
            return jd
        if jd["one"] == table_b and jd["many"] == table_a:
            return jd
    return None


####################################################################################################
# MAIN FUNCTION (GENERATE SQL from DICTs)

def generate_sql_query(query: dict[str, Any], semantic_layer: dict[str, Any]) -> str:
    """
    Generates a BigQuery SQL string given a query definition and a semantic layer definition.
    """
    metrics_requested = query.get("metrics", [])
    dimensions_requested = query.get("dimensions", [])
    filters_requested = query.get("filters", [])

    # Dictionaries to store metric and dimension definitions
    metrics_definitions = []
    dimensions_definitions = []

    # Set to collect all tables used
    tables_used = set()

    # Collect metric definitions
    for metric_name in metrics_requested:
        metric_def = _get_definition(metric_name, semantic_layer.get("metrics", []))
        if not metric_def:
            raise ValueError(
                f"Metric definition for '{metric_name}' not found in semantic layer."
            )
        metrics_definitions.append(metric_def)
        tables_used.add(metric_def["table"])

    # Collect dimension definitions
    for dim_name in dimensions_requested:
        base_dim_name, date_grain = _parse_date_trunc(dim_name)
        dim_def = _get_definition(base_dim_name, semantic_layer.get("dimensions", []))
        if not dim_def:
            raise ValueError(
                f"Dimension definition for '{dim_name}' not found in semantic layer."
            )
        dimensions_definitions.append((dim_def, date_grain, dim_name))
        tables_used.add(dim_def["table"])

    # Determine if multiple tables are involved
    multiple_tables = len(tables_used) > 1

    # Build metric select statements
    metrics_sql = []
    for metric_def in metrics_definitions:
        sql_expr = metric_def["sql"]
        # Check if the metric's SQL expression contains a function
        if "(" in sql_expr and ")" in sql_expr:
            # Find the function and the column inside it
            func_start = sql_expr.find("(")
            func_end = sql_expr.find(")", func_start)
            function = sql_expr[:func_start].strip()
            column = sql_expr[func_start + 1 : func_end].strip()
            # Fully qualify the column if necessary
            if multiple_tables:
                column = f"{metric_def['table']}.{column}"
            # Reconstruct the SQL expression
            sql_expr = f"{function}({column})"
        else:
            # If no function, just fully qualify the column
            if multiple_tables:
                sql_expr = f"{metric_def['table']}.{sql_expr}"
        metrics_sql.append(f"{sql_expr} AS {metric_def['name']}")

    # Build dimension select statements and group by terms
    dimensions_sql = []
    group_by_terms = []
    for dim_def, date_grain, dim_name in dimensions_definitions:
        sql_expr = dim_def["sql"]
        # Fully qualify the column if multiple tables are involved
        if multiple_tables:
            sql_expr = f"{dim_def['table']}.{sql_expr}"

        if date_grain:
            # e.g., DATE_TRUNC(orders.created_at, WEEK) AS ordered_date__week
            trunc_expr = f"DATE_TRUNC({sql_expr}, {date_grain})"
            dimensions_sql.append(f"{trunc_expr} AS {dim_name}")
            group_by_terms.append(trunc_expr)
        else:
            dimensions_sql.append(f"{sql_expr} AS {dim_name}")
            group_by_terms.append(sql_expr)

    # Combine SELECT items
    select_items = metrics_sql + dimensions_sql
    select_clause = ", ".join(select_items) if select_items else "*"

    # Construct FROM and JOIN clauses
    joins = semantic_layer.get("joins", [])
    from_clause = ""
    if not tables_used:
        raise ValueError(
            "No tables identified from metrics or dimensions. Unable to build query."
        )
    elif len(tables_used) == 1:
        from_clause = f"FROM {list(tables_used)[0]}"
    else:
        # Initialize with the first table
        tables = list(tables_used)
        primary_table = tables[0]
        from_clause = f"FROM {primary_table}"
        for other_table in tables[1:]:
            join_def = _find_join_definition(primary_table, other_table, joins)
            if not join_def:
                # Attempt the reverse join definition
                join_def = _find_join_definition(other_table, primary_table, joins)
            if not join_def:
                raise ValueError(
                    f"No join definition found between {primary_table} and {other_table}."
                )
            from_clause += f"\nJOIN {other_table} ON {join_def['join']}"

    # Build WHERE and HAVING clauses
    where_clauses = []
    having_clauses = []

    for filt in filters_requested:
        field_name = filt["field"]
        operator = filt["operator"]
        value = filt["value"]

        base_field_name, date_grain = _parse_date_trunc(field_name)

        # Determine if the filter is on a metric or dimension
        dim_def = _get_definition(base_field_name, semantic_layer.get("dimensions", []))
        metric_def = _get_definition(base_field_name, semantic_layer.get("metrics", []))

        if dim_def:
            sql_expr = dim_def["sql"]
            # Fully qualify the column if multiple tables are involved
            if multiple_tables:
                sql_expr = f"{dim_def['table']}.{sql_expr}"
            if date_grain:
                sql_expr = f"DATE_TRUNC({sql_expr}, {date_grain})"
            if isinstance(value, str):
                value_expr = f"'{value}'"
            else:
                value_expr = str(value)
            filter_expression = f"{sql_expr} {operator} {value_expr}"
            where_clauses.append(filter_expression)
        elif metric_def:
            sql_expr = metric_def["sql"]
            # Handle functions
            if "(" in sql_expr and ")" in sql_expr:
                func_start = sql_expr.find("(")
                func_end = sql_expr.find(")", func_start)
                function = sql_expr[:func_start].strip()
                column = sql_expr[func_start + 1 : func_end].strip()
                # Fully qualify the column if necessary
                if multiple_tables:
                    column = f"{metric_def['table']}.{column}"
                sql_expr = f"{function}({column})"
            else:
                if multiple_tables:
                    sql_expr = f"{metric_def['table']}.{sql_expr}"
            if isinstance(value, str):
                value_expr = f"'{value}'"
            else:
                value_expr = str(value)
            filter_expression = f"{sql_expr} {operator} {value_expr}"
            having_clauses.append(filter_expression)
        else:
            raise ValueError(
                f"Filter field '{field_name}' not found in dimensions or metrics."
            )

    # Assemble the final SQL query
    sql = f"SELECT {select_clause}\n{from_clause}"

    if where_clauses:
        sql += "\nWHERE " + " AND ".join(where_clauses)

    if group_by_terms:
        sql += "\nGROUP BY " + ", ".join(group_by_terms)

    if having_clauses:
        sql += "\nHAVING " + " AND ".join(having_clauses)

    return sql


#####################################################################################################################
# Note that, I assume you don't change the contents of the test db for this one (hence I also check for actual result)
# Should ideally use sqlparse.format for production test
# ----------- UNIT TESTS ------------
class TestGenerateSqlQuery(unittest.TestCase):
    def test_base_sample(self):
        query_base = {"metrics": ["order_count"]}

        semantic_layer_base = {
            "metrics": [{"name": "order_count", "sql": "COUNT(*)", "table": "orders"}]
        }

        expected_sql_base = "SELECT COUNT(*) AS order_count\n" "FROM orders"

        # SQL match
        actual_sql_base = generate_sql_query(query_base, semantic_layer_base)
        self.assertEqual(actual_sql_base.strip(), expected_sql_base.strip())

        # Result match (only total rows for convenience)
        result = query_bigquery(actual_sql_base)
        self.assertEqual(result.total_rows, 1, "Mismatch on total rows")

    def test_query1(self):
        # Already provided in your snippet
        query = {"metrics": ["total_revenue"]}

        semantic_layer = {
            "metrics": [
                {
                    "name": "total_revenue",
                    "sql": "SUM(sale_price)",
                    "table": "order_items",
                }
            ]
        }

        expected_sql = "SELECT SUM(sale_price) AS total_revenue\n" "FROM order_items"

        # SQL match
        actual_sql = generate_sql_query(query, semantic_layer)
        self.assertEqual(actual_sql.strip(), expected_sql.strip())

        # Result match (only total rows for convenience)
        result = query_bigquery(actual_sql)
        self.assertEqual(result.total_rows, 1, "Mismatch on total rows")

    def test_query2(self):
        """
        Query 2: Give me sales revenue by order item status
        {
                "metrics": ["total_revenue"],
                "dimensions": ["status"]
        }
        """
        query_2 = {"metrics": ["total_revenue"], "dimensions": ["status"]}

        semantic_layer_2 = {
            "metrics": [
                {
                    "name": "total_revenue",
                    "sql": "SUM(sale_price)",
                    "table": "order_items",
                }
            ],
            "dimensions": [{"name": "status", "sql": "status", "table": "order_items"}],
        }

        expected_sql_2 = (
            "SELECT SUM(sale_price) AS total_revenue, status AS status\n"
            "FROM order_items\n"
            "GROUP BY status"
        )

        actual_sql_2 = generate_sql_query(query_2, semantic_layer_2)
        self.assertEqual(actual_sql_2.strip(), expected_sql_2.strip())

        result = query_bigquery(actual_sql_2)
        self.assertEqual(result.total_rows, 5, "Mismatch on total rows")

    def test_query3(self):
        """
        Query 3: Show me the sales revenue for order items with status complete
        {
                "metrics": ["total_revenue"],
                "filters": [
                        {
                                "field": "status",
                                "operator": "=",
                                "value": "Complete"
                        }
                ]
        }
        """
        query_3 = {
            "metrics": ["total_revenue"],
            "filters": [{"field": "status", "operator": "=", "value": "Complete"}],
        }

        semantic_layer_3 = {
            "metrics": [
                {
                    "name": "total_revenue",
                    "sql": "SUM(sale_price)",
                    "table": "order_items",
                }
            ],
            "dimensions": [{"name": "status", "sql": "status", "table": "order_items"}],
        }

        expected_sql_3 = (
            "SELECT SUM(sale_price) AS total_revenue\n"
            "FROM order_items\n"
            "WHERE status = 'Complete'"
        )

        actual_sql_3 = generate_sql_query(query_3, semantic_layer_3)
        self.assertEqual(actual_sql_3.strip(), expected_sql_3.strip())

        result = query_bigquery(actual_sql_3)
        self.assertEqual(result.total_rows, 1, "Mismatch on total rows")

    def test_query4(self):
        """
        Query 4: Show me the number of orders with more than 1 items
        {
                "metrics": ["count_of_orders"],
                "filters": [
                        {
                                "field": "num_of_item",
                                "operator": ">",
                                "value": 1
                        }
                ]
        }
        """
        query_4 = {
            "metrics": ["count_of_orders"],
            "filters": [{"field": "num_of_item", "operator": ">", "value": 1}],
        }

        semantic_layer_4 = {
            "metrics": [
                {"name": "count_of_orders", "sql": "COUNT(order_id)", "table": "orders"}
            ],
            "dimensions": [
                {"name": "num_of_item", "sql": "num_of_item", "table": "orders"}
            ],
        }

        expected_sql_4 = (
            "SELECT COUNT(order_id) AS count_of_orders\n"
            "FROM orders\n"
            "WHERE num_of_item > 1"
        )

        actual_sql_4 = generate_sql_query(query_4, semantic_layer_4)
        self.assertEqual(actual_sql_4.strip(), expected_sql_4.strip())

        result = query_bigquery(actual_sql_4)
        self.assertEqual(result.total_rows, 1, "Mismatch on total rows")

    def test_query5(self):
        """
        Query 5: Show me the number of complete orders made by women
        {
                "metrics": ["count_of_orders"],
                "filters": [
                        {
                                "field": "status",
                                "operator": "=",
                                "value": "Complete"
                        },
                        {
                                "field": "gender",
                                "operator": "=",
                                "value": "F"
                        }
                ]
        }
        """
        query_5 = {
            "metrics": ["count_of_orders"],
            "filters": [
                {"field": "status", "operator": "=", "value": "Complete"},
                {"field": "gender", "operator": "=", "value": "F"},
            ],
        }

        semantic_layer_5 = {
            "metrics": [
                {"name": "count_of_orders", "sql": "COUNT(order_id)", "table": "orders"}
            ],
            "dimensions": [
                {"name": "num_of_item", "sql": "num_of_item", "table": "orders"},
                {"name": "gender", "sql": "gender", "table": "orders"},
                {"name": "status", "sql": "status", "table": "orders"},
            ],
        }

        expected_sql_5 = (
            "SELECT COUNT(order_id) AS count_of_orders\n"
            "FROM orders\n"
            "WHERE status = 'Complete' AND gender = 'F'"
        )

        actual_sql_5 = generate_sql_query(query_5, semantic_layer_5)
        self.assertEqual(actual_sql_5.strip(), expected_sql_5.strip())

        result = query_bigquery(actual_sql_5)
        self.assertEqual(result.total_rows, 1, "Mismatch on total rows")

    def test_query6(self):
        """
        Query 6: Show me the orders with value over $1000
        {
                "metrics": ["total_revenue"],
                "dimensions": ["order_id"],
                "filters": [
                        {
                                "field": "total_revenue",
                                "operator": ">",
                                "value": 1000
                        }
                ]
        }
        """
        query_6 = {
            "metrics": ["total_revenue"],
            "dimensions": ["order_id"],
            "filters": [{"field": "total_revenue", "operator": ">", "value": 1000}],
        }

        semantic_layer_6 = {
            "metrics": [
                {
                    "name": "total_revenue",
                    "sql": "SUM(sale_price)",
                    "table": "order_items",
                }
            ],
            "dimensions": [
                {"name": "order_id", "sql": "order_id", "table": "order_items"}
            ],
        }

        expected_sql_6 = (
            "SELECT SUM(sale_price) AS total_revenue, order_id AS order_id\n"
            "FROM order_items\n"
            "GROUP BY order_id\n"
            "HAVING SUM(sale_price) > 1000\n"
        )

        actual_sql_6 = generate_sql_query(query_6, semantic_layer_6)
        self.assertEqual(actual_sql_6.strip(), expected_sql_6.strip())

        result = query_bigquery(actual_sql_6)
        self.assertEqual(result.total_rows, 50, "Mismatch on total rows")

    def test_query7(self):
        """
        Query 7: Show me order details for orders over $1000 in value
        {
                "metrics": ["total_revenue"],
                "dimensions": ["order_id", "gender", "status"],
                "filters": [
                        {
                                "field": "total_revenue",
                                "operator": ">",
                                "value": 1000
                        }
                ]
        }

        We'll assume a semantic layer that includes table joins between
        'order_items' and 'orders'.
        """
        query_7 = {
            "metrics": ["total_revenue"],
            "dimensions": ["order_id", "gender", "status"],
            "filters": [{"field": "total_revenue", "operator": ">", "value": 1000}],
        }

        semantic_layer_7 = {
            "metrics": [
                {
                    "name": "total_revenue",
                    "sql": "SUM(sale_price)",
                    "table": "order_items",
                }
            ],
            "dimensions": [
                {"name": "order_id", "sql": "order_id", "table": "order_items"},
                {"name": "gender", "sql": "gender", "table": "orders"},
                {"name": "status", "sql": "status", "table": "orders"},
            ],
            "joins": [
                {
                    "one": "orders",
                    "many": "order_items",
                    "join": "order_items.order_id = orders.order_id",
                }
            ],
        }

        expected_sql_7_1 = (
            "SELECT SUM(order_items.sale_price) AS total_revenue, order_items.order_id AS order_id, orders.gender AS gender, orders.status AS status\n"
            "FROM order_items\n"
            "JOIN orders ON order_items.order_id = orders.order_id\n"
            "GROUP BY order_items.order_id, orders.gender, orders.status\n"
            "HAVING SUM(order_items.sale_price) > 1000"
        )
        expected_sql_7_2 = (
            "SELECT SUM(order_items.sale_price) AS total_revenue, order_items.order_id AS order_id, orders.gender AS gender, orders.status AS status\n"
            "FROM orders\n"
            "JOIN order_items ON order_items.order_id = orders.order_id\n"
            "GROUP BY order_items.order_id, orders.gender, orders.status\n"
            "HAVING SUM(order_items.sale_price) > 1000"
        )

        actual_sql_7 = generate_sql_query(query_7, semantic_layer_7)
        self.assertTrue(actual_sql_7 in [expected_sql_7_1, expected_sql_7_2])

        result = query_bigquery(actual_sql_7)
        self.assertEqual(result.total_rows, 50, "Mismatch on total rows")

    def test_query8(self):
        """
        Show me weekly sales revenue since the start of 2024
        """
        query_8 = {
            "metrics": ["total_revenue"],
            "dimensions": ["ordered_date__week"],
            "filters": [
                {"field": "ordered_date", "operator": ">=", "value": "2024-01-01"}
            ],
        }

        semantic_layer_8 = {
            "metrics": [
                {
                    "name": "total_revenue",
                    "sql": "SUM(sale_price)",
                    "table": "order_items",
                }
            ],
            "dimensions": [
                {"name": "ordered_date", "sql": "created_at", "table": "orders"}
            ],
            "joins": [
                {
                    "one": "orders",
                    "many": "order_items",
                    "join": "order_items.order_id = orders.order_id",
                }
            ],
        }

        expected_sql_8_1 = (
            "SELECT SUM(order_items.sale_price) AS total_revenue, DATE_TRUNC(orders.created_at, WEEK) AS ordered_date__week\n"
            "FROM order_items\n"
            "JOIN orders ON order_items.order_id = orders.order_id\n"
            "WHERE orders.created_at >= '2024-01-01'\n"
            "GROUP BY DATE_TRUNC(orders.created_at, WEEK)"
        )
        expected_sql_8_2 = (
            "SELECT SUM(order_items.sale_price) AS total_revenue, DATE_TRUNC(orders.created_at, WEEK) AS ordered_date__week\n"
            "FROM orders\n"
            "JOIN order_items ON order_items.order_id = orders.order_id\n"
            "WHERE orders.created_at >= '2024-01-01'\n"
            "GROUP BY DATE_TRUNC(orders.created_at, WEEK)"
        )

        actual_sql_8 = generate_sql_query(query_8, semantic_layer_8)
        self.assertTrue(actual_sql_8 in [expected_sql_8_1, expected_sql_8_2])

        result = query_bigquery(actual_sql_8)
        self.assertEqual(result.total_rows, 56, "Mismatch on total rows")


# Example usage:
if __name__ == "__main__":
    query = {
        "metrics": ["total_revenue"],
        "dimensions": ["ordered_date__week"],
        "filters": [{"field": "ordered_date", "operator": ">=", "value": "2024-01-01"}],
    }

    semantic_layer = {
        "metrics": [
            {"name": "total_revenue", "sql": "SUM(sale_price)", "table": "order_items"}
        ],
        "dimensions": [
            {"name": "ordered_date", "sql": "created_at", "table": "orders"}
        ],
        "joins": [
            {
                "one": "orders",
                "many": "order_items",
                "join": "order_items.order_id = orders.order_id",
            }
        ],
    }

    sql_query = generate_sql_query(query, semantic_layer)
    print(sql_query)

    query_bigquery(sql_query)

    unittest.main(verbosity=2)
