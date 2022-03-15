from conn_utils import *

IMPROVE_THRESHOLD = 0.05
INDEX_TYPES = ['btree', 'brin', 'hash']

def get_create_index_sql(index_candidate):
    table_name, columns, index_type = index_candidate
    columns_str = f"({', '.join(list(columns))})"
    return f"CREATE INDEX on {table_name} USING {index_type} {columns_str}"

def enumerate_index(conn):
    table_name_results = run_query(conn, "SELECT tablename FROM pg_catalog.pg_tables where schemaname='public'")
    table_names = [tup[0] for tup in table_name_results]

    # Brute-force enumerate all 1 and 2-column indexes firstly.
    index_candidates = set()
    for table_name in table_names:
        column_results = run_query(conn, f"SELECT column_name FROM information_schema.columns WHERE table_name='{table_name}'")
        columns = [tup[0] for tup in column_results]

        for index_type in INDEX_TYPES:
            for col1 in columns:
                for col2 in columns:
                    if col1 == col2:
                        index_candidates.add((table_name, (col1,), index_type))
                    elif index_type != 'hash':
                        index_candidates.add((table_name, (col1, col2), index_type))

    # Substract the candiate set with all existing indexes.
    exist_indexes = set()
    index_results = run_query(conn, "SELECT tablename, indexdef from pg_indexes WHERE schemaname='public'")
    for table_name, index_statement in index_results:
        tokens = index_statement.split()
        print(tokens)
        idx = -1
        while tokens[idx].lower() != 'using':
            idx -= 1

        index_type = tokens[idx + 1]
        idx += 2
        index_cols = []
        while idx < 0:
            col = tokens[idx].replace(',', '').replace('(', '').replace(')', '')
            index_cols.append(col)
            idx += 1

        exist_indexes.add((table_name, tuple(index_cols), index_type))
        
    return index_candidates - exist_indexes

def get_workload_costs(queries, conn):
    """
    Get the total cost of each query based on EXPLAIN.
    EXPLAIN returns hypothetical costs when there are hypopg indexes.
    """
    cost_per_query = []
    total_cost = 0.0

    for query in queries:
        explain_result = run_query(conn, f"EXPLAIN {query}")
        root_line = explain_result[0][0]
        tokens = root_line.split()
        # Cost format: (cost=start_cost..total_cost rows width)
        for token in tokens:
            if token.startswith('(cost='):
                query_cost = float(token.split('..')[-1])
                total_cost += query_cost
                cost_per_query.append(query_cost)
                break

    return total_cost, cost_per_query

def recommend_index(queries, conn, hypo_added_index):
    """
    A very simplified Dexter. Brute force enumerate all one and two columns
    indexes not created, including three types.
    For each one, call hypopg to create fake index and explain all queries.
    Select the one causing smallest cumulatative cost.
    The optimization on total cost or portion of queries should exceed certain threshold.
    Or the recommendation should be empty.
    """
    index_candiates = enumerate_index(conn) - hypo_added_index
    # Get initial costs without any hypo indexes.
    original_total_cost, original_cost_per_query = get_workload_costs(queries, conn)
    minimum_cost = original_total_cost
    recommendation = None
    new_cost_per_query = None

    for index_candidate in index_candiates:
        hypo_result = run_query(conn, f"select indexrelid from hypopg_create_index('{get_create_index_sql(index_candidate)}')")
        conn.commit()
        oid = int(hypo_result[0][0])

        total_cost, cost_per_query = get_workload_costs(queries, conn)
        if total_cost < minimum_cost:
            minimum_cost = total_cost
            new_cost_per_query = cost_per_query
            recommendation = index_candidate

        # Remove hypopg index of current index.
        run_query(conn, f"select * from hypopg_drop_index({oid})")
        conn.commit()

    # If optimization is not significant, recommendation is not used.
    if total_cost < (1.0 - IMPROVE_THRESHOLD) * original_total_cost:
        # Add this to hypopg, for further invoke in this iteration.
        hypo_added_index.add(recommendation)
        run_query(conn, f"select indexrelid from hypopg_create_index('{get_create_index_sql(recommendation)}')")
        conn.commit()
        return [f"{get_create_index_sql(recommendation)};",]
    else:
        return []