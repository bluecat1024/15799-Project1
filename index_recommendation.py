from conn_utils import *

IMPROVE_THRESHOLD = 0.1
PER_QUERY_THRESHOLD = 0.5
INDEX_TYPES = ['btree', 'brin', 'hash']

def get_create_index_sql(index_candidate):
    table_name, columns, index_type = index_candidate
    columns_str = f"({', '.join(list(columns))})"
    return f"CREATE INDEX on {table_name} USING {index_type} {columns_str}"

def enumerate_index(conn, queries):
    """
    Get all candidate indexes that can be added.
    Only enumerate up to 3 columns.
    """
    table_name_results = run_query(conn, "SELECT tablename FROM pg_catalog.pg_tables where schemaname='public'")
    table_names = [tup[0] for tup in table_name_results]

    # Firstly filter by column usage in queries, then enumerate to up to 3 columns.
    index_candidates = set()
    for table_name in table_names:
        column_results = run_query(conn, f"SELECT column_name FROM information_schema.columns WHERE table_name='{table_name}'")
        columns = []
        for tup in column_results:
            col_name = tup[0]
            # Use very conservative way so we determine the column not used in workloads.
            found_in_query = False
            for q in queries:
                # If the two words co-appear then cannot filter.
                if q.find(table_name) >= 0 and q.find(col_name) >= 0:
                    found_in_query = True
                    break
            if found_in_query:
                columns.append(col_name)

        for index_type in INDEX_TYPES:
            for col1 in columns:
                for col2 in columns:
                    if col1 == col2:
                        index_candidates.add((table_name, (col1,), index_type))
                    elif index_type != 'hash':
                        # All the names must co-appear in one query to be considered.
                        found_multi_in_query = False
                        for q in queries:
                            if q.find(table_name) >= 0\
                                and q.find(col1) >= 0\
                                and q.find(col2) >= 0:
                                found_multi_in_query = True
                                break
                        if found_multi_in_query:
                            index_candidates.add((table_name, (col1, col2), index_type))

            # Enumerate 3 columns.
            for col1 in columns:
                for col2 in columns:
                    for col3 in columns:
                        if col1 == col2 or col2 == col3\
                            or col1 == col3 or index_type == 'hash':
                            continue

                        # All the names must co-appear in one query to be considered.
                        found_multi_in_query = False
                        for q in queries:
                            if q.find(table_name) >= 0\
                                and q.find(col1) >= 0\
                                and q.find(col2) >= 0\
                                and q.find(col3) >= 0:
                                found_multi_in_query = True
                                break
                        if found_multi_in_query:
                            index_candidates.add((table_name, (col1, col2, col3), index_type))

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

def enumerate_droppables(conn):
    """
    Return all droppable index names.
    The indexes are without unique or other constraints.
    """
    index_name_result = run_query(conn, """
    SELECT s.indexrelname AS indexname FROM 
    pg_catalog.pg_stat_user_indexes s JOIN 
    pg_catalog.pg_index i ON s.indexrelid = i.indexrelid 
    WHERE NOT i.indisunique AND Not EXISTS (SELECT 1 FROM 
    pg_catalog.pg_constraint c WHERE c.conindid = s.indexrelid) 
    and s.schemaname='public'
    """)

    return set([tup[0] for tup in index_name_result])

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
    A very simplified Dexter. Enumerate all up to 3 columns used by queries.
    indexes not created, including three types.
    For each one, call hypopg to create fake index and explain all queries.
    Select the one causing smallest cumulatative cost.
    The optimization on total cost or portion of queries should exceed certain threshold.
    Or the recommendation should be empty.
    """
    index_candiates = enumerate_index(conn, queries) - hypo_added_index
    # Get initial costs without any hypo indexes.
    original_total_cost, original_cost_per_query = get_workload_costs(queries, conn)
    minimum_cost = original_total_cost
    recommendation = None
    new_cost_per_query = original_cost_per_query

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

    # See if the index causes good performance on some query templates.
    is_significant_query_template = False
    for idx in range(len(original_cost_per_query)):
        if new_cost_per_query[idx] <= PER_QUERY_THRESHOLD * original_cost_per_query[idx]:
            is_significant_query_template = True
    # If optimization is not significant, recommendation is not used.
    if minimum_cost < (1.0 - IMPROVE_THRESHOLD) * original_total_cost\
        or (is_significant_query_template and minimum_cost < original_total_cost):
        # Add this to hypopg, for further invoke in this iteration.
        hypo_added_index.add(recommendation)
        run_query(conn, f"select indexrelid from hypopg_create_index('{get_create_index_sql(recommendation)}')")
        conn.commit()
        return [f"{get_create_index_sql(recommendation)};",]
    else:
        return []

def drop_index(queries, conn, hypo_dropped_index):
    """
    My implementation not only supports adding index,
    but also supports dropping index. Like dexter,
    but simpler than hypopg, just set booleans to disable
    indexes, to hypothetically drop the indexes.
    """
    drop_candidates = enumerate_droppables(conn) - hypo_dropped_index
    original_total_cost, original_cost_per_query = get_workload_costs(queries, conn)
    minimum_cost = original_total_cost + 1.0
    recommendation = None
    new_cost_per_query = original_cost_per_query

    for drop_candidate in drop_candidates:
        # Hypothetically disable the index.
        run_query(conn, f"UPDATE pg_index SET indisvalid=false, indisready=false WHERE indexrelid='{drop_candidate}'::regclass")

        total_cost, cost_per_query = get_workload_costs(queries, conn)
        if total_cost < minimum_cost:
            minimum_cost = total_cost
            new_cost_per_query = cost_per_query
            recommendation = drop_candidate

        # Enable the index back again.
        run_query(conn, f"UPDATE pg_index SET indisvalid=true, indisready=true WHERE indexrelid='{drop_candidate}'::regclass")

    # See if the best drop index does not cause spurious degrade on some query templates.
    is_significant_query_template = False
    for idx in range(len(original_cost_per_query)):
        if new_cost_per_query[idx] * PER_QUERY_THRESHOLD >= original_cost_per_query[idx]:
            is_significant_query_template = True

    # Only choose the drop candidate if minimum cost is same or better.
    # Not causing spurious degrade on some queries.
    if minimum_cost <= original_total_cost and not is_significant_query_template:
        # Add this to hypothetically drop list.
        hypo_dropped_index.add(recommendation)
        run_query(conn, f"UPDATE pg_index SET indisvalid=false, indisready=false WHERE indexrelid='{recommendation}'::regclass")
        return [f"DROP INDEX {recommendation};"]
    else:
        return []


