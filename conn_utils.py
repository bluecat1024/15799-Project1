import psycopg2

cursor = None

def get_conn(host, dbname, user, password):
    global cursor
    conn = psycopg2.connect(host=host, dbname=dbname, user=user, password=password)
    cursor = conn.cursor()

    return conn

def run_query(conn, query):
    assert conn is not None
    global cursor
    is_select = query.lower().strip().startswith("select")\
        or query.lower().strip().startswith("explain")\
        or query.lower().strip().startswith("show")
    try:
        cursor.execute(query)
        if is_select:
            return cursor.fetchall()
        else:
            conn.commit()
            return None
    except:
        # Throw exceptions if on error again.
        conn.reset()
        cursor = conn.cursor()
        cursor.execute(query)
        if is_select:
            return cursor.fetchall()
        else:
            conn.commit()
            return None