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
    try:
        cursor.execute(query)
        return cursor.fetchall()
    except:
        # Throw exceptions if on error again.
        conn.reset()
        cursor = conn.cursor()
        cursor.execute(query)
        return cursor.fetchall()