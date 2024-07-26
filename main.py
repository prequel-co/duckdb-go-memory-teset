import duckdb
import time
import threading

# Compaction query
compaction_query = """
COPY (
    SELECT id AS "id", updated_at AS "updated_at", log AS "log",
           CAST(test_decimal AS DECIMAL(38,19)) AS "test_decimal", updated_date AS "updated_date"
    FROM read_parquet(['0.parquet', '1.parquet'])
) TO 'serialized_file.parquet' (FORMAT 'PARQUET', CODEC 'uncompressed')
"""

# Read query
read_query = """
SELECT id AS "id", updated_at AS "updated_at", log AS "log",
       CAST(test_decimal AS VARCHAR) AS "test_decimal", updated_date AS "updated_date"
FROM read_parquet('serialized_file.parquet')
"""

def query_database(conn, query):
    count = 0
    try:
        cur = conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        count = len(rows)
        cur.close()
    except Exception as e:
        print(f"Error executing query: {e}")
    return count

def main():
    # Set up DuckDB connection
    conn = duckdb.connect('test.db')
    conn.execute("PRAGMA disable_object_cache;")
    conn.execute("SET autoinstall_known_extensions=1;")
    conn.execute("SET autoload_known_extensions=1;")
    conn.execute("SET memory_limit='1333MB'")
    conn.execute("SET threads = 2;")
    conn.execute("SET preserve_insertion_order=false;")
    conn.execute("PRAGMA temp_directory='/tmp/';")
    
    # Execute compaction query
    print("Executing compaction query")
    conn.execute(compaction_query)

    # Execute read query
    print("Executing read query")
    count = query_database(conn, read_query)

    # Report result
    print(f"Read: {count} rows")

    conn.close()

if __name__ == "__main__":
    main()