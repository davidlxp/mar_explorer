import duckdb, pathlib

DB_PATH = pathlib.Path("storage/mar_explorer.duckdb")

def get_con():
    con = duckdb.connect(str(DB_PATH))
    con.execute("""
    CREATE TABLE IF NOT EXISTS logs (
        ts TIMESTAMP DEFAULT current_timestamp,
        question TEXT,
        confidence FLOAT,
        citations TEXT
    );
    """)
    con.execute("""
    CREATE TABLE IF NOT EXISTS pr_index (
        id INTEGER,
        text TEXT,
        embedding FLOAT[1536]
    );
    """)
    return con
