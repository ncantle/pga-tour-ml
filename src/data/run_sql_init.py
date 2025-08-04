from sqlalchemy import text
from config.db_config import get_engine

def run_sql_init():
    with open("./sql/init_db.sql") as f:
        init_script = f.read()
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text(init_script))

if __name__ == "__main__":
    run_sql_init()
    print("Database initialized successfully.")