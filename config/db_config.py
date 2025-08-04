import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

# Load environment variables from .env file in project root
load_dotenv()

def get_engine():
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB")

    if not all([user, password, host, port, db]):
        raise ValueError("Missing one or more required database environment variables.")

    db_url = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    return create_engine(db_url, echo=False)
