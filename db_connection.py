import os
import psycopg2
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from dotenv import load_dotenv

load_dotenv()

def _is_unix_socket(host: str) -> bool:
    return host.startswith("/")


def get_connection():
    host     = os.getenv("PG_HOST")
    db       = os.getenv("PG_DATABASE")
    user     = os.getenv("PG_USER")
    password = os.getenv("PG_PASSWORD")

    if _is_unix_socket(host):
        return psycopg2.connect(
            host=host,
            dbname=db,
            user=user,
            password=password,
        )
    else:
        port = os.getenv("PG_PORT", "5432")
        return psycopg2.connect(
            host=host,
            port=port,
            dbname=db,
            user=user,
            password=password,
            sslmode="require",
        )


def get_engine():
    host     = os.getenv("PG_HOST")
    db       = os.getenv("PG_DATABASE")
    user     = os.getenv("PG_USER")
    password = quote_plus(os.getenv("PG_PASSWORD"))

    if _is_unix_socket(host):
        return create_engine(
            f"postgresql+psycopg2://{user}:{password}@/{db}?host={host}"
        )
    else:
        port = os.getenv("PG_PORT", "5432")
        return create_engine(
            f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}?sslmode=require"
        )
