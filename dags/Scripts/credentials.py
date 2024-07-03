import os
from Scripts.db import DBConnection
from dotenv import load_dotenv
load_dotenv()

def get_warehouse_creds() -> DBConnection:
    return DBConnection(
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD'),
        db=os.getenv('POSTGRES_DB'),
        host=os.getenv('POSTGRES_HOST'),
        port=int(os.getenv('POSTGRES_PORT', 5432)),
    )