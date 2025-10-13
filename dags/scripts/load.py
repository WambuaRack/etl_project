import pandas as pd
import psycopg2
from sqlalchemy import create_engine

def load_to_postgres(file_path="quotes_clean.csv"):
    df = pd.read_csv(file_path)

    # PostgreSQL credentials
    user = "postgres"
    password = "postgres"
    host = "postgres"
    port = 5432
    database = "etl_db"

    engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}")

    df.to_sql("quotes", engine, if_exists="replace", index=False)
    print("Data loaded to PostgreSQL successfully!")

if __name__ == "__main__":
    load_to_postgres()
