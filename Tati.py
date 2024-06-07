import sqlalchemy as sa
import pandas as pd
from datetime import datetime, timezone

# Connection string for PostgreSQL
pg_connection_string = "postgresql://postgres:1312@localhost:5432/USA_Project"
pg_engine = sa.create_engine(pg_connection_string, connect_args={"connect_timeout": 10})

def get_engine():
    print("Creating Snowflake engine...")
    try:
        engine = sa.create_engine(
            "snowflake://{user}:{password}@{account}/{database}".format(
                user="DNEXR-JBENSALEM",
                password="Sarra29715739*-+",
                account="gq42038.eu-west-1",
                database="DNEXR"
            ),
            connect_args={"connect_timeout": 10}
        )
        print("Snowflake engine created successfully.")
        return engine
    except Exception as e:
        print(f"Error creating Snowflake engine: {e}")
        return None

def get_last_date_in_pg():
    print("Fetching the last date from PostgreSQL...")
    query = 'SELECT MAX("date") FROM "ISD_DATA"."ISD_CLEANED"'
    try:
        with pg_engine.connect() as connection:
            result = connection.execute(sa.text(query))
            last_date = result.scalar()
            if last_date is None:
                last_date = '1970-01-01'
            print(f"Last date in PostgreSQL: {last_date}")
            return last_date
    except Exception as e:
        print(f"Error fetching the last date from PostgreSQL: {e}")
        return '1970-01-01'

def get_isd_data_in_batches(engine, last_date, chunk_size=1000000):
    query = sa.text("""
        SELECT DATE, ID, PARTITION_KEY, PRECIPITATION, STATION, TEMPERATURE_MAXIMUM, TEMPERATURE_MINIMUM
        FROM "LAB"."ISD_DAILY_CLEANED"
        WHERE DATE > :last_date
    """).bindparams(last_date=str(last_date))

    print("Fetching data from Snowflake in batches...")
    print(f"Query: {query}")
    print(f"last_date: {last_date}")
    try:
        with engine.connect() as connection:
            print("Connected to Snowflake. Executing query...")
            for chunk in pd.read_sql(query, connection, chunksize=chunk_size):
                print(f"Fetched a chunk of size {len(chunk)}.")
                print(chunk.head())
                chunk['imported_at'] = datetime.now(timezone.utc)
                yield chunk
    except Exception as e:
        print(f"Error fetching data from Snowflake: {e}")

def create_schema(engine, schema_name):
    print(f"Creating schema '{schema_name}' if not exists...")
    try:
        with engine.connect() as connection:
            connection.execute(sa.text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))
        print(f"Schema '{schema_name}' created or already exists.")
    except Exception as e:
        print(f"Error creating schema '{schema_name}': {e}")

def main():
    print("Starting main process...")
    engine = get_engine()
    if engine is not None:
        create_schema(pg_engine, 'ISD_DATA')
        last_date = get_last_date_in_pg()
        try:
            for chunk in get_isd_data_in_batches(engine, last_date):
                chunk.to_sql(name='ISD_CLEANED', con=pg_engine, schema='ISD_DATA', if_exists='append', index=False)
                print("A chunk of data saved to the database successfully!")
        except Exception as e:
            print(f"Error saving data to PostgreSQL: {e}")
    else:
        print("Failed to create Snowflake engine.")

if __name__ == "__main__":
    main()
