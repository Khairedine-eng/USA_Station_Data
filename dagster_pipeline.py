import pandas as pd
import sqlalchemy as sa
from datetime import datetime, timezone
from typing import List
from dagster import asset, job, op, Out, In, Output, resource, DynamicOut, DynamicOutput, ScheduleDefinition, repository

# Connection strings
pg_connection_string = "postgresql://postgres:1312@localhost:5432/USA_Project"
pg_engine = sa.create_engine(pg_connection_string, connect_args={"connect_timeout": 10})

@resource
def snowflake_engine(_):
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

@op(required_resource_keys={'snowflake_engine'})
def fetch_last_date_in_pg(context):
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

@op(out=DynamicOut(), required_resource_keys={'snowflake_engine'})
def fetch_isd_data(context, last_date: str, chunk_size: int = 1000000):
    engine = context.resources.snowflake_engine
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
            for i, chunk in enumerate(pd.read_sql(query, connection, chunksize=chunk_size)):
                print(f"Fetched a chunk of size {len(chunk)}.")
                chunk['imported_at'] = datetime.now(timezone.utc)
                yield DynamicOutput(chunk, mapping_key=str(i))
    except Exception as e:
        print(f"Error fetching data from Snowflake: {e}")

@op
def save_to_pg(context, df: pd.DataFrame):
    try:
        df.to_sql(name='ISD_CLEANED', con=pg_engine, schema='ISD_DATA', if_exists='append', index=False)
        print("A chunk of data saved to the database successfully!")
    except Exception as e:
        print(f"Error saving data to PostgreSQL: {e}")

@job(resource_defs={'snowflake_engine': snowflake_engine})
def data_pipeline():
    last_date = fetch_last_date_in_pg()
    data_chunks = fetch_isd_data(last_date)
    data_chunks.map(save_to_pg)


data_pipeline_schedule = ScheduleDefinition(
    job=data_pipeline,
    cron_schedule="0 0 */2 * *",
    execution_timezone="UTC",
)

@repository
def data_pipeline_repo():
    return [data_pipeline, data_pipeline_schedule]

if __name__ == "__main__":
    from dagster import execute_pipeline
    execute_pipeline(data_pipeline)
