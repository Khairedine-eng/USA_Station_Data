import sqlalchemy as sa
from sqlalchemy.engine import reflection
from db_connection import get_engine


def get_table_schema(table_name: str, schema: str = None):
    engine = get_engine()

    # Create a reflection object to introspect the database
    inspector = reflection.Inspector.from_engine(engine)

    # Get columns information
    columns = inspector.get_columns(table_name, schema=schema)

    print(f"Schema for table '{table_name}':")
    for column in columns:
        print(f"{column['name']} ({column['type']})")


import pandas as pd
import sqlalchemy as sa


def get_pg_table_schema(pg_connection_string: str, table_name: str, schema: str = None):
    engine = sa.create_engine(pg_connection_string)

    with engine.connect() as connection:
        inspector = sa.inspect(engine)
        columns = inspector.get_columns(table_name, schema=schema)

        print(f"Schema for table '{table_name}' in PostgreSQL:")
        for column in columns:
            print(f"{column['name']} ({column['type']})")




# Example usage
if __name__ == '__main__':
    # Replace 'your_table_name' and 'your_schema' with actual table name and schema
    get_table_schema('ISD_DAILY_CLEANED', 'LAB')
    pg_connection_string = "postgresql://postgres:1312@localhost:5432/USA_Project"
    get_pg_table_schema(pg_connection_string, 'Unique_Weather_Stations', 'USA_Data')



    # Connect to the PostgreSQL database
    pg_connection_string = "postgresql://postgres:1312@localhost:5432/USA_Project"
    pg_engine = sa.create_engine(pg_connection_string, connect_args={"connect_timeout": 10})

    # Function to read weather stations from PostgreSQL
    def read_stations_from_pg(pg_engine):
        query = 'SELECT * FROM "USA_Data"."Unique_Weather_Stations"'
        df = pd.read_sql_query(query, con=pg_engine)
        return df

    df_ISD_stations = read_stations_from_pg(pg_engine)