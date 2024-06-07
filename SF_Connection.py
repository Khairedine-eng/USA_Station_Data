import snowflake.connector
import pandas as pd

def get_snowflake_connection(user, password, account, database, schema):
    """
    Creates and returns a Snowflake connection using snowflake.connector.

    Parameters:
    user (str): The Snowflake user name.
    password (str): The Snowflake password.
    account (str): The Snowflake account identifier.
    database (str): The Snowflake database name.
    schema (str): The Snowflake schema name.

    Returns:
    connection: A Snowflake connection object.
    """
    try:
        conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=account,
            database=database,
            schema=schema
        )
        print("Connection to Snowflake created successfully.")
        return conn
    except snowflake.connector.errors.OperationalError as oe:
        print(f"OperationalError: {oe}")
    except snowflake.connector.errors.IntegrityError as ie:
        print(f"IntegrityError: {ie}")
    except snowflake.connector.errors.InternalError as ine:
        print(f"InternalError: {ine}")
    except snowflake.connector.errors.ProgrammingError as pe:
        print(f"ProgrammingError: {pe}")
    except snowflake.connector.errors.NotSupportedError as nse:
        print(f"NotSupportedError: {nse}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    return None

def fetch_first_20_rows(conn, table_name):
    """
    Fetches the first 20 rows of the specified table from the Snowflake database.

    Parameters:
    conn (object): Snowflake connection object.
    table_name (str): The name of the table to fetch data from.

    Returns:
    DataFrame: A pandas DataFrame containing the first 20 rows of the table.
    """
    if conn is None:
        print("Connection is not available.")
        return None

    query = f"SELECT * FROM {table_name} LIMIT 20"
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        df = cursor.fetch_pandas_all()
        return df
    except snowflake.connector.errors.ProgrammingError as pe:
        print(f"ProgrammingError: {pe}")
    except Exception as e:
        print(f"Error fetching data: {e}")
    return None

def main():
    # Replace with your actual credentials and database details
    user = "DNEXR-ABDELJAWED"
    password = "1312Acab@"
    account = "MQJSHRJ.GQ42038"
    database = "DNEXR"
    schema = "LAB"  # Replace with your actual schema name

    # Get the Snowflake connection
    conn = get_snowflake_connection(user, password, account, database, schema)

    table_name = "isd_daily_cleaned"  # Replace with your actual table name

    # Fetch the first 20 rows of the table
    df = fetch_first_20_rows(conn, table_name)

    # Print the results if data was fetched successfully
    if df is not None:
        print("First 20 rows of the table:")
        print(df)

    # Close the connection
    if conn:
        conn.close()

if __name__ == "__main__":
    main()
