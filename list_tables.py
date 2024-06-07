from db_connection import get_engine


def list_tables_and_schemas():
    engine = get_engine()
    with engine.connect() as connection:
        # List schemas
        schemas = connection.execute("SHOW SCHEMAS").fetchall()
        print("Schemas:")
        for schema in schemas:
            print(schema[1])

        # List tables in the specific schema
        schema_name = "LAB"  # Replace with the correct schema if necessary
        tables = connection.execute(f"SHOW TABLES IN SCHEMA {schema_name}").fetchall()
        print(f"\nTables in schema '{schema_name}':")
        for table in tables:
            print(table[1])


if __name__ == '__main__':
    list_tables_and_schemas()
