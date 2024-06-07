import pandas as pd
import sqlalchemy as sa


def get_engine():
    engine = sa.create_engine(
        "snowflake://{user}:{password}@{account}/{database}".format(
            user="DNEXR-JBENSALEM",
            password="Sarra29715739*-+",
            account="gq42038.eu-west-1",
            database="DNEXR",
        )
    )
    return engine


def get_table_details(engine, schema, table):
    query = f'SELECT * FROM {schema}.{table}'
    df = pd.read_sql(query, engine)

    details = {
        'head': df.head(),
        'columns': df.columns.tolist(),
        'null_values': df.isnull().sum().to_dict(),
        'nan_counts': df.isna().sum().to_dict(),
        'tail': df.tail(10)
    }

    return details



def test_engine_creation():
    engine = get_engine()
    assert engine is not None
    print("Database connection successful!")

    schema = 'PUBLIC'
    table = 'YOUR_TABLE'
    details = get_table_details(engine, schema, table)

    for key, value in details.items():
        print(f"{key}:\n{value}\n")


if __name__ == '__main__':
    test_engine_creation()
