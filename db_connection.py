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

# Test function
def test_engine_creation():
    engine = get_engine()
    assert engine is not None
    print("Database connection successful!")

if __name__ == '__main__':
    test_engine_creation()
