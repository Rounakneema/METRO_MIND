from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from urllib.parse import quote_plus

# Example db_config
class db_config:
    postgres_user = "metromind_user"
    postgres_password = "MetroMind@2025"  # your password
    postgres_host = "localhost"
    postgres_port = 5432
    postgres_db = "metromind_db"

# URL-encode the password
password = quote_plus(db_config.postgres_password)

# Construct the connection string
DATABASE_URL = f"postgresql+psycopg2://{db_config.postgres_user}:{password}@{db_config.postgres_host}:{db_config.postgres_port}/{db_config.postgres_db}"

# Create engine and session
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

session = SessionLocal()

# Truncate tables
tables_to_clear = ["users"]  # add other tables if needed
for table in tables_to_clear:
    session.execute(text(f"TRUNCATE TABLE {table} CASCADE;"))

session.commit()
session.close()
print("All previous entries deleted.")
