import pandas as pd
import psycopg2
from sqlalchemy import create_engine

# Database connection URL
db_url = "postgres://..."

# SQL Query
query = 'SELECT * FROM "verseReference"'

# Create a database connection using SQLAlchemy engine
engine = create_engine(db_url)

try:
    # Execute the query and store the result in a DataFrame
    df = pd.read_sql_query(query, engine)

    # Save the DataFrame to a TXT file
    df.to_csv('query_results.txt', sep='\t', index=False)
    print("Data saved")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Close the database connection
    engine.dispose()
