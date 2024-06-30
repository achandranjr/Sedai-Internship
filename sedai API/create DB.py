import csv
import psycopg2
from psycopg2 import sql

# Database connection parameters
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASSWORD = "password"
DB_HOST = "localhost"
DB_PORT = "5432"

# CSV file path
CSV_FILE_PATH = r"C:\Users\Aditya\Downloads\Most Streamed Spotify Songs 2024.csv"

# Table name
TABLE_NAME = "spotify"

def create_table(cursor, headers):
    # Create table with columns based on CSV headers, first column as primary key
    columns = [sql.Identifier(header) for header in headers]
    create_table_query = sql.SQL("CREATE TABLE IF NOT EXISTS {} ({} TEXT PRIMARY KEY, {})").format(
        sql.Identifier(TABLE_NAME),
        columns[0],
        sql.SQL(', ').join(sql.SQL("{} TEXT").format(column) for column in columns[1:])
    )
    cursor.execute(create_table_query)

def insert_data(cursor, headers, row):
    # Insert data into the table
    insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({}) ON CONFLICT DO NOTHING").format(
        sql.Identifier(TABLE_NAME),
        sql.SQL(', ').join(map(sql.Identifier, headers)),
        sql.SQL(', ').join(sql.Placeholder() * len(headers))
    )
    cursor.execute(insert_query, row)

def main():
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    cursor = conn.cursor()

    try:
        with open(CSV_FILE_PATH, 'r') as csv_file:
            csv_reader = csv.reader(csv_file)
            headers = next(csv_reader)  # Get the headers

            create_table(cursor, headers)

            for row in csv_reader:
                insert_data(cursor, headers, row)

        conn.commit()
        print("Data successfully imported into PostgreSQL database.")

    except (Exception, psycopg2.Error) as error:
        print(f"Error: {error}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    main()