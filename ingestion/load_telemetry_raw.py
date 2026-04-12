import snowflake.connector
import getpass
import os

def get_connection():
    user = input("Snowflake username: ")
    password = getpass.getpass("Snowflake password: ")
    return snowflake.connector.connect(
        user=user,
        password=password,
        account='AIZZHNC-TT89572',
        warehouse='PITWALL_WH',
        database='PITWALL',
        schema='RAW'
    )

def run():
    conn = get_connection()
    cursor = conn.cursor()

    parquet_path = '/Users/priyal/Desktop/projects/pitwall/data/raw/telemetry_raw_2023_2025.parquet'

    print("Uploading parquet to stage...")
    cursor.execute(f"PUT file://{parquet_path} @~ AUTO_COMPRESS=FALSE OVERWRITE=TRUE")

    print("Loading into TELEMETRY_RAW...")
    cursor.execute("TRUNCATE TABLE PITWALL.RAW.TELEMETRY_RAW")
    cursor.execute("""
        COPY INTO PITWALL.RAW.TELEMETRY_RAW
        FROM @~/telemetry_raw_2023_2025.parquet
        FILE_FORMAT = (TYPE = 'PARQUET')
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    """)

    result = cursor.fetchall()
    print(result)

    cursor.close()
    conn.close()
    print("Done.")

if __name__ == "__main__":
    run()