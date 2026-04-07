import snowflake.connector
import getpass

user = input("Snowflake username (email): ")
password = getpass.getpass("Snowflake password: ")
account = input("Account identifier: ")

conn = snowflake.connector.connect(
    user=user,
    password=password,
    account=account,
    warehouse='PITWALL_WH',
    database='PITWALL',
    schema='RAW'
)

cursor = conn.cursor()

print("Uploading file to stage... (this may take a few minutes for 9M rows)")
cursor.execute("PUT file:///Users/priyal/Desktop/projects/pitwall/data/raw/telemetry_raw_2024.csv @~ AUTO_COMPRESS=TRUE")

print("Loading into table...")
cursor.execute("""
    COPY INTO PITWALL.RAW.TELEMETRY_RAW
    FROM @~/telemetry_raw_2024.csv.gz
    FILE_FORMAT = (
        TYPE = 'CSV'
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        SKIP_HEADER = 1
        NULL_IF = ('', 'None', 'NaN')
    )
""")

print(cursor.fetchall())
cursor.close()
conn.close()