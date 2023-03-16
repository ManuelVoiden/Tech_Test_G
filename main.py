import pandas as pd
import mysql.connector
import io
from google.cloud import storage

def extract_csv(bucket_name, source_blob_name):
    """
    Downloads a CSV file from a Cloud Storage bucket as a pandas DataFrame.
    """
    # create a Cloud Storage client object
    storage_client = storage.Client()

    # get a reference to the bucket and CSV file
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(source_blob_name)

    # download the CSV file as a string
    data = blob.download_as_string()

    # convert the string to a pandas DataFrame
    dataframe = pd.read_csv(io.StringIO(data.decode('utf-8')))

    return dataframe

def load_dataframe_to_sql(dataframe, user, password, host, database, table, schema):
    """
    Loads data from a pandas DataFrame into a MySQL table.
    """
    # create a connection to the MySQL instance
    cnx = mysql.connector.connect(user=user, password=password, host=host, database=database)

    # create a cursor object to execute SQL statements
    cursor = cnx.cursor()

    # create SQL query to create the table with the specified schema
    create_query = f"CREATE TABLE IF NOT EXISTS {table} ({', '.join([f'{col} {dtype}' for col, dtype in schema.items()])})"

    # create the table in the database
    cursor.execute(create_query)

    # insert data from DataFrame into MySQL table
    for index, row in dataframe.iterrows():
        # create SET string for schema
        schema_list = [f"{col}={val}" for col, val in row.to_dict().items()]
        schema_str = ', '.join(schema_list)

        # execute SQL statement
        cursor.execute(f"INSERT INTO {table} SET {schema_str}")

    # commit the changes and close the connection
    cnx.commit()
    cursor.close()
    cnx.close()

def extract_and_load_csv(bucket_name, source_blob_name, user, password, host, database, table, schema):
    """
    Downloads a CSV file from a Cloud Storage bucket and loads its contents into a MySQL table.
    """
    # extract the CSV data as a pandas DataFrame
    dataframe = extract_csv(bucket_name, source_blob_name)

    # load the DataFrame into the MySQL table
    load_dataframe_to_sql(dataframe, user, password, host, database, table, schema)

# set the necessary parameters
bucket_name = 'my-bucket'
source_blob_name = 'data.csv'
user = 'my-user'
password = 'my-password'
host = 'my-host'
database = 'my-database'
table = 'my-table'
schema = {'col1': 'VARCHAR(255)', 'col2': 'INT', 'col3': 'FLOAT'}

# extract the CSV file from the bucket and load its contents into the MySQL table
extract_and_load_csv(bucket_name, source_blob_name, user, password, host, database, table, schema)