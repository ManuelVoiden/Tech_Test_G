from google.cloud import storage
import mysql.connector

def extract_csv(bucket_name, source_blob_name, destination_file_name):
    """
    Downloads a CSV file from a Cloud Storage bucket to a local file.
    """
    # create a Cloud Storage client object
    storage_client = storage.Client()

    # get a reference to the bucket and CSV file
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(source_blob_name)

    # download the CSV file to a local file
    blob.download_to_filename(destination_file_name)

def load_csv_to_sql(file_path, user, password, host, database, table):
    """
    Loads data from a CSV file into a MySQL table.
    """
    # create a connection to the Cloud SQL instance
    cnx = mysql.connector.connect(user=user, password=password,
                                  host=host, database=database)

    # create a cursor object to execute SQL statements
    cursor = cnx.cursor()

    # read data from CSV file and insert into the table
    with open(file_path, 'r') as f:
        for line in f:
            # parse the data and insert into the table
            data = line.split(',')
            cursor.execute(f"INSERT INTO {table} (col1, col2, col3) VALUES (%s, %s, %s)", (data[0], data[1], data[2]))

    # commit the changes and close the connection
    cnx.commit()
    cursor.close()
    cnx.close()

def extract_and_load_csv(bucket_name, source_blob_name, destination_file_name,
                         user, password, host, database, table):
    """
    Downloads a CSV file from a Cloud Storage bucket and loads its contents into a MySQL table.
    """
    # download the CSV file from the Cloud Storage bucket
    extract_csv(bucket_name, source_blob_name, destination_file_name)

    # load the CSV data into the MySQL table
    load_csv_to_sql(destination_file_name, user, password, host, database, table)