from fastapi import FastAPI, HTTPException
from typing import List
from pydantic import BaseModel
import pandas as pd
import mysql.connector
import io
from google.cloud import storage

app = FastAPI()

# define a Pydantic model for the new data
class Employee(BaseModel):
    id: int
    name: str
    datetime: str
    department_id: int
    job_id: int

class Department(BaseModel):
    id: int
    department: str

class Job(BaseModel):
    id: int
    job: str

# define functions for extracting CSV data and loading data into MySQL tables
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

    # insert data from DataFrame into MySQL table
    for index, row in dataframe.iterrows():
        # create SET string for schema
        schema_list = [f"{col}={val}" for col, val in schema.items()]
        schema_str = ', '.join(schema_list)

        # create VALUES tuple for row data
        values_tuple = tuple(row)

        # execute SQL statement
        cursor.execute(f"INSERT INTO {table} SET {schema_str}", values_tuple)

    # commit the changes and close the connection
    cnx.commit()
    cursor.close()
    cnx.close()

# define a POST endpoint that receives new employee data
@app.post("/employees/")
def create_employee(employee: Employee):
    # create a connection to the MySQL instance
    cnx = mysql.connector.connect(user=user, password=password, host=host, database=database)

    # create a cursor object to execute SQL statements
    cursor = cnx.cursor()

    # validate data rules
    if employee.job_id not in jobs['id'].values:
        raise HTTPException(status_code=400, detail="Invalid job id")
    if employee.department_id not in departments['id'].values:
        raise HTTPException(status_code=400, detail="Invalid department id")

    # insert data into hired_employees table
    cursor.execute("INSERT INTO hired_employees (id, name, datetime, department_id, job_id) VALUES (%s, %s, %s, %s, %s)", 
                    (employee.id, employee.name, employee.datetime, employee.department_id, employee.job_id))

    # commit changes and close the connection
    cnx.commit()
    cursor.close()
    cnx.close()

    return {"message": "Employee created successfully"}

@app.post("/departments/")
def create_department(department: Department):
    # create a connection to the MySQL instance
    cnx = mysql.connector.connect(user=user, password=password, host=host, database=database)

    # create a cursor object to execute SQL statements
    cursor = cnx.cursor()

    # insert data into departments table
    cursor.execute("INSERT INTO departments (id, department) VALUES (%s, %s)", (department.id, department.department))

    # commit changes and close the connection
    cnx.commit()
    cursor.close()
    cnx.close()

    return {"message": "Department created successfully"}

# define a POST endpoint that receives new job data
@app.post("/jobs/")
def create_job(job: Job):
    # create a connection to the MySQL instance
    cnx = mysql.connector.connect(user=user, password=password, host=host, database=database)

    # create a cursor object to execute SQL statements
    cursor = cnx.cursor()

    # insert data into jobs table
    cursor.execute("INSERT INTO jobs (id, job) VALUES (%s, %s)", (job.id, job.job))

    # commit changes and close the connection
    cnx.commit()
    cursor.close()
    cnx.close()

    return {"message": "Job created successfully"}