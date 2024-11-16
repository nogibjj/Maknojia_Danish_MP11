import requests
from dotenv import load_dotenv
import os
import json
import base64
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, monotonically_increasing_id
#from pyspark.sql.types import IntegerType

# Load environment variables
load_dotenv()
server_h = os.getenv("SERVER_HOSTNAME")
access_token = os.getenv("ACCESS_TOKEN")
FILESTORE_PATH = "dbfs:/FileStore/danish_mini_project11"
headers = {'Authorization': f'Bearer {access_token}'}
url = f"https://{server_h}/api/2.0"

LOG_FILE = "Output_WRData.md"

# Helper Functions
def log_output(operation, output, query=None):
    """Logs output to a markdown file."""
    with open(LOG_FILE, "a") as file:
        file.write(f"The operation is {operation}\n\n")
        if query: 
            file.write(f"The query is {query}\n\n")
        file.write("The truncated output is: \n\n")
        file.write(output)
        file.write("\n\n")

def perform_request(path, method="POST", data=None):
    """Performs an HTTP request to the Databricks API."""
    session = requests.Session()
    response = session.request(
        method=method,
        url=f"{url}{path}",
        headers=headers,
        data=json.dumps(data) if data else None,
        verify=True
    )
    return response.json()

def upload_file_from_url(url, dbfs_path, overwrite):
    """Uploads a file from a URL to DBFS."""
    response = requests.get(url)
    if response.status_code == 200:
        content = response.content
        # Create file handle
        handle = perform_request("/dbfs/create", 
                                 data={"path": dbfs_path, 
                                       "overwrite": overwrite})["handle"]
        print(f"Uploading file: {dbfs_path}")
        # Add file content in chunks
        for i in range(0, len(content), 2**20):
            perform_request(
                "/dbfs/add-block",
                data={"handle": handle, 
                      "data": base64.standard_b64encode(content[i:i+2**20]).decode()}
            )
        # Close the handle
        perform_request("/dbfs/close", data={"handle": handle})
        print(f"File {dbfs_path} uploaded successfully.")
    else:
        print("Failed to download")

# ETL Functions
def extract(
    source_url=
    "https://raw.githubusercontent.com/nogibjj/Maknojia_Danish_MP5/refs/heads/main/data/WRRankingsWeek5.csv",
    target_path=FILESTORE_PATH+"/WRRankingsWeek5.csv",
    directory=FILESTORE_PATH,
    overwrite=True
):
    """Extracts and uploads data."""
    # Create directory on DBFS
    perform_request("/dbfs/mkdirs", data={"path": directory})
    # Upload file to DBFS
    upload_file_from_url(source_url, target_path, overwrite)
    return target_path

def transform_and_load(dataset=FILESTORE_PATH+"/WRRankingsWeek5.csv"):
    """Transforms and loads data into Delta Lake."""
    spark = SparkSession.builder.appName("Transform and Load WRRankings").getOrCreate()

    # Read the CSV file with the original schema (infer column types)
    df = spark.read.csv(dataset, header=True, inferSchema=True)


    df = df.withColumnRenamed("PLAYER NAME", "PLAYER_NAME") \
           .withColumnRenamed("START/SIT", "START_SIT") \
           .withColumnRenamed("PROJ. FPTS", "PROJ_FPTS") \
           .withColumnRenamed("MATCHUP ", "MATCHUP")

    # Add ID and create new columns based on conditions
    df = df.withColumn("id", monotonically_increasing_id()) \
           .withColumn("Start_Sit_Recommendation",
                       when(col("START_SIT") == "A+", "Must Start")
                       .when(col("START_SIT") == "A", "Consider Starting")
                       .otherwise("Bench"))

    # Save the transformed data as a Delta table
    df.write.format("delta").mode("overwrite").saveAsTable("WRRankings_delta")

    # Log the output of the transformation
    num_rows = df.count()
    print(f"Number of rows in the transformed dataset: {num_rows}")
    log_output("load data", df.limit(10).toPandas().to_markdown())

    return "Transformation and loading completed successfully."


def query_transform():
    """Runs a query on the transformed data."""
    spark = SparkSession.builder.appName("Run Query on WRRankings").getOrCreate()
    query = """
        SELECT TEAM, COUNT(*) AS player_count 
        FROM WRRankings_delta 
        GROUP BY TEAM 
        ORDER BY player_count DESC
    """
    query_result = spark.sql(query)
    log_output("query data", 
               query_result.limit(10).toPandas().to_markdown(), 
               query=query)
    query_result.show()
    return query_result

# Run the ETL pipeline
if __name__ == "__main__":
    extract()  # Step 1: Extract data
    transform_and_load()  # Step 2: Transform and load the data
    query_transform()  # Step 3: Query the transformed data
