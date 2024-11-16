import os
import requests
import base64
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, monotonically_increasing_id
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    FloatType,
)

LOG_FILE = "pyspark_output.md"
FILESTORE_PATH = "dbfs:/FileStore/WRRankings"


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
    url = os.getenv("DATABRICKS_HOST")
    headers = {"Authorization": f"Bearer {os.getenv('DATABRICKS_TOKEN')}"}
    response = requests.request(
        method=method,
        url=f"{url}{path}",
        headers=headers,
        data=json.dumps(data) if data else None,
        verify=True,
    )
    return response.json()


def upload_file_from_url(url, dbfs_path, overwrite):
    """Uploads a file from a URL to DBFS."""
    response = requests.get(url)
    if response.status_code == 200:
        content = response.content
        # Create file handle
        handle = perform_request(
            "/dbfs/create", data={"path": dbfs_path, "overwrite": overwrite}
        )["handle"]
        for i in range(0, len(content), 2**20):
            perform_request(
                "/dbfs/add-block",
                data={
                    "handle": handle,
                    "data": base64.standard_b64encode(content[i : i + 2**20]).decode(),
                },
            )
        perform_request("/dbfs/close", data={"handle": handle})
    else:
        print("Failed to download")


# ETL Functions
def extract(
    source_url="https://raw.githubusercontent.com/nogibjj/Maknojia_Danish_MP5/refs/heads/main/data/WRRankingsWeek5.csv",
    target_path=f"{FILESTORE_PATH}/WRRankingsWeek5.csv",
    directory=FILESTORE_PATH,
    overwrite=True,
):
    """Extracts data and uploads it to DBFS."""
    perform_request("/dbfs/mkdirs", data={"path": directory})
    upload_file_from_url(source_url, target_path, overwrite)
    return target_path


def transform_and_load(dataset=f"{FILESTORE_PATH}/WRRankingsWeek5.csv"):
    """Transforms and loads data into Delta Lake."""
    spark = SparkSession.builder.appName("Transform and Load WRRankings").getOrCreate()

    schema = StructType(
        [
            StructField("RK", IntegerType(), True),
            StructField("PLAYER NAME", StringType(), True),
            StructField("TEAM", StringType(), True),
            StructField("OPP", StringType(), True),
            StructField("MATCHUP", StringType(), True),
            StructField("START/SIT", StringType(), True),
            StructField("PROJ. FPTS", FloatType(), True),
        ]
    )

    df = spark.read.csv(dataset, header=True, schema=schema)
    df = df.withColumnRenamed("PROJ. FPTS", "PROJ_FPTS")

    df = df.withColumn("id", monotonically_increasing_id()).withColumn(
        "Start_Sit_Recommendation",
        when(col("START/SIT") == "A+", "Must Start")
        .when(col("START/SIT") == "A", "Consider Starting")
        .otherwise("Bench"),
    )

    df.write.format("delta").mode("overwrite").saveAsTable("WRRankings_delta")
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
    log_output(
        "query data", query_result.limit(10).toPandas().to_markdown(), query=query
    )
    query_result.show()
    return query_result
