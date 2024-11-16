import os
from dotenv import load_dotenv


# Load environment variables
load_dotenv()

def test_spark_connection():
    # Fetch configuration from .env
    server_hostname = os.getenv('SERVER_HOSTNAME')
    access_token = os.getenv('ACCESS_TOKEN')
    http_path = os.getenv('HTTP_PATH')

    # Check if variables are properly loaded
    assert server_hostname, "SERVER_HOSTNAME is not set in .env"
    assert access_token, "ACCESS_TOKEN is not set in .env"
    assert http_path, "HTTP_PATH is not set in .env"

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("TestSparkConnection") \
        .config("spark.databricks.service.serverHostname", server_hostname) \
        .config("spark.databricks.service.token", access_token) \
        .config("spark.databricks.service.httpPath", http_path) \
        .getOrCreate()

    try:
        # Test a simple query or operation
        df = spark.range(10)
        assert df.count() == 10, "Test query failed!"
        print("Spark connection test passed.")
    finally:
        spark.stop()

if __name__ == "__main__":
    test_spark_connection()
