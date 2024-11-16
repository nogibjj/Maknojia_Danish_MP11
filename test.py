import requests
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Fetch environment variables
server_h = os.getenv("SERVER_HOSTNAME")
access_token = os.getenv("ACCESS_TOKEN")
FILESTORE_PATH = "dbfs:/FileStore/danish_mini_project11"

# Check if environment variables are loaded properly
if not server_h or not access_token:
    raise ValueError("Not set in the environment variables.")

# Construct the URL
url = f"https://{server_h}/api/2.0"

# Function to check if a file path exists and auth settings still work
def check_filestore_path(path, headers): 
    try:
        response = requests.get(url + f"/dbfs/get-status?path={path}", headers=headers)
        response.raise_for_status()
        
        # Check if 'path' key exists in response
        return response.json().get('path') is not None
    except requests.exceptions.RequestException as e:
        print(f"Error checking file path: {e}")
        return False

# Test if the specified FILESTORE_PATH exists
def test_databricks():
    headers = {'Authorization': f'Bearer {access_token}'}
    if check_filestore_path(FILESTORE_PATH, headers):
        print(f"File path {FILESTORE_PATH} exists.")
    else:
        print(f"File path {FILESTORE_PATH} does not exist")

if __name__ == "__main__":
    test_databricks()
