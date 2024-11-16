import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Fetch a single environment variable
server_h = os.getenv("SERVER_HOSTNAME")

# Check if it's loaded properly
if not server_h:
    raise ValueError("SERVER_HOSTNAME is not set in the environment variables.")

print(f"SERVER_HOSTNAME: {server_h}")
