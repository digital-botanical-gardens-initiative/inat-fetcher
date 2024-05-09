import os
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

# Loads .env variables
load_dotenv()

# Define the Directus instance, mail and password from .env
directus_instance = os.getenv("DIRECTUS_INSTANCE")
directus_login = f"{directus_instance}/auth/login"

# Define the collection name and API url
collection_name = "Inaturalist_Data_Test"
directus_api = f"{directus_instance}/items/{collection_name}/"
directus_email = os.getenv("DIRECTUS_EMAIL")
directus_password = os.getenv("DIRECTUS_PASSWORD")

# Create a session object for making requests
session = requests.Session()

# Send a POST request to the login endpoint
response = session.post(directus_login, json={"email": directus_email, "password": directus_password})

# Test if connection is successful
if response.status_code == 200:
    # Stores the access token
    data = response.json()["data"]
    directus_token = data["access_token"]

    # Construct headers with authentication token
    headers = {
        "Authorization": f"Bearer {directus_token}",
        "Content-Type": "application/json",
    }

# To obtain actual path to inat_fetcher dir
p = Path(__file__).parents[1]

# Load dataframe
data_out_path = "/data/out/"
output_filename = "inat_observations_treated"
filename_suffix = "csv"
path_to_output_file = os.path.join(str(p) + data_out_path, output_filename + "." + filename_suffix)
df = pd.read_csv(path_to_output_file)

# Define the threshold for text length
threshold = 255

# Create an empty dictionary to store the biggest values of each column
longest_content = {}

# Create an empty dictionary to store the fields to create
observation = {}

# Loop over the columns to create the dict
for col_name in df.columns:
    # Replace dots with underscores in field names
    new_col_name = col_name.replace(".", "_")
    # Add to the dictionary
    observation[new_col_name] = col_name

    # Find the longest content in the column
    longest = df[col_name].astype(str).apply(len).max()

    # Store the longest content for the column
    longest_content[new_col_name] = longest


# Request directus to create the columns
for i in observation:
    col_init = str.replace(str(observation[i]), "['", "")
    col = str.replace(col_init, "']", "")
    col_clean = str.replace(col, ".", "_")
    df_type = str(df[col].dtype)
    df_col_name = df[col].name

    # Replace types to match directus ones
    if df_type == "object" and longest_content[i] < threshold:
        dir_type = "string"
    elif df_type == "int64" and longest_content[i] < threshold:
        dir_type = "integer"
    elif df_type == "bool" and longest_content[i] < threshold:
        dir_type = "boolean"
    elif df_type == "float64" and longest_content[i] < threshold:
        dir_type = "float"
    elif df_col_name == "geojson.coordinates" or df_col_name == "swiped_loc":
        dir_type = "geometry.Point"
    elif longest_content[i] >= threshold:
        dir_type = "text"
    else:
        # If type is not handled by the ones already made, print it so we can integrate it easily
        print(f"not handled type: {type}")
        

    # Construct directus url
    url = f"{directus_instance}/fields/{collection_name}"
    # Create a field for each csv column
    data = {"field": col_clean, "type": dir_type}
    # Make directus request
    response = requests.post(url, json=data, headers=headers, timeout=10)
    # Check if adding is success
    if response.status_code == 200:
        print("yes")
    # else print the type and the column name
    elif response.status_code == 400:
        print("field already created")
    else:
        print(response.status_code)
        print(dir_type)
        print(col_clean)
