import math
import os
import typing
from pathlib import Path
from typing import Dict

import pandas as pd
import requests
from dotenv import load_dotenv

# Loads .env variables
load_dotenv()

# Define the Directus instance, mail and password from .env
directus_instance = os.getenv("DIRECTUS_INSTANCE")
directus_login = f"{directus_instance}/auth/login"

# Define the collection name and API url
collection_name = "Curation_Data"
directus_api = f"{directus_instance}/items/{collection_name}/"
directus_email = os.getenv("DIRECTUS_EMAIL")
directus_password = os.getenv("DIRECTUS_PASSWORD")

# To obtain actual path to inat_fetcher dir
p = Path(__file__).parents[1]

# Load dataframe
data_out_path = "/data/out/"
output_filename = "inat_observations_treated"
filename_suffix = "csv"
path_to_output_file = os.path.join(str(p) + data_out_path, output_filename + "." + filename_suffix)
df = pd.read_csv(path_to_output_file)

# Create a session object for making requests
session = requests.Session()

# Send a POST request to the login endpoint
response = session.post(directus_login, json={"email": directus_email, "password": directus_password})

geo_prefix = '"type":"Point","coordinates":'

# Test if connection is successful
if response.status_code == 200:
    # Stores the access token
    data = response.json()["data"]
    directus_token = data["access_token"]

    # Construct headers with authentication token
    headers = {"Authorization": f"Bearer {directus_token}", "Content-Type": "application/json"}

    # Create an empty dictionary to store the fields to create
    observation: Dict[str, typing.Any] = {}

    # Format each observation for directus
    for col_name in df.columns:
        # Replace dots with underscores in field names
        new_col_name = col_name.replace(".", "_")
        # Add to the dictionary
        observation[new_col_name] = None  # Initialize with None

    # Iterate over each row in the DataFrame
    for i in range(len(df)):
        # Convert each row to a dictionary
        obs = df.iloc[i].to_dict()

        # Convert problematic float values
        for key, value in obs.items():
            if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
                obs[key] = None if math.isnan(value) else str(value)

        # Update the observation dictionary with values from the current row
        for col_name, value in obs.items():
            if col_name == "geojson.coordinates" and value:
                observation[col_name.replace(".", "_")] = "{" + geo_prefix + value + "}"
            else:
                observation[col_name.replace(".", "_")] = value

        # Send the POST request to create or update the fields
        response = session.post(url=directus_api, headers=headers, json=observation)
        # Check if the request was successful
        if response.status_code != 200:
            directus_observation = f"{directus_api}{obs['id']}"
            response2 = session.patch(url=directus_observation, headers=headers, json=observation)
            if response2.status_code != 200:
                print(f"Error: {response2.status_code} - {response2.text}")
                print(obs["emi_external_id"])
