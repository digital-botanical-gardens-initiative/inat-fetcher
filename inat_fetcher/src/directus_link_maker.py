import os

import requests
from dotenv import load_dotenv

# Loads .env variables
load_dotenv()

# Define the Directus instance, mail and password from .env
directus_instance = os.getenv("DIRECTUS_INSTANCE")
directus_login = f"{directus_instance}/auth/login"

# Define the collection name and API url
collection_name = "Inat_Data"
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
    headers = {"Authorization": f"Bearer {directus_token}", "Content-Type": "application/json"}

    # Send the get request to create or update the fields
    response = session.get(url=f"{directus_api}?limit=-1", headers=headers)
    data = response.json()["data"]
    item_id = [item["id"] for item in data]
    emi_id = [item["emi_external_id"] for item in data]
    for i in range(len(item_id)):
        if emi_id[i]:
            directus_patch = f"{directus_instance}/items/Field_Samples/" + emi_id[i]
            inaturalist_link = "https://www.inaturalist.org/observations/" + item_id[i]
            observation = {"inat_observation_id": item_id[i], "inaturalist_link": inaturalist_link}
            response = session.patch(url=directus_patch, headers=headers, json=observation)
            if response.status_code != 200:
                print(f"error, couldn't make the link between {item_id[i]} and {emi_id[i]}")
                print(response.status_code)
                print(response.text)

    # Send get request to check if data has been added to directus. If not, set inat_observation_id and inaturalist_link to absent
    directus_api2 = f"{directus_instance}/items/Qfield_Data/"
    response2 = session.get(url=f"{directus_api2}?limit=-1", headers=headers)
    data2 = response2.json()["data"]
    item_id2 = [item["field_sample_name"] for item in data2]
    emi_id2 = [item["field_sample_id_pk"] for item in data2]
    for i in range(len(item_id2)):
        if item_id2[i] == "Mixte":
            print(item_id2[i])
            print(emi_id2[i])
            directus_patch2 = f"{directus_instance}/items/Field_Samples/" + emi_id2[i]
            observation2 = {"inat_observation_id": "absent", "inaturalist_link": "absent"}
            response2 = session.patch(url=directus_patch2, headers=headers, json=observation2)
