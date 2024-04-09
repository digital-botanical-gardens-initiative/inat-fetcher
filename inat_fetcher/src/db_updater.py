import os
from pathlib import Path
import json

import requests
from dotenv import load_dotenv
import pandas as pd

# Loads .env variables
load_dotenv()

# Define the Directus instance, mail and password from .env
directus_instance = os.getenv("DIRECTUS_INSTANCE")
directus_login = f"{directus_instance}/auth/login"

# Define the collection name and API url
collection_name = "inaturalist_data"
directus_api = f"{directus_instance}/items/{collection_name}"
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
# Test if connection is successful
if response.status_code == 200:
    # Stores the access token
    data = response.json()["data"]
    directus_token = data["access_token"]

    # Construct headers with authentication token
    headers = {"Authorization": f"Bearer {directus_token}", "Content-Type": "application/json"}

    for i in range(len(df)):

        # Format each observation for directus
        observation = {
            "id": df['id'][i],
            "quality_grade": df['quality_grade'][i],
            "time_observed_at": df['observed_on_details.date'][i],
            "taxon_geoprivacy": df['taxon_geoprivacy'][i],
            "annotations": df['annotations'][i],
            "uuid": df['uuid'][i],
            "cached_votes_total": df['cached_votes_total'][i],
            "identifications_most_agree": df['identifications_most_agree'][i],
            "species_guess": df['species_guess'][i],
            "identifications_most_disagree": df['identifications_most_disagree'][i],
            "tags": df['tags'][i],
            "positional_accuracy": df['positional_accuracy'][i],
            "comments_count": df['comments_count'][i],
            "site_id": df['site_id'][i],
            "license_code": df['license_code'][i],
            "quality_metrics": df['quality_metrics'][i],
            "public_positional_accuracy": df['public_positional_accuracy'][i],
            "reviewed_by": df['reviewed_by'][i],
            "oauth_application_id": df['oauth_application_id'][i],
            "flags": df['flags'][i],
            "created_at": df['created_at'][i],
            "description": df['description'][i],
            "project_ids_with_curator_id": df['project_ids_with_curator_id'][i],
            "updated_at": df['updated_at'][i],
            "sounds": df['sounds'][i],
            "place_ids": df['place_ids'][i],
            "captive": df['captive'][i],
            "ident_taxon_ids": df['ident_taxon_ids'][i],
            "outlinks": df['outlinks'][i],
            "faves_count": df['faves_count'][i],
            "num_identification_agreements": df['num_identification_agreements'][i],
            "comments": df['comments'][i],
            "map_scale": df['map_scale'][i],
            "uri": df['uri'][i],
            "project_ids": df['project_ids'][i],
            "community_taxon_id": df['community_taxon_id'][i],
            "owners_identification_from_vision": df['owners_identification_from_vision'][i],
            "identifications_count": df['identifications_count'][i],
            "obscured": df['obscured'][i],
            "num_identification_disagreements": df['num_identification_disagreements'][i],
            "location": df['location'][i],
            "votes": df['votes'][i],
            "spam": df['spam'][i],
            "mappable": df['mappable'][i],
            "identifications_some_agree": df['identifications_some_agree'][i],
            "project_ids_without_curator_id": df['project_ids_without_curator_id'][i],
            "place_guess": df['place_guess'][i],
            "identifications": df['identifications'][i],
            "project_observations": df['project_observations'][i],
            "photos": df['photos'][i],
            "faves": df['faves'][i],
            "observed_on": df['observed_on'][i],
            "photo_url": df['photo_url'][i],
            "taxon_is_active": df['taxon.is_active'][i],
            "taxon_ancestry": df['taxon.ancestry'][i],
            "taxon_min_species_ancestry": df['taxon.min_species_ancestry'][i],
            "taxon_endemic": df['taxon.endemic'][i],
            "taxon_iconic_taxon_id": df['taxon.iconic_taxon_id'][i],
            "taxon_min_species_taxon_id": df['taxon.min_species_taxon_id'][i],
            "taxon_threatened": df['taxon.threatened'][i],
            "taxon_rank_level": df['taxon.rank_level'][i],
            "taxon_introduced": df['taxon.introduced'][i],
            "taxon_native": df['taxon.native'][i],
            "taxon_parent_id": df['taxon.parent_id'][i],
            "taxon_name": df['taxon.name'][i],
            "taxon_rank": df['taxon.rank'][i],
            "taxon_extinct": df['taxon.extinct'][i],
            "taxon_id": df['taxon.id'][i],
            "taxon_ancestor_ids": df['taxon.ancestor_ids'][i],
            "taxon_photos_locked": df['taxon.photos_locked'][i],
            "taxon_taxon_schemes_count": df['taxon.taxon_schemes_count'][i],
            "taxon_wikipedia_url": df['taxon.wikipedia_url'][i],
            "taxon_current_synonymous_taxon_ids": df['taxon.current_synonymous_taxon_ids'][i],
            "taxon_created_at": df['taxon.created_at'][i],
            "taxon_taxon_changes_count": df['taxon.taxon_changes_count'][i],
            "taxon_complete_species_count": df['taxon.observations_count'][i],
            "taxon_universal_search_rank": df['taxon.universal_search_rank'][i],
            "taxon_observations_count": df['taxon.observations_count'][i],
            "taxon_atlas_id": df['taxon.atlas_id'][i],
            "taxon_iconic_taxon_name": df['taxon.iconic_taxon_name'][i],
            "taxon_preferred_common_name": df['taxon.preferred_common_name'][i],
            "geojson_coordinates": df['geojson.coordinates'][i],
            "geojson_type": df['geojson.type'][i],
            "user_site_id": df['user.site_id'][i],
            "user_created_at": df['user.created_at'][i],
            "user_id": df['user.id'][i],
            "user_login": df['user.login'][i],
            "user_spam": df['user.spam'][i],
            "user_suspended": df['user.suspended'][i],
            "user_login_autocomplete": df['user.login_autocomplete'][i],
            "user_login_exact": df['user.login_exact'][i],
            "user_name": df['user.name'][i],
            "user_name_autocomplete": df['user.name_autocomplete'][i],
            "user_icon": df['user.icon'][i],
            "user_observations_count": df['user.observations_count'][i],
            "user_identifications_count": df['user.identifications_count'][i],
            "user_journal_posts_count": df['user.journal_posts_count'][i],
            "user_activity_count": df['user.activity_count'][i],
            "user_species_count": df['user.species_count'][i],
            "user_universal_search_rank": df['user.universal_search_rank'][i],
            "user_roles": df['user.roles'][i],
            "user_icon_url": df['user.icon_url'][i],
            "taxon_conservation_status_source_id": df['taxon.conservation_status.source_id'][i],
            "taxon_conservation_status_authority": df['taxon.conservation_status.authority'][i],
            "taxon_conservation_status_status": df['taxon.conservation_status.status'][i],
            "taxon_conservation_status_status_name": df['taxon.conservation_status.status_name'][i],
            "taxon_conservation_status_geoprivacy": df['taxon.conservation_status.geoprivacy'][i],
            "taxon_conservation_status_iucn": df['taxon.conservation_status.iucn'][i],
            "observed_on_details": df['observed_on_details.date'][i],
            "created_time_zone": df['created_time_zone'][i],
            "observed_time_zone": df['observed_time_zone'][i],
            "time_zone_offset": df['time_zone_offset'][i],
            "observed_on_string": df['observed_on_string'][i],
            "created_at_details_date": df['created_at_details.date'][i],
            "created_at_details_week": df['created_at_details.week'][i],
            "created_at_details_month": df['created_at_details.month'][i],
            "created_at_details_hour": df['created_at_details.hour'][i],
            "created_at_details_year": df['created_at_details.year'][i],
            "created_at_details_day": df['created_at_details.day'][i],
            "swiped_loc": df['swiped_loc'][i],
            "emi_external_id": df['emi_external_id'][i]
        }

        #json_observation = json.dumps(observation)

        # Send the POST request to create the fields
        response = requests.post(directus_api, headers=headers, data=observation, timeout=10)
        # Check if the request was successful
        if response.status_code == 200:
            print("observation correctly created")
        else:
            directus_observation = f"{directus_api}/{df['id'][i]}"
            response2 = requests.patch(directus_observation, headers=headers, data=observation, timeout=10)
            if response2.status_code == 200:
                print("observation correctly updated")
            else:
                print(f"Error: {response2.status_code} - {response2.text}")
