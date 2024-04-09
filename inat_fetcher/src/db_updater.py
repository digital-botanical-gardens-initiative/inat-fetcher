import json
import os

import requests
from dotenv import load_dotenv

# Loads .env variables
load_dotenv()

# Define the Directus instance, mail and password from .env
directus_instance = os.getenv("DIRECTUS_INSTANCE")
directus_login = directus_instance + "/auth/login"
# Define the collection name
collection_name = "inaturalist_data"
directus_api = directus_instance + "/items/" + collection_name
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

# Define the fields to create
fields = [
    {"field": "id", "type": "numeric"},
    {"field": "quality_grade", "type": "string", "length": 25},
    {"field": "time_observed_at", "type": "datetime"},
    {"field": "taxon_geoprivacy", "type": "string", "length": 25},
    {"field": "annotations", "type": "string", "length": 25},
    {"field": "uuid", "type": "text"},
    {"field": "cached_votes_total", "type": "numeric"},
    {"field": "identifications_most_agree", "type": "boolean"},
    {"field": "species_guess", "type": "string", "length": 100},
    {"field": "identifications_most_disagree", "type": "boolean"},
    {"field": "tags", "type": "string", "length": 150},
    {"field": "positional_accuracy", "type": "numeric"},
    {"field": "comments_count", "type": "numeric"},
    {"field": "site_id", "type": "boolean"},
    {"field": "license_code", "type": "string", "length": 25},
    {"field": "quality_metrics", "type": "text"},
    {"field": "public_positional_accuracy", "type": "numeric"},
    {"field": "reviewed_by", "type": "string", "length": 60},
    {"field": "oauth_application_id", "type": "numeric"},
    {"field": "flags", "type": "string", "length": 25},
    {"field": "created_at", "type": "datetime"},
    {"field": "description", "type": "text"},
    {"field": "project_ids_with_curator_id", "type": "string", "length": 25},
    {"field": "updated_at", "type": "datetime"},
    {"field": "sounds", "type": "string", "length": 25},
    {"field": "place_ids", "type": "string", "length": 250},
    {"field": "captive", "type": "boolean"},
    {"field": "ident_taxon_ids", "type": "text"},
    {"field": "outlinks", "type": "text"},
    {"field": "faves_count", "type": "numeric"},
    {"field": "ofvs", "type": "text"},
    {"field": "num_identification_agreements", "type": "numeric"},
    {"field": "comments", "type": "text"},
    {"field": "map_scale", "type": "numeric"},
    {"field": "uri", "type": "string", "length": 250},
    {"field": "project_ids", "type": "string", "length": 25},
    {"field": "community_taxon_id", "type": "numeric"},
    {"field": "owners_identification_from_vision", "type": "boolean"},
    {"field": "identifications_count", "type": "numeric"},
    {"field": "obscured", "type": "boolean"},
    {"field": "num_identification_disagreements", "type": "numeric"},
    {"field": "geoprivacy", "type": "boolean"},
    {"field": "location", "type": "string", "length": 100},
    {"field": "votes", "type": "text"},
    {"field": "spam", "type": "boolean"},
    {"field": "mappable", "type": "boolean"},
    {"field": "identifications_some_agree", "type": "boolean"},
    {"field": "project_ids_without_curator_id", "type": "string", "length": 25},
    {"field": "place_guess", "type": "text"},
    {"field": "identifications", "type": "text"},
    {"field": "project_observations", "type": "text"},
    {"field": "photos", "type": "text"},
    {"field": "faves", "type": "text"},
    {"field": "non_owner_ids", "type": "text"},
    {"field": "observed_on", "type": "datetime"},
    {"field": "photo_url", "type": "text"},
    {"field": "taxon_is_active", "type": "boolean"},
    {"field": "taxon_ancestry", "type": "text"},
    {"field": "taxon_min_species_ancestry", "type": "text"},
    {"field": "taxon_endemic", "type": "boolean"},
    {"field": "taxon_iconic_taxon_id", "type": "numeric"},
    {"field": "taxon_min_species_taxon_id", "type": "numeric"},
    {"field": "taxon_threatened", "type": "boolean"},
    {"field": "taxon_rank_level", "type": "numeric"},
    {"field": "taxon_introduced", "type": "boolean"},
    {"field": "taxon_native", "type": "boolean"},
    {"field": "taxon_parent_id", "type": "numeric"},
    {"field": "taxon_name", "type": "string", "length": 100},
    {"field": "taxon_rank", "type": "string", "length": 25},
    {"field": "taxon_extinct", "type": "boolean"},
    {"field": "taxon_id", "type": "numeric"},
    {"field": "taxon_ancestor_ids", "type": "text"},
    {"field": "taxon_photos_locked", "type": "boolean"},
    {"field": "taxon_taxon_schemes_count", "type": "numeric"},
    {"field": "taxon_wikipedia_url", "type": "text"},
    {"field": "taxon_current_synonymous_taxon_ids", "type": "text"},
    {"field": "taxon_created_at", "type": "datetime"},
    {"field": "taxon_taxon_changes_count", "type": "numeric"},
    {"field": "taxon_complete_species_count", "type": "boolean"},
    {"field": "taxon_universal_search_rank", "type": "numeric"},
    {"field": "taxon_observations_count", "type": "numeric"},
    {"field": "taxon_flag_counts_resolved", "type": "numeric"},
    {"field": "taxon_flag_counts_unresolved", "type": "numeric"},
    {"field": "taxon_atlas_id", "type": "string", "length": 50},
    {"field": "taxon_default_photo_id", "type": "numeric"},
    {"field": "taxon_default_photo_license_code", "type": "string", "length": 25},
    {"field": "taxon_default_photo_attribution", "type": "text"},
    {"field": "taxon_default_photo_url", "type": "text"},
    {"field": "taxon_default_photo_original_dimensions_height", "type": "numeric"},
    {"field": "taxon_default_photo_original_dimensions_width", "type": "numeric"},
    {"field": "taxon_default_photo_flags", "type": "text"},
    {"field": "taxon_default_photo_square_url", "type": "text"},
    {"field": "taxon_default_photo_medium_url", "type": "text"},
    {"field": "taxon_iconic_taxon_name", "type": "string", "length": 25},
    {"field": "taxon_preferred_common_name", "type": "string", "length": 100},
    {"field": "preferences_prefers_community_taxon", "type": "boolean"},
    {"field": "geojson_coordinates", "type": "string", "length": 100},
    {"field": "geojson_type", "type": "string", "length": 25},
    {"field": "user_site_id", "type": "numeric"},
    {"field": "user_created_at", "type": "datetime"},
    {"field": "user_id", "type": "numeric"},
    {"field": "user_login", "type": "string", "length": 25},
    {"field": "user_spam", "type": "boolean"},
    {"field": "user_suspended", "type": "boolean"},
    {"field": "user_login_autocomplete", "type": "string", "length": 25},
    {"field": "user_login_exact", "type": "string", "length": 25},
    {"field": "user_name", "type": "string", "length": 25},
    {"field": "user_name_autocomplete", "type": "string", "length": 25},
    {"field": "user_orcid", "type": "boolean"},
    {"field": "user_icon", "type": "text"},
    {"field": "user_observations_count", "type": "numeric"},
    {"field": "user_identifications_count", "type": "numeric"},
    {"field": "user_journal_posts_count", "type": "numeric"},
    {"field": "user_activity_count", "type": "numeric"},
    {"field": "user_species_count", "type": "numeric"},
    {"field": "user_universal_search_rank", "type": "numeric"},
    {"field": "user_roles", "type": "string", "length": 25},
    {"field": "user_icon_url", "type": "text"},
    {"field": "taxon_default_photo", "type": "boolean"},
    {"field": "taxon_conservation_status_place_id", "type": "boolean"},
    {"field": "taxon_conservation_status_source_id", "type": "numeric"},
    {"field": "taxon_conservation_status_user_id", "type": "boolean"},
    {"field": "taxon_conservation_status_authority", "type": "string", "length": 250},
    {"field": "taxon_conservation_status_status", "type": "string", "length": 25},
    {"field": "taxon_conservation_status_status_name", "type": "string", "length": 50},
    {"field": "taxon_conservation_status_geoprivacy", "type": "string", "length": 50},
    {"field": "taxon_conservation_status_iucn", "type": "string", "length": 25},
    {"field": "observed_on_details", "type": "boolean"},
    {"field": "created_time_zone", "type": "string", "length": 100},
    {"field": "observed_time_zone", "type": "string", "length": 100},
    {"field": "time_zone_offset", "type": "string", "length": 100},
    {"field": "observed_on_string", "type": "boolean"},
    {"field": "created_at_details_date", "type": "datetime"},
    {"field": "created_at_details_week", "type": "numeric"},
    {"field": "created_at_details_month", "type": "numeric"},
    {"field": "created_at_details_hour", "type": "numeric"},
    {"field": "created_at_details_year", "type": "numeric"},
    {"field": "created_at_details_day", "type": "numeric"},
    {"field": "swiped_loc", "type": "string", "length": 100},
    {"field": "emi_external_id", "type": "string", "length": 50},
]

# Construct the request payload
payload = {"fields": fields}

# Construct headers with authentication token
headers = {"Authorization": f"Bearer {directus_token}", "Content-Type": "application/json"}

# Construct the API endpoint for creating fields
endpoint = f"{DIRECTUS_URL}/tables/{COLLECTION_NAME}/fields"

# Send the POST request to create the fields
response = requests.post(endpoint, headers=headers, data=json.dumps(payload))

# Check if the request was successful
if response.status_code == 200:
    print("Fields created successfully.")
else:
    print(f"Error: {response.status_code} - {response.text}")
