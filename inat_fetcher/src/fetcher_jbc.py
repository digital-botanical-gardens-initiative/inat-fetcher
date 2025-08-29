import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from pyinaturalist import get_observations
from pyinaturalist_convert import to_dataframe

load_dotenv()

# To obtain actual path to inat_fetcher dir
p = Path(__file__).parents[1]

# Set up paths and filenames
data_in_path = "/data/in/"
output_filename = "inat_observations_raw"
filename_suffix = "csv"
path_to_output_file = os.path.join(str(p) + data_in_path, output_filename + "." + filename_suffix)

# import env variable
access_token = os.getenv("INATURALIST_ACCESS_TOKEN")

# Fetch values from iNaturalist for the different users
response_project = get_observations(project_id=130644, page="all", per_page=200, access_token=access_token)
df_project = to_dataframe(response_project)

# Subset the df for comments is not empty (empty is [])
df_commented = df_project[df_project["comments"].map(len) > 0]


# subset df_project for observation id 305767127

df_305767127 = df_project[df_project["id"] == 305767127]

# save previous df_305767127 as csv

df_305767127.to_csv("./df_305767127.csv", index=False)


# subset df_project for taxon.atlas_id = 20357.0

df_atlas_20357 = df_project[df_project["taxon.atlas_id"] == 20357.0]






# Write the table as CSV
df.to_csv(path_to_output_file, index=False)

print("csv correctly written")
