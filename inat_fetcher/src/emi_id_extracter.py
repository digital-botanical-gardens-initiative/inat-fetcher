import os
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

# To obtain actual path to inat_fetcher dir
p = Path(__file__).parents[1]

# Set up paths and filenames
data_in_path = "/data/in/"
data_out_path = "/data/out/"
input_filename = "inat_observations_raw"
output_filename = "inat_observations_treated"
recovery_filename = "inat_observation_recovery"
filename_suffix = ".csv"
path_to_input_file = os.path.join(str(p) + data_in_path, input_filename + filename_suffix)
path_to_output_file = os.path.join(str(p) + data_out_path, output_filename + filename_suffix)
path_to_recovery_file = os.path.join(str(p) + data_out_path, recovery_filename + filename_suffix)

# Request to directus to obtain projects codes
url = os.getenv("DIRECTUS_INSTANCE")
collection_url = f"{url}/items/Projects"
column = "project_id"
params = {"sort[]": f"{column}"}
session = requests.Session()
response = session.get(collection_url, params=params)
data = response.json()["data"]
project_names = [item[column] for item in data]

# Aggregate patterns
pattern = "(" + "|".join(project_names) + ")_[0-9]{6}"
pattern_all = "(" + "|".join(project_names) + ")_[0-9]{6}|dbgi_spl_[0-9]{6}"

# Load dataframe
df = pd.read_csv(path_to_input_file)

# Create a new column with all values initialized to 'NA'
df["emi_external_id"] = "NA"

# loop over each line
for i in range(len(df)):
    # if the 'tags' line is not empty
    if df.tags[i] != "[]":
        cleaned_string = str.replace(df.tags[i][2:-2], "emi_external_id:", "")
        df.loc[i, "emi_external_id"] = cleaned_string
    elif df["ofvs.15466"][i] != "":
        df.loc[i, "emi_external_id"] = str(df["ofvs.15466"][i])

# Split dataframe based on emi_external_id column matching the pattern

pattern_matched_df = df[df["emi_external_id"].str.match(str(pattern_all))]
pattern_matched_df["emi_external_id"] = pattern_matched_df["emi_external_id"].replace(r"dbgi_spl_", "dbgi_", regex=True)
pattern_unmatched_df = df[~df["emi_external_id"].str.match(str(pattern_all))]

# We keep the tables
pattern_matched_df.to_csv(path_to_output_file, index=False)
pattern_unmatched_df.to_csv(path_to_recovery_file, index=False)

print("csv correctly updated")
