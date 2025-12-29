import os
import ast
import re
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

# To obtain actual path to inat_fetcher dir
p = Path(__file__).parents[1]

# Set up paths and filenames
data_in_path = p / "data" / "in"
data_out_path = p / "data" / "out"
data_out_path.mkdir(parents=True, exist_ok=True)
input_filename = "inat_observations_raw"
output_filename = "inat_observations_treated"
recovery_filename = "inat_observation_recovery"
filename_suffix = ".csv"
path_to_input_file = data_in_path / f"{input_filename}{filename_suffix}"
path_to_output_file = data_out_path / f"{output_filename}{filename_suffix}"
path_to_recovery_file = data_out_path / f"{recovery_filename}{filename_suffix}"

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
df = pd.read_csv(path_to_input_file, low_memory=False)

# Create a new column with all values initialized to 'NA'
df["emi_external_id"] = "NA"

def extract_emi_external_id(tags_value, ofvs_value):
    if isinstance(tags_value, str) and tags_value not in ("", "[]"):
        try:
            tags_list = ast.literal_eval(tags_value)
            if isinstance(tags_list, list):
                for tag in tags_list:
                    if isinstance(tag, str) and tag.startswith("emi_external_id:"):
                        return tag.split(":", 1)[1]
        except (ValueError, SyntaxError):
            pass
        match = re.search(r"emi_external_id:([^',\]]+)", tags_value)
        if match:
            return match.group(1)

    if pd.notna(ofvs_value) and str(ofvs_value) != "":
        return str(ofvs_value)

    return "NA"


# loop over each line
for i in range(len(df)):
    df.loc[i, "emi_external_id"] = extract_emi_external_id(df.tags[i], df["ofvs.15466"][i])

# Split dataframe based on emi_external_id column matching the pattern

pattern_matched_df = df[df["emi_external_id"].str.match(str(pattern_all))].copy()
pattern_matched_df["emi_external_id"] = pattern_matched_df["emi_external_id"].replace(r"dbgi_spl_", "dbgi_", regex=True)
pattern_unmatched_df = df[~df["emi_external_id"].str.match(str(pattern_all))]

# We keep the tables
pattern_matched_df.to_csv(path_to_output_file, index=False)
pattern_unmatched_df.to_csv(path_to_recovery_file, index=False)

print("csv correctly updated")
