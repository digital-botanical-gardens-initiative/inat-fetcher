import os
from pathlib import Path

import pandas as pd

# To obtain actual path to inat_fetcher dir
p = Path(__file__).parents[1]

# Set up paths and filenames
data_in_path = "/data/in/"
data_out_path = "/data/out/"
input_filename = "inat_observations_loc"
output_filename = "inat_observations_treated"
filename_suffix = "csv"
path_to_input_file = os.path.join(str(p) + data_in_path, input_filename + "." + filename_suffix)
path_to_output_file = os.path.join(str(p) + data_out_path, output_filename + "." + filename_suffix)

# Load dataframe
df = pd.read_csv(path_to_input_file)

# Create a new column with all values initialized to 'NA'
df["emi_external_id"] = "NA"

# loop over each line
for i in range(len(df)):
    # if the ofvs line is not empty
    if df.tags[i] != "[]":
        cleaned_string = df.tags[i][2:-2]
        print(cleaned_string)

# We keep the table
df.to_csv(path_to_output_file, index=False)

print("csv correctly updated")
