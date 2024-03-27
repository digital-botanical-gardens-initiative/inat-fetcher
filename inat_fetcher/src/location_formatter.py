import os
from pathlib import Path

import pandas as pd

# To obtain actual path to inat_fetcher dir
p = Path(__file__).parents[1]

# Set up paths and filenames
data_in_path = "/data/in/"
input_filename = "inat_observations_raw"
output_filename = "inat_observations_loc"
filename_suffix = "csv"
path_to_input_file = os.path.join(str(p) + data_in_path, input_filename + "." + filename_suffix)
path_to_output_file = os.path.join(str(p) + data_in_path, output_filename + "." + filename_suffix)

# Load dataframe
df = pd.read_csv(path_to_input_file)

# Create a new column with all values initialized to 'NA'
df["swiped_loc"] = "NA"

# Use the apply method to process each row in the dataframe
df["swiped_loc"] = df["location"].apply(lambda x: tuple(map(float, x.strip("[]").split(",")))[::-1])

# We keep the table
df.to_csv(path_to_output_file, index=False)

print("csv correctly updated")
