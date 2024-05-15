import os
from pathlib import Path

import pandas as pd
from pyinaturalist import get_observations
from pyinaturalist_convert import to_dataframe

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

response_dbgi = get_observations(user_id="dbgi", page="all", per_page=200, access_token=access_token)
df_dbgi = to_dataframe(response_dbgi)

response_edouardbruelhart = get_observations(
    user_id="edouardbruelhart", page="all", per_page=200, access_token=access_token
)
df_edouardbruelhart = to_dataframe(response_edouardbruelhart)

response_edouardbrulhart = get_observations(
    user_id="edouard-brulhart", page="all", per_page=200, access_token=access_token
)
df_edouardbrulhart = to_dataframe(response_edouardbrulhart)

response_manu_dfz = get_observations(user_id="manu_dfz", page="all", per_page=200, access_token=access_token)
df_manu_dfz = to_dataframe(response_manu_dfz)

response_lenditaschwegler = get_observations(
    user_id="lenditaschwegler", page="all", per_page=200, access_token=access_token
)
df_lenditaschwegler = to_dataframe(response_lenditaschwegler)

response_guetchuengst = get_observations(user_id="guetchuengst", page="all", per_page=200, access_token=access_token)
df_guetchuengst = to_dataframe(response_guetchuengst)

# Merge iNaturalist data
df = pd.concat(
    [df_project, df_dbgi, df_edouardbruelhart, df_edouardbrulhart, df_manu_dfz, df_lenditaschwegler, df_guetchuengst],
    ignore_index=True,
)

# shift column 'id' to first position
first_column = df.pop("id")

# insert column using insert(position,column_name,
df.insert(0, "id", first_column)

# Drop duplicates
df = df.drop_duplicates(subset=["id"])

# Write the table as CSV
df.to_csv(path_to_output_file, index=False)

print("csv correctly written")
