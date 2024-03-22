import os
from pathlib import Path

# import db_updater
from dotenv import load_dotenv
from pyinaturalist import get_observations
from pyinaturalist_convert import to_dataframe

# import format_module

# Loads .env variables
load_dotenv()

# To obtain actual path to inat_fetcher dir
p = Path(__file__).parents[1]

# Set up paths and filenames
data_out_path = "/data/out/"
output_filename = "inat_observations"
filename_suffix = "csv"
path_to_output_file = os.path.join(str(p) + data_out_path, output_filename + "." + filename_suffix)

# import env variable
access_token = os.getenv("ACCESS_TOKEN")

response = get_observations(project_id=130644, page="all", per_page=200, access_token=access_token)
df = to_dataframe(response)

# Before exporting we move the id column to the beginning since it is needed to be at this position to be detected as a PK in airtbale or siomnilar dbs

# shift column 'id' to first position
first_column = df.pop("id")

# insert column using insert(position,column_name,
# first_column) function
df.insert(0, "id", first_column)

# formatting of data
# format_module.location_formatting(df,'location','swiped_loc')
# format_module.dbgi_id_extract(df)

# We keep the table
df.to_csv(path_to_output_file, index=False)


# update the database using db_updater.py script
if os.path.exists(path_to_output_file):
    print("path: ${path_to_output_file}")
    # db_updater
else:
    print("csv generation error")
