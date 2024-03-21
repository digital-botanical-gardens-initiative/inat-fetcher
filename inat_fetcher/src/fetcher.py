from pyinaturalist import *
from dotenv import load_dotenv
from pathlib import Path
import os

# Loads .env variables
load_dotenv()

# To be sure to be placed as repo directory level
p = Path(__file__).parents[1]
print(p)
os.chdir(p)

# Set up paths and filenames
data_out_path = "./data/out/"
output_filename = "inat_observations"
filename_suffix = 'csv'
path_to_output_file = os.path.join(data_out_path, output_filename + "." + filename_suffix)

# Import env variable
access_token=os.getenv('ACCESS_TOKEN')


