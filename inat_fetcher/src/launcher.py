import os
import subprocess
from pathlib import Path

# To obtain actual path to inat_fetcher dir
p = Path(__file__).parents[1]

scripts_folder = "/src/"
path_to_scripts = os.path.join(str(p) + scripts_folder)

# Run fetcher
subprocess.run(["python", f"{path_to_scripts}fetcher.py"]).check_returncode()

# Run location formatter
subprocess.run(["python", f"{path_to_scripts}location_formatter.py"]).check_returncode()

# Run emi id extracter
subprocess.run(["python", f"{path_to_scripts}emi_id_extracter.py"]).check_returncode()

# Run create directus fields
subprocess.run(["python", f"{path_to_scripts}create_directus_fields.py"]).check_returncode()

# Run db updater
subprocess.run(["python", f"{path_to_scripts}db_updater.py"]).check_returncode()

# Run directus link maker
subprocess.run(["python", f"{path_to_scripts}directus_link_maker.py"]).check_returncode()