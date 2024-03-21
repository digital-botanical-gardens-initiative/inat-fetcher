from pyinaturalist import *
from dotenv import load_dotenv
from pathlib import Path
import os

#loads .env variables
load_dotenv()

#To be sure to be placed as repo directory level
p = Path(__file__).parents[1]
print(p)
os.chdir(p)