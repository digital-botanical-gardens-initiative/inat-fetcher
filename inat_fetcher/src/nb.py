import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from pyinaturalist import get_observations
from pyinaturalist_convert import to_dataframe

from datetime import datetime, timedelta

# import ipyplot
from dateutil.relativedelta import relativedelta
from IPython.display import Image
from pyinaturalist import (
    ICONIC_TAXA,
    Observation,
    TaxonCount,
    UserCount,
    enable_logging,
    get_observation_histogram,
    get_observation_identifiers,
    get_observation_observers,
    get_observation_species_counts,
    get_observations,
    get_observations_by_id,
    update_observation,
    delete_observation,
    pprint,
)
from rich import print

enable_logging()

load_dotenv(dotenv_path=Path(__file__).parent / ".env")


access_token = os.getenv("INATURALIST_ACCESS_TOKEN_TODAY")

print(f"access_token: {access_token}")


# To obtain actual path to inat_fetcher dir
p = Path(__file__).parents[1]


USERNAME = "dbgi"


# We fetch observation 309960238

my_observations = get_observations_by_id([309962477], access_token=access_token)
print(my_observations)
pprint(my_observations[0])

my_observations = get_observations(tag='emi_external_id:dbgi_009910', user_id='dbgi', page='all')

pprint(my_observations)


resp = get_observations(q='emi_external_id:dbgi_009910', search_on='tags', user_id='dbgi', page='all')
print(len(resp['results']))
print(resp['results'][0]['tags']) # verify tag present

pprint(resp)