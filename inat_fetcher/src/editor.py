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


update_observation(
    309816396,
    access_token=access_token,
    # description='Original observer: @ifedenat25',
    tag_list=['emi_determiner:DBGI']
)


response = get_observation_histogram(
    interval='month',
    d1='2020-01-01',
    d2='2020-12-31',
    place_id=8057,
)

response = get_observations_by_id(309816396, access_token=access_token, refresh=True)
obs = Observation.from_json_list(response)[0]

print(obs)
new_tags = sorted(set(obs.tags + ['emi_determiner:DBGI']))

update_observation(
    309816396,
    access_token=access_token,
    tag_list=','.join(new_tags)
)


# response = get_observations(
#     taxon_name='Danaus plexippus',
#     created_on='2020-08-27',
#     photos=True,
#     geo=True,
#     geoprivacy='open',
#     place_id=7953,
# )

# Fetch all observation made by user dbgi for in Jardin botanique de Champex place id = 220507

response = get_observations(user_id=USERNAME, place_id=220507, page="all")
my_observations = Observation.from_json_list(response)
print(f"Number of observations in Jardin botanique de Champex: {len(my_observations)}")



pprint(my_observations[0])
print(my_observations[0])



# We fetch the list of observation in Jardin botanique de Champex with place_id=220507 and for which     num_identification_disagreements is > 0
# Parameter "identifications" must have one of the following values: ['most_agree', 'most_disagree', 'some_agree']

response = get_observations(user_id=USERNAME, place_id=220507, identifications=['some_agree'], page="all")
my_observations_some_agree = Observation.from_json_list(response)
print(f"Number of observations in Jardin botanique de Champex with identifications: {len(my_observations_some_agree)}")

pprint(my_observations_some_agree[:])
print(my_observations_some_agree[0])

# pyinaturalist.v1.observations.delete_observation(observation_id, access_token=None, **params)
# Delete an observation

# Notes

#  Requires authentication

# API reference: DELETE /observations/{id}

# Parameters:
# observation_id (int) – iNaturalist observation ID

# access_token (Optional[str]) – An access token for user authentication, as returned by get_access_token()

# dry_run (Optional[bool]) – Just log the request instead of sending a real request

# session (Optional[Session]) –

# An existing Session object to use instead of creating a new one

# Example

# token = get_access_token()
# delete_observation(17932425, token)
# Returns:
# If successful, no response is returned from this endpoint

# Raises:
# .ObservationNotFound –

# requests.HTTPError –

# trouble shooting.
# list observation overlapping between my_observations and my_observations_some_agree
set_my_observations = set([obs.id for obs in my_observations])
set_my_observations_some_agree = set([obs.id for obs in my_observations_some_agree])
overlapping_observations = set_my_observations.intersection(set_my_observations_some_agree)
print(f"Number of overlapping observations: {len(overlapping_observations)}")
print(f"Overlapping observations: {overlapping_observations}")

# OK that seems to work. NMow we delete all observation in my_observations which are NOT in my_observations_some_agree
for obs in my_observations:
    if obs.id in overlapping_observations:
        print(f"Keeping observation {obs.id} which is in my_observations_some_agree")
    else:
        print(f"Deleting observation {obs.id} which has {len(obs.identifications)} identifications")
        # pyinaturalist.v1.observations.delete_observation(observation_id, access_token=None, **params)
        delete_observation(obs.id, access_token=access_token, dry_run=False)
        # delete_observation(obs.id, access_token=access_token, dry_run=True)
        print(f"Observation {obs.id} deleted")

delete_observation(290121478, access_token=access_token, dry_run=True)
