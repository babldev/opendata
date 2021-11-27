# opendata

Finds, downloads, parses, and standardizes public bikeshare data into a standard pandas dataframe format.

```python
import asyncio
from opendata.sources.bikeshare.bay_wheels import trips as bay_wheels

trips_df, _ = asyncio.run(bay_wheels.async_load(trip_sample_rate=1000))

len(trips_df.index)
# 8731

trips_df.columns
# Index(['started_at', 'ended_at', 'start_station_id', 'end_station_id',
#        'start_station_name', 'end_station_name', 'rideable_type', 'ride_id',
#        'start_lat', 'start_lng', 'end_lat', 'end_lng', 'gender', 'user_type',
#        'bike_id', 'birth_year'],
#       dtype='object')
```

An example analysis can be found here: https://observablehq.com/@brady/bikeshare

Supports sampling and local file caching to improve performance.

### Markets supported

```python
import opendata.sources.bikeshare.bay_wheels
import opendata.sources.bikeshare.bixi
import opendata.sources.bikeshare.divvy
import opendata.sources.bikeshare.capital_bikeshare
import opendata.sources.bikeshare.citi_bike
import opendata.sources.bikeshare.cogo
import opendata.sources.bikeshare.niceride
import opendata.sources.bikeshare.bluebikes
import opendata.sources.bikeshare.metro_bike_share
import opendata.sources.bikeshare.indego
```

## Bootstrap
Set up your environment
```bash
brew install chromedriver
brew install python3
python3 -m pip install pre-commit
```

```bash
pre-commit install --install-hooks
python3 -m venv venv
source venv/bin/activate
python3 -m pip install -r requirements.txt
```
## Entering virtualenv

```python
python3 -m venv venv
source venv/bin/activate
python3 -m pip install -r requirements.txt
```

## Usage

Try the test export to CSV:
```bash
python3 test.py
```

## Updating pip requirements

```python
pip-compile
```

## Pre-commit setup

```python
pre-commit install --install-hooks
```

## Bikeshare markets to add
### USA
- 119k/yr Pittsburgh (google drive links)
- 180k/yr Austin (date and time fields separate)

### World
- 3868k/yr Ecobici (need station CSV)
- 2900k/yr Toronto (needs more investigation)
- 650k/yr Vancouver (google drive links)
