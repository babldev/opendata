# opendata

Finds, downloads, parses, and standardizes public bikeshare data into a standard CSV format.

```sh
python3 market_to_csv.py bay_wheels --sample-rate 1000
```

An example analysis can be found here: https://observablehq.com/@brady/bikeshare-2023

Supports sampling, local file caching, and networking retries to improve performance.

### Markets supported

```sh
python3 market_to_csv.py bay_wheels
python3 market_to_csv.py bixi
python3 market_to_csv.py divvy
python3 market_to_csv.py capital_bikeshare
python3 market_to_csv.py citi_bike
python3 market_to_csv.py cogo
python3 market_to_csv.py niceride
python3 market_to_csv.py bluebikes
python3 market_to_csv.py metro_bike_share
python3 market_to_csv.py indego
```

## Bootstrap
Set up your environment
```sh
brew install chromedriver
brew install python3
python3 -m pip install pre-commit
```

```sh
pre-commit install --install-hooks
python3 -m venv venv
source venv/bin/activate
python3 -m pip install -r requirements.txt
```

## Updating pip requirements

```sh
pip-compile
```

## Pre-commit setup

```sh
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
