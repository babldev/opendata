# opendata

Finds, downloads, parses, and standardizes public bikeshare data into a standard pandas dataframe format.

An example analysis can be found here: https://observablehq.com/@brady/bikeshare

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
