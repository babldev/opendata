from opendata.sources.bikeshare import BikeshareCSVDataSource
from opendata.sources.bikeshare import create_stations_parsers
from opendata.sources.bikeshare import create_trips_parsers


trips = BikeshareCSVDataSource(
    data_url="https://bixi.com/en/open-data",
    info_url="https://bixi.com/en/open-data",
    license_url="https://bixi.com/en/website-terms",
    license_name="Terms and Conditions",
    trips_parsers=create_trips_parsers(
        started_at__cols=["start_date"],
        ended_at__cols=["end_date"],
        start_station_id__cols=["start_station_code", "emplacement_pk_start"],
        end_station_id__cols=["end_station_code", "emplacement_pk_end"],
        start_station_name__cols=[],
        end_station_name__cols=[],
        rideable_type__cols=[],
        ride_id__cols=[],
        start_lat__cols=[],
        start_lng__cols=[],
        end_lat__cols=[],
        end_lng__cols=[],
        gender__cols=[],
        user_type__cols=["is_member"],
        bike_id__cols=[],
        birth_year__cols=[],
    ),
    stations_parsers=create_stations_parsers(
        id__cols=["pk", "code", "Code"],
        name__cols=["name"],
        lat__cols=["latitude"],
        lng__cols=["longitude"],
        created_at__cols=[],
        is_active__cols=[],
    ),
    ignore_cols={"duration_sec"},
)
