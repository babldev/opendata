from opendata.sources.bikeshare import BikeshareCSVDataSource
from opendata.sources.bikeshare import create_trips_parsers


trips = BikeshareCSVDataSource(
    data_url="https://s3.amazonaws.com/hubway-data/index.html",
    info_url="https://www.bluebikes.com/system-data",
    license_url="https://www.bluebikes.com/data-license-agreement",
    license_name="Bluebikes Data License Agreement",
    trips_parsers=create_trips_parsers(
        started_at__cols=["started_at", "starttime"],
        ended_at__cols=["ended_at", "stoptime"],
        start_station_id__cols=["start_station_id"],
        end_station_id__cols=["end_station_id", "end station id", "start station id"],
        start_station_name__cols=["start station name", "start_station_name"],
        end_station_name__cols=["end station name", "end_station_name"],
        rideable_type__cols=["rideable_type"],
        ride_id__cols=["ride_id"],
        start_lat__cols=["start_lat", "start station latitude"],
        start_lng__cols=["start_lng", "start station longitude"],
        end_lat__cols=["end_lat", "end station latitude"],
        end_lng__cols=["end_lng", "end station longitude"],
        gender__cols=["gender"],
        user_type__cols=["member_casual", "usertype"],
        bike_id__cols=["bikeid"],
        birth_year__cols=["birth year"],
    ),
    ignore_cols={"tripduration", "postal code"},
)
