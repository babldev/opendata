from opendata.sources.bikeshare import BikeshareCSVDataSource
from opendata.sources.bikeshare import create_trips_parsers


trips = BikeshareCSVDataSource(
    data_url="https://s3.amazonaws.com/tripdata/index.html",
    info_url="https://ride.citibikenyc.com/system-data",
    license_url="https://ride.citibikenyc.com/data-sharing-policy",
    license_name="NYCBS Data Use Policy",
    trips_parsers=create_trips_parsers(
        started_at__cols=["starttime", "Start Time", "started_at"],
        ended_at__cols=["stoptime", "Stop Time", "ended_at"],
        start_station_id__cols=[
            "start station id",
            "Start Station ID",
            "start_station_id",
        ],
        end_station_id__cols=["end station id", "End Station ID", "end_station_id"],
        start_station_name__cols=[
            "start station name",
            "Start Station Name",
            "start_station_name",
        ],
        end_station_name__cols=[
            "end station name",
            "End Station Name",
            "end_station_name",
        ],
        rideable_type__cols=["rideable_type"],
        ride_id__cols=["ride_id"],
        start_lat__cols=[
            "start_lat",
            "start station latitude",
            "Start Station Latitude",
        ],
        start_lng__cols=[
            "start_lng",
            "start station longitude",
            "Start Station Longitude",
        ],
        end_lat__cols=["end_lat", "end station latitude", "End Station Latitude"],
        end_lng__cols=["end_lng", "end station longitude", "End Station Longitude"],
        gender__cols=["Gender", "gender"],
        user_type__cols=["User Type", "usertype", "member_casual"],
        bike_id__cols=["bikeid", "Bike ID"],
        birth_year__cols=["birth year", "Birth Year"],
    ),
    ignore_cols={"tripduration", "Trip Duration"},
)
