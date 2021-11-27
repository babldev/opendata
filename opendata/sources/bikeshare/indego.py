from opendata.sources.bikeshare import BikeshareCSVDataSource
from opendata.sources.bikeshare import create_stations_parsers
from opendata.sources.bikeshare import create_trips_parsers


trips = BikeshareCSVDataSource(
    data_url="https://www.rideindego.com/about/data/",
    info_url="https://www.rideindego.com/about/data/",
    license_url="https://www.rideindego.com/terms-and-conditions/",
    license_name="Terms and Conditions",
    trips_parsers=create_trips_parsers(
        started_at__cols=["start_time"],
        ended_at__cols=["end_time"],
        start_station_id__cols=["start_station_id", "start_station"],
        end_station_id__cols=["end_station_id", "end_station"],
        start_station_name__cols=[],
        end_station_name__cols=[],
        rideable_type__cols=["bike_type"],
        ride_id__cols=[],
        start_lat__cols=["start_lat"],
        start_lng__cols=["start_lon"],
        end_lat__cols=["end_lat"],
        end_lng__cols=["end_lon"],
        gender__cols=[],
        user_type__cols=["passholder_type"],
        bike_id__cols=["bike_id"],
        birth_year__cols=[],
    ),
    stations_parsers=create_stations_parsers(
        id__cols=["Station_ID"],
        name__cols=["Station_Name"],
        lat__cols=[],
        lng__cols=[],
        created_at__cols=["Day of Go_live_date"],
        is_active__cols=["Status"],
    ),
    ignore_cols={"trip_route_category", "plan_duration", "trip_id", "duration"},
)
