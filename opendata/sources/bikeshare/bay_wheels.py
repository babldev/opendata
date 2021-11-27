from opendata.sources.bikeshare import BikeshareCSVDataSource
from opendata.sources.bikeshare import create_trips_parsers


trips = BikeshareCSVDataSource(
    data_url="https://s3.amazonaws.com/baywheels-data/index.html",
    info_url="https://www.lyft.com/bikes/bay-wheels/system-data",
    license_url="https://baywheels-assets.s3.amazonaws.com/data-license-agreement.html",
    license_name="Bay Wheels License Agreement",
    trips_parsers=create_trips_parsers(
        started_at__cols=["start_time", "started_at"],
        ended_at__cols=["end_time", "ended_at"],
        start_station_id__cols=["start_station_id"],
        end_station_id__cols=["end_station_id"],
        start_station_name__cols=["start_station_name"],
        end_station_name__cols=["end_station_name"],
        rideable_type__cols=["rideable_type"],
        ride_id__cols=["ride_id"],
        start_lat__cols=["start_station_latitude", "start_lat"],
        start_lng__cols=["start_station_longitude", "start_lng"],
        end_lat__cols=["end_station_latitude", "end_lat"],
        end_lng__cols=["end_station_longitude", "end_lng"],
        gender__cols=[],
        user_type__cols=["member_casual", "user_type"],
        bike_id__cols=["bike_id"],
        birth_year__cols=[],
    ),
    ignore_cols={"rental_access_method", "duration_sec", "bike_share_for_all_trip"},
)
