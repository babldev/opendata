from opendata.sources.bikeshare import BikeshareCSVDataSource
from opendata.sources.bikeshare import create_trips_parsers


trips = BikeshareCSVDataSource(
    data_url="https://s3.amazonaws.com/cogo-sys-data/index.html",
    info_url="https://www.cogobikeshare.com/system-data",
    license_url="https://www.cogobikeshare.com/data-license-agreement",
    license_name="CoGo Data License Agreement",
    trips_parsers=create_trips_parsers(
        started_at__cols=["Start Time and Date", "started_at", "start_time"],
        ended_at__cols=["Stop Time and Date", "ended_at", "end_time"],
        start_station_id__cols=[
            "Start Station ID",
            "from_station_id",
            "start_station_id",
        ],
        end_station_id__cols=["Stop Station ID", "to_station_id", "end_station_id"],
        start_station_name__cols=[
            "Start Station Name",
            "from_station_name",
            "start_station_name",
        ],
        end_station_name__cols=[
            "Stop Station Name",
            "to_station_name",
            "end_station_name",
        ],
        rideable_type__cols=["rideable_type"],
        ride_id__cols=["ride_id", "trip_id"],
        start_lat__cols=["start_lat", "Start Station Lat"],  # from_station_location ?
        start_lng__cols=["start_lng", "Start Station Long"],  # from_station_location ?
        end_lat__cols=["end_lat", "Stop Station Lat"],  # to_station_location ?
        end_lng__cols=["end_lng", "Stop Station Long"],  # to_station_location ?
        gender__cols=["Gender", "gender"],
        user_type__cols=["member_casual", "usertype", "User Type"],
        bike_id__cols=["bikeid", "Bike ID"],
        birth_year__cols=[
            "Year of Birth",
            "birthyear",
        ],
    ),
    ignore_cols={
        "to_station_location",
        "tripduration",
        "from_station_location",
        "is_equity",
    },
)
