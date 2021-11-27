from opendata.sources.bikeshare import BikeshareCSVDataSource
from opendata.sources.bikeshare import create_trips_parsers


trips = BikeshareCSVDataSource(
    data_url="https://divvy-tripdata.s3.amazonaws.com/index.html",
    info_url="https://www.divvybikes.com/system-data",
    license_url="https://www.divvybikes.com/data-license-agreement",
    license_name="Divvy Data License Agreement",
    trips_parsers=create_trips_parsers(
        started_at__cols=[
            "started_at",
            "start_time",
            "starttime",
            "01 - Rental Details Local Start Time",
        ],
        ended_at__cols=[
            "ended_at",
            "end_time",
            "stoptime",
            "01 - Rental Details Local End Time",
        ],
        start_station_id__cols=[
            "start_station_id",
            "from_station_id",
            "03 - Rental Start Station ID",
        ],
        end_station_id__cols=[
            "end_station_id",
            "to_station_id",
            "02 - Rental End Station ID",
        ],
        start_station_name__cols=[
            "start_station_name",
            "from_station_name",
            "03 - Rental Start Station Name",
        ],
        end_station_name__cols=[
            "end_station_name",
            "to_station_name",
            "02 - Rental End Station Name",
        ],
        rideable_type__cols=["rideable_type"],
        ride_id__cols=["ride_id", "trip_id", "01 - Rental Details Rental ID"],
        start_lat__cols=["start_lat", "latitude"],
        start_lng__cols=["start_lng", "longitude"],
        end_lat__cols=["end_lat"],
        end_lng__cols=["end_lng"],
        gender__cols=["Member Gender", "gender"],
        user_type__cols=[
            "usertype",
            "User Type",
            "member_casual",
        ],
        bike_id__cols=["bikeid", "01 - Rental Details Bike ID"],
        birth_year__cols=[
            "birthyear",
            "birthday",
            "05 - Member Details Member Birthday Year",
        ],
    ),
    ignore_cols={
        "city",
        "landmark",
        "id",
        "dateCreated",
        "tripduration",
        "dpcapacity",
        "online_date",
        "01 - Rental Details Duration In Seconds Uncapped",
        "online date",
        "Unnamed: 7",
        "name",
    },
)
