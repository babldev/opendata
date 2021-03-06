from opendata.sources.bikeshare import BikeshareCSVDataSource
from opendata.sources.bikeshare import create_trips_parsers

trips = BikeshareCSVDataSource(
    data_url="https://s3.amazonaws.com/niceride-data/index.html",
    info_url="https://www.niceridemn.com/system-data",
    license_url="https://assets.niceridemn.com/data-license-agreement.html",
    license_name="Nice Ride Minnesota Data License Agreement",
    trips_parsers=create_trips_parsers(
        started_at__cols=["started_at", "Start date", "start_time"],
        ended_at__cols=["ended_at", "End date", "end_time"],
        start_station_id__cols=[
            "Start station number",
            "start_station_id",
            "start station id",
        ],
        end_station_id__cols=["End station number", "end_station_id", "end station id"],
        start_station_name__cols=[
            "Start station",
            "start_station_name",
            "start station name",
        ],
        end_station_name__cols=["End station", "end_station_name", "end station name"],
        rideable_type__cols=["rideable_type"],
        ride_id__cols=["ride_id"],
        start_lat__cols=["start_lat", "start station latitude", "Lat", "Latitude"],
        start_lng__cols=["start_lng", "start station longitude", "Long", "Longitude"],
        end_lat__cols=["end_lat", "end station latitude"],
        end_lng__cols=["end_lng", "end station longitude"],
        gender__cols=["gender"],
        user_type__cols=["usertype", "Account type", "member_casual"],
        bike_id__cols=["bikeid"],
        birth_year__cols=["birth year"],
    ),
    ignore_cols={
        "Total duration (Seconds)",
        "Number",
        "Start terminal",
        "Total duration (seconds)",
        "Total duration (ms)",
        "NbDocks",
        "Terminal",
        "Station",
        "Nb Docks",
        "bike type",
        "Notes",
        "Nb docks",
        "Total docks",
        "Install date",
        "Name",
        "End terminal",
        "Unnamed: 5",
        "tripduration",
    },
)
