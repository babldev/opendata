from opendata.sources.bikeshare import BikeshareCSVDataSource
from opendata.sources.bikeshare import ColumnParser
from opendata.sources.bikeshare import create_trips_parsers


trips = BikeshareCSVDataSource(
    data_url="https://s3.amazonaws.com/capitalbikeshare-data/index.html",
    info_url="https://www.capitalbikeshare.com/system-data",
    license_url="https://www.capitalbikeshare.com/data-license-agreement",
    license_name="Capital Bikeshare Data License Agreement",
    trips_parsers=create_trips_parsers(
        started_at__cols=["started_at", "Start date"],
        ended_at__cols=["ended_at", "End date"],
        start_station_id__cols=["Start station number", "start_station_id"],
        end_station_id__cols=["End station number", "end_station_id"],
        start_station_name__cols=["Start station", "start_station_name"],
        end_station_name__cols=["End station", "end_station_name"],
        rideable_type__cols=["rideable_type"],
        ride_id__cols=["ride_id"],
        start_lat__cols=["start_lat"],
        start_lng__cols=["start_lng"],
        end_lat__cols=["end_lat"],
        end_lng__cols=["end_lng"],
        gender__cols=[],
        user_type__cols=["member_casual", "Member type"],
        bike_id__cols=["Bike number"],
        birth_year__cols=[],
    ),
    ignore_cols={"Duration"},
)
