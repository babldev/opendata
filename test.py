import asyncio
import logging

from opendata.sources.bikeshare.capital_bikeshare import trips

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)

# Sample 1 out of 1000 for better memory performance
trips_df, _ = asyncio.run(trips.async_load(trip_sample_rate=1000))
trips_df.to_csv("cabi_trips.csv")
print("Sampled trip file written to cabi.csv")
