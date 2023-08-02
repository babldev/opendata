import argparse
import asyncio
import logging
import importlib

SUPPORTED_MARKETS = {
    'bay_wheels',
    'bixi',
    'bluebikes',
    'capital_bikeshare',
    'citi_bike',
    'cogo',
    'divvy',
    'indego',
    'metro_bike_share',
    'niceride',
}


def market_to_csv(market: str, sample_rate: int) -> None:
    trips = importlib.import_module(f'opendata.sources.bikeshare.{market}').trips

    logging.basicConfig(
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
    )

    # Sample 1 out of 1000 for better memory performance
    trips_df, _ = asyncio.run(trips.async_load(trip_sample_rate=sample_rate))
    trips_df.to_csv(f"{market}.csv")
    print(f"Sampled trip file written to {market}.csv")


if __name__ == '__main__':
    # Parse arguments for market string and sample rate
    parser = argparse.ArgumentParser(description='Download market data to CSV.')
    parser.add_argument('market', type=str,
                        # valdate market string
                        choices=SUPPORTED_MARKETS,
                        help='Market name string')
    parser.add_argument('--sample_rate', type=int, default=1000, help='Sample rate for trips, default 1000')
    args = parser.parse_args()
    market_to_csv(args.market, args.sample_rate)