import argparse

import pandas as pd

from opendata.sources.bikeshare import SUPPORTED_MARKETS


def export_total_for_year(year: int) -> None:
    totals = []

    for market in SUPPORTED_MARKETS:
        df = pd.read_csv(f"{market}.csv")
        totals.append([market, len(df[df["started_at"].str.contains(str(year))])])

    totals_df = pd.DataFrame(totals, columns=["system", "count"])
    totals_df.to_csv("totals.csv", index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Calculate total rides for a given year"
    )
    parser.add_argument("year", type=int, help="year to calculate totals for")
    args = parser.parse_args()
    export_total_for_year(args.year)
