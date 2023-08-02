import asyncio
import csv
import hashlib
import io
import logging
import os
import re
from dataclasses import dataclass
from enum import auto
from enum import Enum
from typing import Dict
from typing import IO
from typing import Iterable
from typing import List
from typing import Optional
from typing import Pattern
from typing import Set
from typing import Tuple
from zipfile import ZipFile

import aiohttp
import pandas as pd
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from opendata.data_source import DataSource


# create logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.55 Safari/537.36"
)


@dataclass
class ColumnParser:
    to_column: str
    from_columns: List[str]
    dtype: str
    remap_values: Optional[Dict[str, Set[str]]] = None


class FileType(Enum):
    UNKNOWN = auto()
    TRIPS = auto()
    STATIONS = auto()


def create_trips_parsers(
    started_at__cols: List[str],
    ended_at__cols: List[str],
    start_station_id__cols: List[str],
    end_station_id__cols: List[str],
    start_station_name__cols: List[str],
    end_station_name__cols: List[str],
    rideable_type__cols: List[str],
    ride_id__cols: List[str],
    start_lat__cols: List[str],
    start_lng__cols: List[str],
    end_lat__cols: List[str],
    end_lng__cols: List[str],
    gender__cols: List[str],
    user_type__cols: List[str],
    bike_id__cols: List[str],
    birth_year__cols: List[str],
) -> List[ColumnParser]:
    return [
        ColumnParser(
            to_column="started_at",
            from_columns=started_at__cols,
            dtype="datetime64[ns]",
        ),
        ColumnParser(
            to_column="ended_at",
            from_columns=ended_at__cols,
            dtype="datetime64[ns]",
        ),
        ColumnParser(
            to_column="start_station_id",
            from_columns=start_station_id__cols,
            dtype="string",
        ),
        ColumnParser(
            to_column="end_station_id",
            from_columns=end_station_id__cols,
            dtype="string",
        ),
        ColumnParser(
            to_column="start_station_name",
            from_columns=start_station_name__cols,
            dtype="string",
        ),
        ColumnParser(
            to_column="end_station_name",
            from_columns=end_station_name__cols,
            dtype="string",
        ),
        ColumnParser(
            to_column="rideable_type",
            from_columns=rideable_type__cols,
            dtype="string",
            remap_values={
                "classic_bike": {"docked_bike", "classic_bike"},
                "electric_bike": {"electric_bike"},
            },
        ),
        ColumnParser(
            to_column="ride_id",
            from_columns=ride_id__cols,
            dtype="string",
        ),
        ColumnParser(
            to_column="start_lat",
            from_columns=start_lat__cols,
            dtype="float64",
        ),
        ColumnParser(
            to_column="start_lng",
            from_columns=start_lng__cols,
            dtype="float64",
        ),
        ColumnParser(
            to_column="end_lat",
            from_columns=end_lat__cols,
            dtype="float64",
        ),
        ColumnParser(
            to_column="end_lng",
            from_columns=end_lng__cols,
            dtype="float64",
        ),
        ColumnParser(
            to_column="gender",
            from_columns=gender__cols,
            dtype="string",
            remap_values={
                "N/A": {"0"},  # N/A maps to null in the remapper
                "male": {"1", "male"},
                "female": {"2", "female"},
            },
        ),
        ColumnParser(
            to_column="user_type",
            from_columns=user_type__cols,
            dtype="string",
            remap_values={
                "casual": {
                    "customer",
                    "casual",
                    "0",
                },  # bixi uses 1 to indicate membership
                "member": {"subscriber", "member", "1"},
            },
        ),
        ColumnParser(
            to_column="bike_id",
            from_columns=bike_id__cols,
            dtype="string",
        ),
        ColumnParser(
            to_column="birth_year",
            from_columns=birth_year__cols,
            dtype="string",
        ),
    ]


def create_stations_parsers(
    id__cols: List[str],
    name__cols: List[str],
    lat__cols: List[str],
    lng__cols: List[str],
    created_at__cols: List[str],
    is_active__cols: List[str],
) -> List[ColumnParser]:
    return [
        ColumnParser(
            to_column="station__id",
            from_columns=id__cols,
            dtype="string",
        ),
        ColumnParser(
            to_column="station__name",
            from_columns=name__cols,
            dtype="string",
        ),
        ColumnParser(
            to_column="station__lat",
            from_columns=lat__cols,
            dtype="float64",
        ),
        ColumnParser(
            to_column="station__lng",
            from_columns=lng__cols,
            dtype="float64",
        ),
        ColumnParser(
            to_column="station__created_at",
            from_columns=created_at__cols,
            dtype="datetime64[ns]",
        ),
        ColumnParser(
            to_column="station__is_active",
            from_columns=is_active__cols,
            dtype="string",
        ),
    ]


@dataclass
class ParseConfig:
    dtypes: Dict[str, str]
    parse_dates: List[str]


@dataclass
class CachedFileInfo:
    local_path: str
    remote_path: str


def dtype_mapping(parsers: List[ColumnParser]) -> ParseConfig:
    """Return the dtypes to be used when parsing the CSV file"""
    dtypes = {}
    parse_dates = []
    for parser in parsers:
        for column in parser.from_columns:
            if parser.dtype.startswith("datetime"):
                parse_dates.append(column)
            else:
                dtypes[column] = parser.dtype
    return ParseConfig(
        dtypes=dtypes,
        parse_dates=parse_dates,
    )


def get_columns_parsed(parsers: List[ColumnParser]) -> Set[str]:
    """A list of strings of column names the CSV parser will inspect"""
    columns = set()
    for parser in parsers:
        columns.update(parser.from_columns)
    return columns


def determine_filetype(
    trip_cols: Set[str], station_cols: Set[str], csv_file: IO
) -> FileType:
    if not station_cols:
        return FileType.TRIPS

    header_df = pd.read_csv(csv_file, header=0, nrows=0)
    csv_file.seek(0)  # we need to read the file again later, so reset
    if not set(header_df.columns):
        return FileType.TRIPS

    columns_found = set(header_df.columns)
    if len(columns_found.intersection(trip_cols)) / len(trip_cols) > len(
        columns_found.intersection(station_cols)
    ) / len(station_cols):
        return FileType.TRIPS
    else:
        return FileType.STATIONS


def open_and_concat_paths(
    cached_files: List[CachedFileInfo],
    trip_sample_rate: int,
    trip_parsers: List[ColumnParser],
    ignore_cols: Set[str],
    station_parsers: Optional[List[ColumnParser]] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Open a list of locally cached files and concatenate into a single CSV"""
    trip_dfs: List[pd.DataFrame] = [pd.DataFrame()]
    station_dfs: List[pd.DataFrame] = [pd.DataFrame()]
    trip_parse_config = dtype_mapping(trip_parsers)
    station_parse_config = dtype_mapping(station_parsers) if station_parsers else None

    trip_cols = get_columns_parsed(trip_parsers)
    station_cols = get_columns_parsed(station_parsers) if station_parsers else set()

    for cached_file_info in cached_files:
        try:
            with ZipFile(cached_file_info.local_path) as zip:
                for zipinfo in zip.infolist():
                    if zipinfo.filename.startswith("__MACOSX"):
                        # __MACOSX directory includes CSV files we don't want
                        continue
                    elif zipinfo.filename.endswith("csv"):
                        with zip.open(zipinfo.filename) as f:
                            filetype = determine_filetype(
                                trip_cols=trip_cols,
                                station_cols=station_cols,
                                csv_file=f,
                            )
                            if filetype == FileType.TRIPS:
                                trip_dfs.append(
                                    pd.read_csv(
                                        f,
                                        dtype=trip_parse_config.dtypes,
                                        header=0,
                                        skiprows=lambda i: i % trip_sample_rate != 0,
                                    )
                                )
                            elif filetype == FileType.STATIONS:
                                station_dfs.append(
                                    pd.read_csv(
                                        f,
                                        dtype=station_parse_config.dtypes
                                        if station_parse_config
                                        else None,
                                        header=0,
                                    )
                                )
                    else:
                        logging.warning(f"Unexpected file: {zipinfo.filename}")
        except:
            logging.exception(
                f"Failed to open cached file {cached_file_info.local_path} from {cached_file_info.remote_path}"
            )
            continue

    trips_result = pd.concat(trip_dfs)
    log_csv_column_results(trips_result, trip_parsers, ignore_cols, log_label="Trips")

    stations_result = pd.concat(station_dfs)
    if station_parsers:
        log_csv_column_results(
            stations_result, station_parsers, ignore_cols, log_label="Stations"
        )

    return trips_result, stations_result


def log_csv_column_results(
    df: pd.DataFrame, parsers: List[ColumnParser], ignore_cols: Set[str], log_label: str
) -> None:
    columns_found = set(df.columns) - ignore_cols
    columns_expected = get_columns_parsed(parsers)

    columns_parsed = columns_found.intersection(columns_expected)
    columns_missing = columns_expected - columns_found
    columns_unexpected = columns_found - columns_expected
    if columns_parsed:
        logging.info(f"{log_label} columns parsed: {columns_parsed}")
    if columns_missing:
        logging.warning(f"{log_label} columns missing: {columns_missing}")
    if columns_unexpected:
        logging.warning(f"{log_label} columns unexpected: {columns_unexpected}")


def extract_hrefs_from_url(
    url: str, href_pattern: Pattern, timeout_sec: int
) -> Iterable[str]:
    options = webdriver.ChromeOptions()
    options.add_argument("headless")
    browser = webdriver.Chrome(chrome_options=options)
    browser.get(url)

    try:
        WebDriverWait(browser, timeout_sec).until(
            EC.presence_of_element_located((By.TAG_NAME, "a"))
        )
    except TimeoutException:
        logger.debug(f"Loading {url} timed out...")

    logger.debug(f"Page {url} loaded!")

    a_elements = browser.find_elements_by_tag_name("a")
    for a_element in a_elements:
        href = a_element.get_attribute("href")
        if href and href_pattern.match(href):
            logger.debug(f"Found download at path {href}")
            yield href


def merge_columns(parsers: List[ColumnParser], df: pd.DataFrame) -> pd.DataFrame:
    new_df = pd.DataFrame()
    for parser in parsers:
        assigned = False
        col = pd.Series()
        for from_column in parser.from_columns:
            if not assigned:
                col = df[from_column]
                assigned = True
            else:
                col = col.fillna(df[from_column])

        if parser.remap_values:
            col = remap_values(col, parser.remap_values)

        new_df[parser.to_column] = col

    return new_df


def remap_values(series: pd.Series, remap_values: Dict[str, Set[str]]) -> pd.Series:
    def remap_one(cur_val: Optional[str]) -> Optional[str]:
        if not isinstance(cur_val, str):
            return cur_val

        cur_val = cur_val.lower()
        for to_val, from_vals in remap_values.items():
            if cur_val in from_vals:  # from_vals should be lowercase already
                return to_val if to_val != "N/A" else None

        return cur_val

    return series.apply(remap_one)


def parse_datetime_columns(
    parsers: List[ColumnParser], df: pd.DataFrame
) -> pd.DataFrame:
    for parser in parsers:
        if not parser.dtype.startswith("datetime"):
            continue

        df[parser.to_column] = pd.to_datetime(df[parser.to_column])

    return df


def drop_malformed_trips(df: pd.DataFrame) -> pd.DataFrame:
    """Some markets like Niceride have null rows in the CSV"""
    null_started_at = df["started_at"].isna()
    count = null_started_at.sum()
    if count > 0:
        logger.warn(f"Dropping {count} rows missing a started_at timestamp")
        return df[null_started_at == False]
    else:
        return df


def file_path_for_url(url: str, data_dir_path: str) -> str:
    url_hash = hashlib.sha256()
    url_hash.update(url.encode("utf-8"))
    return os.path.join(data_dir_path, url_hash.hexdigest())


async def async_download_urls(
    data_dir_path: str, urls: Iterable[str]
) -> List[CachedFileInfo]:
    logger.debug(f"working dir: {data_dir_path}")
    async with aiohttp.ClientSession() as session:
        return await asyncio.gather(
            *[
                download_url(data_dir_path=data_dir_path, session=session, url=url)
                for url in urls
            ]
        )


async def download_url(
    data_dir_path: str, session: aiohttp.ClientSession, url: str
) -> CachedFileInfo:
    local_path = file_path_for_url(url=url, data_dir_path=data_dir_path)
    cached_file_info = CachedFileInfo(local_path=local_path, remote_path=url)
    if os.path.exists(local_path):
        logging.debug(f"File {url} cached at {local_path}")
        return cached_file_info

    tmp_local_path = local_path + ".tmp"
    # Metro Bike Share 403's without a user agent...
    async with session.get(url, headers={"User-Agent": USER_AGENT}) as resp:
        if not (resp.status >= 200 and resp.status < 300):
            logger.warn(
                f"Unexpected download status {resp.status} for {url}: {await resp.text()}"
            )
            return cached_file_info  # TODO: This should be an exception

        with open(tmp_local_path, "wb") as fd:
            logger.info(f"Started downloading: {url} to {local_path}")
            while True:
                chunk = await resp.content.read(2**16)
                if not chunk:
                    break
                fd.write(chunk)

    os.replace(tmp_local_path, local_path)
    logger.info(f"Finished downloading: {url}")
    return cached_file_info


def merge_stations_table(
    trips_df: pd.DataFrame, stations_df: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    # A bit of a hack to dedup multiple station entries
    stations_df = stations_df.groupby("station__id").tail(1).set_index("station__id")
    stations_df.index = stations_df.index.astype(
        "string"
    )  # pandas switches to integer types sometimes :(
    STATION_COLS = [
        "station__name",
        "station__lat",
        "station__lng",
        "station__created_at",
        "station__is_active",
    ]

    trips_df = trips_df.merge(
        stations_df, how="left", left_on="start_station_id", right_index=True
    )
    # We use .astype() because merge() sometimes changes dtype
    trips_df["start_station_name"].fillna(
        trips_df["station__name"].astype("string"), inplace=True
    )
    trips_df["start_lat"].fillna(
        trips_df["station__lat"].astype("float64"), inplace=True
    )
    trips_df["start_lng"].fillna(
        trips_df["station__lng"].astype("float64"), inplace=True
    )
    trips_df.drop(columns=STATION_COLS, inplace=True)

    trips_df = trips_df.merge(
        stations_df, how="left", left_on="end_station_id", right_index=True
    )
    trips_df["end_station_name"].fillna(
        trips_df["station__name"].astype("string"), inplace=True
    )
    trips_df["end_lat"].fillna(trips_df["station__lat"].astype("float64"), inplace=True)
    trips_df["end_lng"].fillna(trips_df["station__lng"].astype("float64"), inplace=True)
    trips_df.drop(columns=STATION_COLS, inplace=True)
    return trips_df, stations_df


@dataclass
class BikeshareCSVDataSource(DataSource):
    trips_parsers: List[ColumnParser]
    ignore_cols: Set[str]
    stations_parsers: Optional[List[ColumnParser]] = None

    async def async_load(
        self, trip_sample_rate: int = 1, raw_data: bool = False
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        zip_file_urls = extract_hrefs_from_url(
            url=self.data_url,
            href_pattern=re.compile(r".*\.zip$"),
            timeout_sec=10,
        )
        working_dir = self.ensure_data_dir()
        cached_file_info = await async_download_urls(working_dir, zip_file_urls)
        trips_df, stations_df = open_and_concat_paths(
            cached_files=cached_file_info,
            trip_sample_rate=trip_sample_rate,
            trip_parsers=self.trips_parsers,
            station_parsers=self.stations_parsers,
            ignore_cols=self.ignore_cols,
        )

        if raw_data:
            return trips_df, stations_df

        trips_df = merge_columns(self.trips_parsers, trips_df)
        if self.stations_parsers:
            stations_df = merge_columns(self.stations_parsers, stations_df)

        trips_df = parse_datetime_columns(self.trips_parsers, trips_df)
        if self.stations_parsers:
            stations_df = parse_datetime_columns(self.stations_parsers, stations_df)

        if self.stations_parsers:
            trips_df, stations_df = merge_stations_table(
                trips_df=trips_df, stations_df=stations_df
            )

        trips_df = drop_malformed_trips(trips_df)
        return trips_df, stations_df
