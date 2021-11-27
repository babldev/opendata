import pathlib
from dataclasses import dataclass

import pandas as pd


@dataclass
class DataSource:
    data_url: str
    info_url: str
    license_url: str
    license_name: str

    def load(self) -> pd.DataFrame:
        raise NotImplementedError("Data source must implement a load function")

    def ensure_data_dir(self) -> str:
        path = pathlib.Path(self.data_dir_path)
        path.mkdir(parents=True, exist_ok=True)
        return path.as_posix()

    @property
    def data_dir_path(self) -> str:
        return f"data/{self.__class__.__name__}"
