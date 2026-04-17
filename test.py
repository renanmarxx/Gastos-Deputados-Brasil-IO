import abc

import pandas as pd


class BaseTest(abc.ABC):
    @abc.abstractmethod
    def test(self):
        pass

    def test_not_null(self, df: pd.DataFrame, column: str):
        assert df[column].notnull().all(), f"Column {column} contains null values."
