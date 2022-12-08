import datetime
import unittest
from unittest.mock import MagicMock

import pandas as pd

from src.ETLInterface import ETLInterface
from src.main import Task1, Task2


class TestMain(unittest.TestCase):
    def test_interface_implementation(self):
        """
        Check if classes implement all abstract methods from ETLInterface
        """
        spark = MagicMock()
        try:
            Task1_instance = Task1(spark)
            Task2_instance = Task2(spark)
            assert isinstance(Task1_instance, ETLInterface) == True
            assert isinstance(Task2_instance, ETLInterface) == True
        except TypeError:
            assert False

    def test_lambda_PT_time_udf(self):
        """
        Check if lambda function converts ISO 8601 duration correctly
        """
        spark = MagicMock()
        test_instance = Task1(spark)
        series = pd.Series(["PT5M", "PT50M", "PT2H5M", None])
        pd.testing.assert_series_equal(
            series.apply(test_instance.lambda_PT_time_udf()),
            pd.Series([5, 50, 125, None]),
        )

    def test_lambda_str_date_udf(self):
        """
        Check if lambda function converts str to date correctly
        """
        spark = MagicMock()
        test_instance = Task1(spark)
        series = pd.Series(["2022-01-01", "2023-01-01", "2024-01-01"])
        pd.testing.assert_series_equal(
            series.apply(test_instance.lambda_str_date_udf()),
            pd.Series(
                [
                    datetime.datetime(2022, 1, 1),
                    datetime.datetime(2023, 1, 1),
                    datetime.datetime(2024, 1, 1),
                ]
            ),
        )


if __name__ == "__main__":
    unittest.main()
