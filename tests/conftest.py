from unittest.mock import Mock

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("UnitTesting").getOrCreate()


@pytest.fixture
def spark_session_mock():
    return Mock(spec=SparkSession)
