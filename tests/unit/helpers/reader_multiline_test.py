from unittest.mock import Mock

import pytest


@pytest.fixture
def json_string_parser_mock():
    return Mock(spec=JsonStringParser)


@pytest.fixture
def struct_flattener_mock():
    return Mock(spec=StructFlattener)


def test_load_whitout_flatten(spark_session_mock, json_string_parser_mock, struct_flattener_mock):
    reader = MultilineJsonReader(spark_session_mock, json_string_parser_mock, struct_flattener_mock)
    spark_session_mock.read.text.return_value.collect.return_value = [{"value": "mocked file content"}]
    json_string_parser_mock.get.return_value = ["mocked", "json", "strings"]
    spark_session_mock.read.json.return_value = "mocked dataframe"

    result = reader.load("mock/path", flatten=False)

    spark_session_mock.read.text.assert_called_once_with("mock/path", wholetext=True)
    json_string_parser_mock.get.assert_called_once_with(text_str="mocked file content")
    assert result == "mocked dataframe", "Expected mocked dataframe"
