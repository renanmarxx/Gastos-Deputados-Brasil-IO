def test_clean_text_str(parser):
    input_str = '{\n\t"key": "value"}\n{\t"key2": "value2"}'
    expected_output = '{"key": "value"}|||{"key2": "value2"}'
    assert parser.clean_text_str(input_str) == expected_output

def test_parse_str_to_dict(parser):
    input_str = '{"key": "value"}|||{"key2": "value2"}'
    expected_output = [{"key": "value"}, {"key2": "value2"}]
    assert parser.parse_str_to_dict(input_str) == expected_output

def test_get(parser):
    input_str = '{\n\t"key": "value"}\n{\t"key2": "value2"}'
    expected_output = [{"key": "value"}, {"key2": "value2"}]
    assert parser.get(input_str) == expected_output