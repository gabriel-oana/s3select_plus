from select_plus.src.models.models import CompressionTypes, CSVOutputSerialization, CSVInputSerialization, \
    JSONOutputSerialization, JSONInputSerialization, InputSerialization, OutputSerialization
from tests.util.test_wrapper import TestWrapper


class TestModels(TestWrapper):

    def test_compression_types(self):
        c = CompressionTypes()
        self.assertEqual(c.none, 'NONE')
        self.assertEqual(c.bzip2, 'BZIP2')
        self.assertEqual(c.gzip, 'GZIP')

    def test_csv_input_serialization_valid(self):
        c = CSVInputSerialization(
            file_header_info='USE',
            comments='test1',
            quote_escape_character='test2',
            record_delimiter='test3',
            field_delimiter='test4',
            quote_character='test5',
            allow_quoted_record_delimiter=True
        )

        dict_repr = c.as_dict()
        expected = {'FileHeaderInfo': 'USE', 'Comments': 'test1', 'QuoteEscapeCharacter': 'test2',
                    'RecordDelimiter': 'test3', 'FieldDelimiter': 'test4', 'QuoteCharacter': 'test5',
                    'AllowQuotedRecordDelimiter': True}

        self.assertDictEqual(dict_repr, expected)

    def test_csv_input_serialization_valid_with_missing_params(self):
        c = CSVInputSerialization(
            file_header_info='USE',
            comments='test1',
            quote_escape_character='test2',
            record_delimiter='test3',
        )

        dict_repr = c.as_dict()
        expected = {'FileHeaderInfo': 'USE', 'Comments': 'test1', 'QuoteEscapeCharacter': 'test2',
                    'RecordDelimiter': 'test3'}

        self.assertDictEqual(dict_repr, expected)

    def test_csv_input_serialization_invalid(self):
        self.assertRaises(RuntimeError, CSVInputSerialization, file_header_info='wrong')

    def test_json_input_serialization_valid(self):
        c = JSONInputSerialization(
            Type='DOCUMENT'
        )

        dict_repr = c.as_dict()
        expected = {"Type": "DOCUMENT"}

        self.assertDictEqual(dict_repr, expected)

    def test_json_input_serialization_invalid(self):
        self.assertRaises(RuntimeError, JSONInputSerialization, Type='wrong')

    def test_input_serialization_with_compression_types(self):
        c = CompressionTypes()
        i = InputSerialization(
            compression_type=c.gzip,
            parquet={}
        )

        dict_repr = i.as_dict()
        expected = {'CompressionType': 'GZIP', 'Parquet': {}}
        self.assertDictEqual(dict_repr, expected)

    def test_input_serialization_with_compression_str(self):
        i = InputSerialization(
            compression_type='GZIP',
            parquet={}
        )

        dict_repr = i.as_dict()
        expected = {'CompressionType': 'GZIP', 'Parquet': {}}
        self.assertDictEqual(dict_repr, expected)

    def test_input_serialization_with_json_types(self):
        j = JSONInputSerialization(
            Type='DOCUMENT'
        )
        i = InputSerialization(
            json=j
        )

        dict_repr = i.as_dict()
        expected = {'CompressionType': 'NONE', 'JSON': {'Type': 'DOCUMENT'}}
        self.assertDictEqual(dict_repr, expected)

    def test_input_serialization_with_json_str(self):
        i = InputSerialization(
            json={'Type': 'DOCUMENT'}
        )

        dict_repr = i.as_dict()
        expected = {'CompressionType': 'NONE', 'JSON': {'Type': 'DOCUMENT'}}
        self.assertDictEqual(dict_repr, expected)

    def test_input_serialization_with_csv_types(self):
        c = CSVInputSerialization(
            file_header_info='USE'
        )
        i = InputSerialization(
            csv=c
        )

        dict_repr = i.as_dict()
        expected = {'CompressionType': 'NONE', 'CSV': {'FileHeaderInfo': 'USE'}}
        self.assertDictEqual(dict_repr, expected)

    def test_input_serialization_with_csv_str(self):
        i = InputSerialization(
            csv={'FileHeaderInfo': 'USE'}
        )

        dict_repr = i.as_dict()
        expected = {'CompressionType': 'NONE', 'CSV': {'FileHeaderInfo': 'USE'}}
        self.assertDictEqual(dict_repr, expected)

    def test_input_serialization_with_parquet_str(self):
        i = InputSerialization(
            parquet={}
        )

        dict_repr = i.as_dict()
        expected = {'CompressionType': 'NONE', 'Parquet': {}}
        self.assertDictEqual(dict_repr, expected)

    def test_input_serialization_fails_with_more_than_one_serializer(self):
        self.assertRaises(RuntimeError, InputSerialization, parquet={}, json={})

    def test_csv_output_serialization(self):
        c = CSVOutputSerialization(
            quote_fields='ALWAYS',
            quote_character='test1',
            quote_escape_character='test2',
            record_delimiter='test3',
            field_delimiter='test4'
        )

        dict_repr = c.as_dict()
        expected = {'QuoteFields': 'ALWAYS', 'QuoteEscapeCharacter': 'test2', 'RecordDelimiter': 'test3',
                    'FieldDelimiter': 'test4', 'QuoteCharacter': 'test1'}
        self.assertDictEqual(dict_repr, expected)

    def test_csv_output_serialization_raises(self):
        self.assertRaises(RuntimeError, CSVOutputSerialization, quote_fields='wrong')

    def test_json_output_serialization(self):
        j = JSONOutputSerialization(
            record_delimiter='test1',
        )

        dict_repr = j.as_dict()
        expected = {'RecordDelimiter': 'test1'}
        self.assertDictEqual(dict_repr, expected)

    def test_output_serialization_with_csv_types(self):
        c = CSVOutputSerialization()
        o = OutputSerialization(
            csv=c
        )

        dict_repr = o.as_dict()
        expected = {'CSV': {'QuoteFields': 'ASNEEDED'}}
        self.assertDictEqual(dict_repr, expected)

    def test_output_serialization_with_csv_str(self):
        o = OutputSerialization(
            csv={'QuoteFields': 'ASNEEDED'}
        )

        dict_repr = o.as_dict()
        expected = {'CSV': {'QuoteFields': 'ASNEEDED'}}
        self.assertDictEqual(dict_repr, expected)

    def test_output_serialization_with_json_types(self):
        j = JSONOutputSerialization()
        o = OutputSerialization(
            json=j
        )

        dict_repr = o.as_dict()
        expected = {'JSON': {}}
        self.assertDictEqual(dict_repr, expected)

    def test_output_serialization_with_json_str(self):
        o = OutputSerialization(
            json={'QuoteFields': 'ASNEEDED'}
        )

        dict_repr = o.as_dict()
        expected = {'JSON': {'QuoteFields': 'ASNEEDED'}}
        self.assertDictEqual(dict_repr, expected)

    def test_output_serialization_fails_with_more_than_one_serializer(self):
        self.assertRaises(RuntimeError, OutputSerialization, json={}, csv={})

