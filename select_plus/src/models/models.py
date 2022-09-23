from typing import Optional, Union
from dataclasses import dataclass


@dataclass
class EngineResultsStats:
    cost: float
    files_processed: int
    bytes_scanned: int
    bytes_returned: int
    bytes_processed: int


@dataclass
class EngineResults:
    payload: list
    stats: EngineResultsStats


@dataclass
class CompressionTypes:
    none: str = 'NONE'
    gzip: str = 'GZIP'
    bzip2: str = 'BZIP2'


@dataclass
class CSVInputSerialization:
    file_header_info: Optional[str] = None
    comments: Optional[str] = None
    quote_escape_character: Optional[str] = None
    record_delimiter: Optional[str] = None
    field_delimiter: Optional[str] = None
    quote_character: Optional[str] = None
    allow_quoted_record_delimiter: Optional[bool] = False

    def __post_init__(self):
        # Validate the file_header_info
        if self.file_header_info:
            if self.file_header_info not in ['USE', 'IGNORE', 'NONE']:
                raise RuntimeError('CSV Input Serialization field file_header_info must be USE | IGNORE | NONE')

    def as_dict(self) -> dict:
        params = {}
        if self.file_header_info:
            params['FileHeaderInfo'] = self.file_header_info
        if self.comments:
            params['Comments'] = self.comments
        if self.quote_escape_character:
            params['QuoteEscapeCharacter'] = self.quote_escape_character
        if self.record_delimiter:
            params['RecordDelimiter'] = self.record_delimiter
        if self.field_delimiter:
            params['FieldDelimiter'] = self.field_delimiter
        if self.quote_character:
            params['QuoteCharacter'] = self.quote_character
        if self.allow_quoted_record_delimiter:
            params['AllowQuotedRecordDelimiter'] = self.allow_quoted_record_delimiter

        return params


@dataclass
class JSONInputSerialization:
    Type: Optional[str] = None

    def __post_init__(self):
        # Validate the file_header_info
        if self.Type:
            if self.Type not in ['LINES', 'DOCUMENT']:
                raise RuntimeError('JSON Input Serialization field Type must be ALWAYS | DOCUMENT')

    def as_dict(self) -> dict:
        params = {}
        if self.Type:
            params['Type'] = self.Type
        return params


@dataclass
class InputSerialization:
    compression_type: Union[Optional[CompressionTypes], str] = CompressionTypes.none
    csv: Union[Optional[CSVInputSerialization], dict] = None
    json: Union[Optional[JSONInputSerialization], dict] = None
    parquet: dict = None

    def __post_init__(self):
        # At least one type must be set between CSV, JSON or PARQUET
        param_list = [self.csv, self.json, self.parquet]
        items_in_param_list = sum(x is not None for x in param_list)

        if items_in_param_list != 1:
            raise RuntimeError('InputSerializationError: Only one of the inputs must be selected: csv, json or parquet')

    def as_dict(self) -> dict:
        params = {}
        if self.compression_type:
            params['CompressionType'] = self.compression_type
        if self.csv:
            params['CSV'] = self.csv.as_dict() if isinstance(self.csv, CSVInputSerialization) else self.csv
        if self.json:
            params['JSON'] = self.json.as_dict() if isinstance(self.json, JSONInputSerialization) else self.json
        if self.parquet is not None:
            params['Parquet'] = {}
        return params


@dataclass
class CSVOutputSerialization:
    quote_fields: Optional[str] = 'ASNEEDED'
    quote_escape_character: Optional[str] = None
    record_delimiter: Optional[str] = None
    field_delimiter: Optional[str] = None
    quote_character: Optional[str] = None

    def __post_init__(self):
        # Validate the quote_fields
        if self.quote_fields:
            if self.quote_fields not in ['ALWAYS', 'ASNEEDED']:
                raise RuntimeError('CSV Output Serialization field quote_fields must be ALWAYS | ASNEEDED')

    def as_dict(self):
        params = {}
        if self.quote_fields:
            params['QuoteFields'] = self.quote_fields
        if self.quote_character:
            params['QuoteEscapeCharacter'] = self.quote_escape_character
        if self.record_delimiter:
            params['RecordDelimiter'] = self.record_delimiter
        if self.field_delimiter:
            params['FieldDelimiter'] = self.field_delimiter
        if self.quote_character:
            params['QuoteCharacter'] = self.quote_character

        return params


@dataclass
class JSONOutputSerialization:
    record_delimiter: Optional[str] = None

    def as_dict(self):
        params = {}
        if self.record_delimiter:
            params['RecordDelimiter'] = self.record_delimiter
        return params


@dataclass()
class OutputSerialization:
    csv: Union[Optional[CSVOutputSerialization], dict] = None
    json: Union[Optional[JSONOutputSerialization], dict] = None

    def __post_init__(self):
        # At least one type must be set between CSV, JSON
        param_list = [self.csv, self.json]
        items_in_param_list = sum(x is not None for x in param_list)

        if items_in_param_list != 1:
            raise RuntimeError('OutputSerializationError: Only one of the inputs must be selected: csv or json')

    def as_dict(self):
        params = {}
        if self.csv:
            params['CSV'] = self.csv.as_dict() if isinstance(self.csv, CSVOutputSerialization) else self.csv
        if self.json:
            params['JSON'] = self.json.as_dict() if isinstance(self.json, JSONOutputSerialization) else self.json
        return params
