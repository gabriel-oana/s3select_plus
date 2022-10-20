<h2><b>[v1.0.3]</b></h2>

<h3><u>Bug fixes</u></h3>
* Fixed issue where the "extra_func_args" are not set. Now an extra function can be declared without requiring additional arguments.

<h3><u>Improvements</u></h3>
* Results parser added for JSON files. Now one can call "results.payload_dict" to have the JSON results nicely parsed into lists / dicts.
By default, "results.payload" is a string representation of the JSON data extracted.
* Results parser added for CSV files. Now one can call "results.payload_csv" to have CSV results parsed as a list of lists (each list is a row).