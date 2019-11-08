# Orangebox

A Cleanflight/Betaflight blackbox log parser written in Python 3. 

Orangebox has no dependencies other than the Python standard library, although it might be worthy to investigate 
how using `numpy` could possibly bring some performance gain in the future. If so, it shouldn't be a usability barrier 
since any user wanting to make something out of the decoded data will more than likely have `numpy` installed 
transitively as well as it's also a dependency of libraries like `matplotlib` and `pandas` (among others).

This parser was roughly modeled after the one in [Blackbox Log Viewer](https://github.com/betaflight/blackbox-log-viewer) hence produces the same output.

## Usage (so-called User Guide)

```python3
from orangebox import Parser

parser = Parser.load("LOG00042.BFL")

# Print headers
print(parser.headers)

# Print the names of fields
print(parser.field_names)

# Print field values frame by frame
for frame in parser.frames():
    print(frame.data)

# Complete list of events only available once all frames have been parsed
print(parser.events)
```

## Development tools

You can use `parser_test.py` to test against a CSV file generated by [blackbox-tools](https://github.com/cleanflight/blackbox-tools)
or [Blackbox Log Viewer](https://github.com/betaflight/blackbox-log-viewer). It compares the decoded values to the ones in the CSV file and shows the differences with some additional info. 
Please note that `blackbox_decode` and exporting from Blackbox Log Viewer can produce different results.

```
usage: parser_test.py [-h] [-a] [-v] path csv_path

positional arguments:
  path                  Path to a .BFL file
  csv_path              Path to a .CSV file for verification

optional arguments:
  -h, --help            show this help message and exit
  -a, --show-all-fields
                        Show all fields of differing frames (default: False)
  -v                    Control verbosity (can be used multiple times)
                        (default: 0)
``` 

To profile or measure the parser's execution you can use `parser_benchmark.py` and `parser_profile.py` with a single argument that's a path to a BFL file:

```bash
python3 parser_benchmark.py ~/logs/LOG00042.BFL
```

## Contributing

* Contributions are very welcome!
* Please follow the [PEP8](https://www.python.org/dev/peps/pep-0008/) Style Guido.

## Changelog

### 0.1.0-beta

* First release (with a lot of missing parts)

## Known issues

* Does not support multiple logs in a single file (flashchip logs)
* No explicit validation of raw data against corruption, missing headers or whatsoever, but it's highly likely that a Python exception will be raised in these cases anyway
* Tested only on logs generated by Betaflight
* No parsing of GPS frames (I don't use it, you're welcome to contribute)
* Not all event frames are parsed (see [TODO](orangebox/events.py) comments)
* Some decoders are missing, possibly related to GPS data (see [TODO](orangebox/decoders.py) comments)

## Acknowledgement

Original blackbox data encoder and decoder was written by [Nicholas Sherlock](https://github.com/thenickdude).

## License

This project is licensed under GPLv3.