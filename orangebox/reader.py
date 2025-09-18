# Orangebox - Cleanflight/Betaflight blackbox data parser.
# Copyright (C) 2019  Károly Kiripolszky
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import logging
from typing import BinaryIO, Dict, Iterator, List, Optional

from .decoders import decoder_map
from .predictors import predictor_map
from .tools import _trycast
from .types import FieldDef, FrameType, Headers

MAX_FRAME_SIZE = 256
# Increased buffer size for better I/O performance
DEFAULT_BUFFER_SIZE = 2 * 1024 * 1024  # 2MB buffer

_log = logging.getLogger(__name__)


class Reader:
    """Optimized file-like object for reading a flight log and store the raw data in a structured way.
    Includes performance optimizations for faster parsing.
    """
    __slots__ = [
        '_headers', '_field_defs', '_log_index', '_header_size', '_path',
        '_frame_data_ptr', '_log_pointers', '_frame_data', '_frame_data_len',
        '_top_comments', '_frame_data_view', '_buffer_size'
    ]

    def __init__(self, path: str, log_index: Optional[int] = None, buffer_size: int = DEFAULT_BUFFER_SIZE):
        """
        :param path: Path to a log file
        :param log_index: Session index within log file. If set to `None` (the default) there will be no session selected and headers and frame data won't be read until the first call to `.set_log_index()`.
        :param buffer_size: Buffer size for file I/O operations
        """
        self._headers = {}  # type: Headers
        self._field_defs = {}  # type: Dict[FrameType, List[FieldDef]]
        self._log_index = 0
        self._header_size = 0
        self._path = path
        self._buffer_size = buffer_size
        _log.info("Processing: " + path)
        self._frame_data_ptr = 0
        self._log_pointers = []  # type: List[int]
        self._frame_data = b''
        self._frame_data_len = 0
        self._frame_data_view = None  # type: Optional[memoryview]
        self._top_comments = []

        with open(path, "rb", buffering=self._buffer_size) as f:
            if not f.seekable():
                msg = "Input file must be seekable"
                _log.critical(msg)
                raise IOError(msg)
            self._parse_comments(f)
            self._find_pointers(f)
        if log_index is not None:
            self.set_log_index(log_index)

    def _parse_comments(self, f):
        """
        parse first lines of the file that begin with '#' into self._top_comments
        after execution file pointer stays at the first char of line coming after
        the last comment line or at the very first char of the file if file doesn't have comments.
        Comments can not be separated with non-comment data.
        """
        # Remember the starting position
        start_pos = f.tell()

        # Clear any existing comments
        self._top_comments = []

        while True:
            # Remember current position in case we need to backtrack
            current_pos = f.tell()

            # Read one line
            line = f.readline()

            # If we reached end of file, go back to start of this iteration
            if not line:
                f.seek(current_pos)
                break

            # Decode the line if it's bytes (for binary mode)
            if isinstance(line, bytes):
                try:
                    line_str = line.decode('ascii').rstrip('\r\n')
                except UnicodeDecodeError:
                    # If we can't decode, this might not be a text comment
                    f.seek(current_pos)
                    break
            else:
                line_str = line.rstrip('\r\n')

            # Check if line starts with '#'
            if line_str.startswith('#'):
                # Add comment to list (without the '#' prefix)
                self._top_comments.append(line_str[1:].lstrip())
            else:
                # This line doesn't start with '#', so we're done with comments
                # Go back to the beginning of this line
                f.seek(current_pos)
                break

        # If no comments were found, make sure we're back at the start
        if not self._top_comments:
            f.seek(start_pos)

    def set_log_index(self, index: int):
        """Set the current log index and read its corresponding frame data as raw bytes, plus parse the raw headers of
        the selected log. Optimized for better memory usage and I/O performance.

        :param index: The selected log index
        :raise RuntimeError: If ``index`` is smaller than 1 or greater than `.log_count`
        """
        if index < 1 or self.log_count < index:
            raise RuntimeError("Invalid log_index: {:d} (1 <= x < {:d})".format(index, self.log_count))

        start = self._log_pointers[index - 1]

        with open(self._path, "rb", buffering=self._buffer_size) as f:
            f.seek(start)
            self._update_headers(f)
            f.seek(start + self._header_size)

            # Calculate data size
            size = self._log_pointers[index] - start - self._header_size if index < self.log_count else None

            if size is not None:
                # Read all data at once for better performance
                self._frame_data = f.read(size)
            else:
                # Read remaining file in optimized chunks
                chunks = []
                while True:
                    chunk = f.read(self._buffer_size)
                    if not chunk:
                        break
                    chunks.append(chunk)
                self._frame_data = b''.join(chunks)

        self._log_index = index
        self._frame_data_ptr = 0
        self._frame_data_len = len(self._frame_data)
        # Create memoryview for faster byte access
        self._frame_data_view = memoryview(self._frame_data)
        self._build_field_defs()
        _log.info("Log #{:d} out of {:d} (start: 0x{:X}, size: {:d})"
                  .format(self._log_index, self.log_count, start, self._frame_data_len))

    def _update_headers(self, f: BinaryIO):
        start = f.tell()
        while True:
            line = f.readline()
            if not line:
                # nothing left to read
                break
            has_next = self._parse_header_line(line)
            if not has_next:
                f.seek(-len(line), 1)
                _log.debug(
                    "End of headers at {0:d} (0x{0:X}) (headers: {1:d})".format(f.tell(), len(self._headers.keys())))
                break
        self._header_size = f.tell() - start

    def _parse_header_line(self, data: bytes) -> bool:
        """Parse a header line and return its resulting character length.

        Return None if the line cannot be parsed.
        """
        if data[0] != 72:  # 72 == ord('H')
            # not a header line
            return False
        line = data.decode().replace("H ", "", 1)
        name, value = line.split(':', 1)
        self._headers[name.strip()] = [_trycast(s.strip()) for s in value.split(',')] if ',' in value \
            else _trycast(value.strip())
        return True

    def _find_pointers(self, f: BinaryIO):
        start = f.tell()
        first_line = f.readline()
        assert first_line.startswith("H Product:".encode('ascii')), "Unexpected log start line"
        f.seek(start)
        content = f.read()
        new_index = content.find(first_line)
        step = len(first_line)
        while -1 < new_index:
            self._log_pointers.append(start + new_index)
            new_index = content.find(first_line, new_index + step + 1)
        _log.info(f"Found log starts: {self._log_pointers}")

    def _build_field_defs(self):
        """Use the read headers to populate the `field_defs` property.
        """
        headers = self._headers
        field_defs = self._field_defs
        predictors = predictor_map
        decoders = decoder_map
        for frame_type in FrameType:
            # field header format: 'Field <FrameType> <Property>'
            for header_key, header_value in headers.items():
                if "Field " + frame_type.value not in header_key:
                    # skip headers unrelated to defining fields
                    continue
                if frame_type not in field_defs:
                    field_defs[frame_type] = [FieldDef(frame_type) for _ in range(len(header_value))]
                prop = header_key.split(" ", 2)[-1]
                for i, framedef_value in enumerate(header_value):
                    fdef_name = field_defs[frame_type][i].name
                    if fdef_name == "GPS_coord[1]" and framedef_value == 7:
                        framedef_value = 256  # catch latitude
                    field_defs[frame_type][i].__dict__[prop] = framedef_value
                    if prop == "predictor":
                        if framedef_value not in predictors:
                            raise RuntimeError("No predictor found for {:d}".format(framedef_value))
                        else:
                            field_defs[frame_type][i].predictorfun = predictors[framedef_value]
                    elif prop == "encoding":
                        if framedef_value not in decoders:
                            raise RuntimeError("No decoder found for {:d}".format(framedef_value))
                        else:
                            decoder = decoders[framedef_value]
                            if decoder.__name__.endswith("_versioned"):
                                # short circuit calls to versioned decoders
                                # noinspection PyArgumentList
                                decoder = decoder(headers.get("Data version"))
                            field_defs[frame_type][i].decoderfun = decoder
        if FrameType.INTER not in field_defs:
            # partial or missing header information
            return
        # copy field names from INTRA to INTER defs
        for i, fdef in enumerate(field_defs[FrameType.INTER]):
            fdef.name = field_defs[FrameType.INTRA][i].name

    # Optimized data access methods
    def read_batch(self, size: int) -> Optional[memoryview]:
        """Read a batch of bytes efficiently using memoryview"""
        if self._frame_data_ptr + size > self._frame_data_len:
            remaining = self._frame_data_len - self._frame_data_ptr
            if remaining <= 0:
                return None
            size = remaining

        batch = self._frame_data_view[self._frame_data_ptr:self._frame_data_ptr + size]
        self._frame_data_ptr += size
        return batch

    def read_bytes(self, count: int) -> Optional[bytes]:
        """Read multiple bytes efficiently"""
        if self._frame_data_ptr + count > self._frame_data_len:
            return None

        result = self._frame_data[self._frame_data_ptr:self._frame_data_ptr + count]
        self._frame_data_ptr += count
        return result

    def peek_byte(self) -> Optional[int]:
        """Peek at the next byte without advancing pointer"""
        if self._frame_data_ptr >= self._frame_data_len:
            return None
        return self._frame_data[self._frame_data_ptr]

    def skip_bytes(self, count: int) -> bool:
        """Skip bytes efficiently"""
        if self._frame_data_ptr + count > self._frame_data_len:
            return False
        self._frame_data_ptr += count
        return True

    @property
    def log_index(self) -> int:
        """Return the currently set log index. May return 0 if `.set_log_index()` haven't been called yet.

        :type: int
        """
        return self._log_index

    @property
    def log_count(self) -> int:
        """The number of logs in the current file.

        :type: int
        """
        return len(self._log_pointers)

    @property
    def log_pointers(self) -> List[int]:
        """List of byte pointers to the start of each log file, including headers.

        :type: List[int]
        """
        return list(self._log_pointers)

    @property
    def headers(self) -> Headers:
        """Dict of parsed headers.

        :type: dict
        """
        return dict(self._headers)

    @property
    def field_defs(self) -> Dict[FrameType, List[FieldDef]]:
        """Dict of built field definitions.

        :type: dict
        """
        return dict(self._field_defs)

    def value(self) -> Optional[int]:
        """Get current byte value.
        """
        if self._frame_data_len == self._frame_data_ptr:
            return None
        return self._frame_data[self._frame_data_ptr]

    def has_subsequent(self, data: bytes) -> bool:
        """Return `True` if upcoming bytes equal ``data``.
        """
        return self._frame_data[self._frame_data_ptr:self._frame_data_ptr + len(data)] == data

    def tell(self) -> int:
        """IO protocol
        """
        return self._frame_data_ptr

    def seek(self, n: int):
        """IO protocol
        """
        self._frame_data_ptr = n

    def __iter__(self) -> Iterator[Optional[int]]:
        return self

    def __next__(self) -> Optional[int]:
        if self._frame_data_len == self._frame_data_ptr:
            return None
        byte = self._frame_data[self._frame_data_ptr]
        self._frame_data_ptr += 1
        return byte

    def __len__(self) -> int:
        return self._frame_data_len