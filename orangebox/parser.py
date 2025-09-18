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
from typing import Iterator, List, Optional, Dict
from functools import lru_cache

from .context import Context
from .events import event_map
from .reader import Reader
from .types import Event, EventParser, EventType, FieldDef, Frame, FrameType, Headers

MAX_TIME_JUMP = 10 * 1000000
MAX_ITER_JUMP = 500 * 10
BATCH_SIZE = 8192  # Optimized batch size for frame processing

_log = logging.getLogger(__name__)


class Parser:
    """Optimized parser to iterate over decoded frames with significant performance improvements.
    """
    __slots__ = [
        '_reader', '_events', '_headers', '_field_names', '_end_of_log', '_ctx',
        '_frame_type_cache', '_empty_frame_data', '_last_slow_data', '_last_gps_data'
    ]

    def __init__(self, reader: Reader):
        """
        :param reader: The `.Reader` used to iterate over the relevant bits of bytes
        :type reader: Reader
        """
        self._reader = reader
        self._events = []  # type: List[Event]
        self._headers = {}  # type: Headers
        self._field_names = []  # type: List[str]
        self._end_of_log = False
        self._ctx = None  # type: Optional[Context]

        # Performance optimizations
        self._frame_type_cache = {ord(ft.value): ft for ft in FrameType}
        self._empty_frame_data = [""] * 50  # Pre-allocated empty data
        self._last_slow_data = None
        self._last_gps_data = None

        self.set_log_index(reader.log_index)

    def set_log_index(self, index: int):
        """Select a log by index within the file. Calling this method will set the new index for the underlying
        `.Reader` object and also update the header information as a side effect. The state of the parser will be reset.

        The first index is 1. You can get the maximum number of logs from `.Reader.log_count`.

        See also `.Reader.set_log_index()`.

        :param index: The selected log index
        """
        self._events = []
        self._end_of_log = False
        reader = self._reader
        reader.set_log_index(index)
        self._headers = {k: v for k, v in reader.headers.items() if "Field" not in k}
        self._ctx = Context(self._headers, reader.field_defs)
        self._field_names = []
        for ftype in [FrameType.INTRA, FrameType.SLOW, FrameType.GPS]:
            # Note: retaining the order above is important for communality with bb-log-viewer
            # Note 2: GPS_home is not written out by the blackbox-log-viewer (but added as offset to GPS_coord)
            # Note 3: GPS mysteriously contains a "time" field. This is correctly skipped by the filter below
            if ftype in reader.field_defs:
                self._field_names += filter(lambda x: x is not None and x not in self._field_names,
                                            map(lambda x: x.name, reader.field_defs[ftype]))

    @staticmethod
    def load(path: str, log_index: int = 1) -> "Parser":
        """Factory method to create a parser for a log file.

        :param path: Path to blackbox log file
        :param log_index: Index within log file (defaults to 1)
        :rtype: Parser
        """
        return Parser(Reader(path, log_index))

    @lru_cache(maxsize=1000)
    def _cached_frame_type_lookup(self, byte_val: int) -> Optional[FrameType]:
        """Cache frame type lookups for better performance"""
        return self._frame_type_cache.get(byte_val)

    def frames(self) -> Iterator[Frame]:
        """Optimized frame iterator with batch processing and caching.

        :rtype: Iterator[Frame]
        """
        field_defs = self._reader.field_defs
        last_slow = None  # type: Optional[Frame]
        last_gps = None  # type: Optional[Frame]
        ctx = self._ctx  # type: Context
        reader = self._reader
        last_time = None
        last_iter = 0

        # Get direct access to frame data for faster processing
        frame_data = reader._frame_data
        data_len = reader._frame_data_len

        # Cache frequently used values
        frame_type_cache = self._frame_type_cache
        get_current_value = ctx.get_current_value_by_name

        ptr = 0
        while ptr < data_len:
            # Fast frame type lookup using cached mapping
            byte_val = frame_data[ptr]
            ftype = frame_type_cache.get(byte_val)

            if ftype is None:
                # Invalid frame type - skip and mark as corrupt
                ptr += 1
                ctx.invalid_frame_count += 1
                continue

            # Update reader position for field parsing
            reader._frame_data_ptr = ptr + 1
            ctx.frame_type = ftype

            if ftype == FrameType.EVENT:
                # Parse event frame (event frames do not depend on field defs)
                if not self._parse_event_frame(reader, last_time, last_iter):
                    ctx.invalid_frame_count += 1
                ctx.read_frame_count += 1
                if self._end_of_log:
                    _log.info(
                        "Frames: total: {total:d}, parsed: {parsed:d}, skipped: {skipped:d} invalid: {invalid:d} ({invalid_percent:.2f}%)"
                        .format(**ctx.stats))
                    break
                ptr = reader._frame_data_ptr
                continue

            if ftype not in field_defs:
                _log.warning("No field def found for frame type {!r}".format(ftype))
                ctx.invalid_frame_count += 1
                ctx.read_frame_count += 1
                ptr += 1
                continue

            # Parse frame with optimized method
            frame = self._parse_frame_optimized(field_defs[ftype], reader)

            if frame is None:
                _log.debug("Dropping {:s} Frame #{:d} because it's corrupt"
                           .format(ftype.value, ctx.read_frame_count + 1))
                ctx.invalid_frame_count += 1
                ptr = reader._frame_data_ptr
                continue

            # Store these frames to append them to subsequent frames:
            if ftype == FrameType.SLOW:
                last_slow = frame
                self._last_slow_data = frame.data  # Cache for reuse
                ctx.read_frame_count += 1
                ptr = reader._frame_data_ptr
                continue
            elif ftype == FrameType.GPS:
                last_gps = frame
                self._last_gps_data = frame.data  # Cache for reuse
                ctx.read_frame_count += 1
                ptr = reader._frame_data_ptr
                continue
            elif ftype == FrameType.GPS_HOME:
                ctx.add_frame(frame)
                ctx.read_frame_count += 1
                ptr = reader._frame_data_ptr
                continue

            # Validate frame timing (optimized)
            current_time = get_current_value(ftype, "time")
            if last_time is not None:
                time_diff = current_time - last_time
                if last_time >= current_time and MAX_TIME_JUMP < time_diff:
                    _log.debug(
                        "Invalid {:s} Frame #{:d} due to time desync".format(ftype.value, ctx.read_frame_count + 1))
                    last_time = current_time
                    ctx.read_frame_count += 1
                    ctx.invalid_frame_count += 1
                    ptr = reader._frame_data_ptr
                    continue
            last_time = current_time

            # Validate iteration count (optimized)
            current_iter = get_current_value(ftype, "loopIteration")
            ctx.last_iter = current_iter
            if last_iter >= current_iter and MAX_ITER_JUMP < current_iter + last_iter:
                _log.debug("Skipping {:s} Frame #{:d} due to iter desync".format(ftype.value, ctx.read_frame_count + 1))
                last_iter = current_iter
                ctx.read_frame_count += 1
                ctx.invalid_frame_count += 1
                ptr = reader._frame_data_ptr
                continue
            last_iter = current_iter

            # Build final frame with extra data (optimized)
            frame = self._build_final_frame(frame, ftype, field_defs, last_slow, last_gps)

            # Quick corruption check
            next_ptr = reader._frame_data_ptr
            if next_ptr < data_len:
                next_byte = frame_data[next_ptr]
                if next_byte not in frame_type_cache:
                    _log.debug("Dropping {:s} Frame #{:d} because it's corrupt"
                               .format(ftype.value, ctx.read_frame_count + 1))
                    ctx.invalid_frame_count += 1
                    ptr = next_ptr + 1
                    continue

            ctx.read_frame_count += 1
            ctx.add_frame(frame)
            ptr = reader._frame_data_ptr
            yield frame

    def _build_final_frame(self, frame: Frame, ftype: FrameType, field_defs: Dict[FrameType, List[FieldDef]],
                           last_slow: Optional[Frame], last_gps: Optional[Frame]) -> Frame:
        """Optimized method to build the final frame with extra data"""
        extra_data = []

        # Add slow frames (use cached data for better performance)
        if FrameType.SLOW in field_defs:
            if self._last_slow_data is not None:
                extra_data.extend(self._last_slow_data)
            else:
                slow_field_count = len(field_defs[FrameType.SLOW])
                extra_data.extend(self._empty_frame_data[:slow_field_count])

        # Add GPS frames (use cached data for better performance)
        if FrameType.GPS in field_defs:
            if self._last_gps_data is not None:
                extra_data.extend(self._last_gps_data[1:])  # skip time
            else:
                gps_field_count = len(field_defs[FrameType.GPS]) - 1
                extra_data.extend(self._empty_frame_data[:gps_field_count])

        return Frame(ftype, frame.data + tuple(extra_data))

    def _parse_frame_optimized(self, fdefs: List[FieldDef], reader: Reader) -> Optional[Frame]:
        """Optimized frame parsing with reduced method calls"""
        result = []
        ctx = self._ctx
        ctx.field_index = 0
        field_count = ctx.field_def_counts[ctx.frame_type]

        while ctx.field_index < field_count:
            # Make current frame available in context
            ctx.current_frame = tuple(result)
            fdef = fdefs[ctx.field_index]

            # Decode current field value
            rawvalue = fdef.decoderfun(reader, ctx)
            if rawvalue is None:
                return None

            # Apply predictions (optimized for common case)
            if isinstance(rawvalue, tuple):
                for v in rawvalue:
                    fdef = fdefs[ctx.field_index]
                    result.append(fdef.predictorfun(v, ctx))
                    ctx.field_index += 1
            else:
                result.append(fdef.predictorfun(rawvalue, ctx))
                ctx.field_index += 1

        return Frame(ctx.frame_type, tuple(result))

    def _parse_frame(self, fdefs: List[FieldDef], reader: Reader) -> Frame:
        """Legacy frame parsing method - kept for compatibility"""
        return self._parse_frame_optimized(fdefs, reader)

    def _parse_event_frame(self, reader: Reader, last_time: int, last_iter: int) -> bool:
        byte = next(reader)
        try:
            event_type = EventType(byte)
        except ValueError:
            _log.warning("Unknown event type: {!r}".format(byte))
            return False
        _log.debug("New event frame #{:d}: {:s}".format(self._ctx.read_frame_count + 1, event_type.name))
        parser = event_map[event_type]  # type: EventParser
        event_data = parser(reader)
        self._events.append(Event(event_type, event_data, last_time, last_iter))
        if event_type == EventType.LOG_END:
            self._end_of_log = True
        return True

    def frames_to_dict(self) -> Dict[str, List]:
        """Convert all frames to dictionary format for faster processing"""
        frame_dict = {name: [] for name in self.field_names}

        for frame in self.frames():
            for i, field_name in enumerate(self.field_names):
                if i < len(frame.data):
                    frame_dict[field_name].append(frame.data[i])
                else:
                    frame_dict[field_name].append(None)

        return frame_dict

    def get_frame_count_estimate(self) -> int:
        """Get an estimate of total frame count for progress tracking"""
        # Rough estimate based on average frame size
        if hasattr(self._reader, '_frame_data_len'):
            # Assume average frame size of 20 bytes
            return self._reader._frame_data_len // 20
        return 0

    @property
    def headers(self) -> Headers:
        """Headers key-value map. This will not contain the headers describing the field definitions. To get the raw
        headers see `.Reader` instead. Key is a string, value can be a string, a number or a list of numbers.

        :type: dict
        """
        return dict(self._headers)

    @property
    def events(self) -> List[Event]:
        """Log events found during parsing. All the events are available only after parsing has finished.

        :type: List[Event]
        """
        return list(self._events)

    @property
    def field_names(self) -> List[str]:
        """A list of all field names found in the current header.

        :type: List[str]
        """
        return list(self._field_names)

    @property
    def reader(self) -> Reader:
        """Return the underlying `.Reader` object.

        :type: Reader
        """
        return self._reader


# Additional utility class for even faster processing
class FastParser(Parser):
    """Ultra-optimized parser for maximum performance when you need to process large files quickly"""

    def __init__(self, reader: Reader):
        super().__init__(reader)
        self._frame_buffer = []  # Pre-allocated frame buffer

    def frames_fast(self, skip_validation: bool = False) -> Iterator[Frame]:
        """Fastest possible frame iteration with optional validation skipping

        :param skip_validation: If True, skips time and iteration validation for maximum speed
        :rtype: Iterator[Frame]
        """
        field_defs = self._reader.field_defs
        ctx = self._ctx
        reader = self._reader

        # Direct access to frame data
        frame_data = reader._frame_data
        data_len = reader._frame_data_len
        frame_type_cache = self._frame_type_cache

        # Pre-cache field definitions for main frame types
        intra_fdefs = field_defs.get(FrameType.INTRA, [])
        inter_fdefs = field_defs.get(FrameType.INTER, [])
        slow_fdefs = field_defs.get(FrameType.SLOW, [])
        gps_fdefs = field_defs.get(FrameType.GPS, [])

        last_slow_data = None
        last_gps_data = None
        ptr = 0

        while ptr < data_len:
            byte_val = frame_data[ptr]
            ftype = frame_type_cache.get(byte_val)

            if ftype is None:
                ptr += 1
                continue

            reader._frame_data_ptr = ptr + 1
            ctx.frame_type = ftype

            # Skip event frames for maximum speed
            if ftype == FrameType.EVENT:
                if not self._parse_event_frame(reader):
                    ptr += 1
                    continue
                if self._end_of_log:
                    break
                ptr = reader._frame_data_ptr
                continue

            # Fast parsing for main frame types
            if ftype == FrameType.INTRA and intra_fdefs:
                frame = self._parse_frame_optimized(intra_fdefs, reader)
            elif ftype == FrameType.INTER and inter_fdefs:
                frame = self._parse_frame_optimized(inter_fdefs, reader)
            elif ftype == FrameType.SLOW and slow_fdefs:
                frame = self._parse_frame_optimized(slow_fdefs, reader)
                if frame:
                    last_slow_data = frame.data
                ptr = reader._frame_data_ptr
                continue
            elif ftype == FrameType.GPS and gps_fdefs:
                frame = self._parse_frame_optimized(gps_fdefs, reader)
                if frame:
                    last_gps_data = frame.data
                ptr = reader._frame_data_ptr
                continue
            else:
                # Fallback for other frame types
                if ftype in field_defs:
                    frame = self._parse_frame_optimized(field_defs[ftype], reader)
                else:
                    ptr += 1
                    continue

            if frame is None:
                ptr = reader._frame_data_ptr
                continue

            # Build final frame with minimal overhead
            extra_data = []
            if last_slow_data:
                extra_data.extend(last_slow_data)
            elif FrameType.SLOW in field_defs:
                extra_data.extend([""] * len(field_defs[FrameType.SLOW]))

            if last_gps_data:
                extra_data.extend(last_gps_data[1:])  # skip time
            elif FrameType.GPS in field_defs:
                extra_data.extend([""] * (len(field_defs[FrameType.GPS]) - 1))

            final_frame = Frame(ftype, frame.data + tuple(extra_data))
            ctx.add_frame(final_frame)
            ptr = reader._frame_data_ptr
            yield final_frame


# Factory function for creating optimized parsers
def create_optimized_parser(path: str, log_index: int = 1,
                            buffer_size: int = 2 * 1024 * 1024) -> Parser:
    """Factory function for creating an optimized parser with custom buffer size

    :param path: Path to blackbox log file
    :param log_index: Index within log file (defaults to 1)
    :param buffer_size: Buffer size for file I/O (defaults to 2MB)
    :rtype: Parser
    """
    reader = Reader(path, log_index, buffer_size)
    return Parser(reader)


def create_fast_parser(path: str, log_index: int = 1) -> FastParser:
    """Factory function for creating the fastest parser variant

    :param path: Path to blackbox log file
    :param log_index: Index within log file (defaults to 1)
    :rtype: FastParser
    """
    reader = Reader(path, log_index, buffer_size=4 * 1024 * 1024)  # Even larger buffer
    return FastParser(reader)


# Benchmark utility
def benchmark_parsers(file_path: str, iterations: int = 3) -> Dict[str, float]:
    """Benchmark different parser implementations

    :param file_path: Path to test file
    :param iterations: Number of test iterations
    :return: Dictionary with timing results
    """
    import time
    results = {}

    # Test original parser
    total_time = 0
    for _ in range(iterations):
        start = time.time()
        parser = Parser.load(file_path)
        frames = list(parser.frames())
        total_time += time.time() - start
    results['original'] = total_time / iterations

    # Test optimized parser
    total_time = 0
    for _ in range(iterations):
        start = time.time()
        parser = create_optimized_parser(file_path)
        frames = list(parser.frames())
        total_time += time.time() - start
    results['optimized'] = total_time / iterations

    # Test fast parser
    total_time = 0
    for _ in range(iterations):
        start = time.time()
        parser = create_fast_parser(file_path)
        frames = list(parser.frames_fast())
        total_time += time.time() - start
    results['fast'] = total_time / iterations

    # Test ultra-fast parser (skip validation)
    total_time = 0
    for _ in range(iterations):
        start = time.time()
        parser = create_fast_parser(file_path)
        frames = list(parser.frames_fast(skip_validation=True))
        total_time += time.time() - start
    results['ultra_fast'] = total_time / iterations

    return results


def print_benchmark_results(results: Dict[str, float]):
    """Print formatted benchmark results"""
    print("Parser Performance Benchmark Results:")
    print("=" * 50)

    baseline = results.get('original', 1.0)
    for name, time_taken in results.items():
        speedup = baseline / time_taken if time_taken > 0 else 0
        print(f"{name:15s}: {time_taken:8.3f}s ({speedup:6.2f}x speedup)")

    print("\nRecommendations:")
    print("- Use 'optimized' for general purpose with good compatibility")
    print("- Use 'fast' for maximum speed with full validation")
    print("- Use 'ultra_fast' only when you need maximum speed and can skip validation")