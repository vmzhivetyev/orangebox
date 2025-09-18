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
from typing import Dict, Optional

from .decoders import _unsigned_vb
from .reader import Reader
from .tools import map_to
from .types import EventParser, EventType

END_OF_LOG_MESSAGE = b'End of log\x00'

event_map = dict()  # type: Dict[EventType, EventParser]


@map_to(EventType.SYNC_BEEP, event_map)
def sync_beep(data: Reader) -> Optional[dict]:
    return {"time": _unsigned_vb(data), }


@map_to(EventType.FLIGHT_MODE, event_map)
def flight_mode(data: Reader) -> Optional[dict]:
    return {
        "new_flags": _unsigned_vb(data),
        "old_flags": _unsigned_vb(data),
    }


@map_to(EventType.AUTOTUNE_TARGETS, event_map)
def autotune_targets(_: Reader) -> Optional[dict]:
    # TODO
    pass


@map_to(EventType.AUTOTUNE_CYCLE_START, event_map)
def autotune_cycle_start(_: Reader) -> Optional[dict]:
    # TODO
    pass


@map_to(EventType.AUTOTUNE_CYCLE_RESULT, event_map)
def autotune_cycle_result(_: Reader) -> Optional[dict]:
    # TODO
    pass


@map_to(EventType.GTUNE_CYCLE_RESULT, event_map)
def gtune_cycle_result(_: Reader) -> Optional[dict]:
    # TODO
    pass


@map_to(EventType.CUSTOM, event_map)
def custom(_: Reader) -> Optional[dict]:
    # TODO
    pass


@map_to(EventType.CUSTOM_BLANK, event_map)
def custom_blank(_: Reader) -> Optional[dict]:
    # TODO
    pass


@map_to(EventType.TWITCH_TEST, event_map)
def twitch_test(_: Reader) -> Optional[dict]:
    # TODO
    pass


INFLIGHT_ADJUSTMENT_FUNCTIONS = [
    {"name": "None"},
    {"name": "RC Rate", "scale": 0.01},
    {"name": "RC Expo", "scale": 0.01},
    {"name": "Throttle Expo", "scale": 0.01},
    {"name": "Pitch & Roll Rate", "scale": 0.01},
    {"name": "Yaw rate", "scale": 0.01},
    {"name": "Pitch & Roll P", "scale": 0.1, "scalef": 1},
    {"name": "Pitch & Roll I", "scale": 0.001, "scalef": 0.1},
    {"name": "Pitch & Roll D", "scalef": 1000},
    {"name": "Yaw P", "scale": 0.1, "scalef": 1},
    {"name": "Yaw I", "scale": 0.001, "scalef": 0.1},
    {"name": "Yaw D", "scalef": 1000},
    {"name": "Rate Profile"},
    {"name": "Pitch Rate", "scale": 0.01},
    {"name": "Roll Rate", "scale": 0.01},
    {"name": "Pitch P", "scale": 0.1, "scalef": 1},
    {"name": "Pitch I", "scale": 0.001, "scalef": 0.1},
    {"name": "Pitch D", "scalef": 1000},
    {"name": "Roll P", "scale": 0.1, "scalef": 1},
    {"name": "Roll I", "scale": 0.001, "scalef": 0.1},
    {"name": "Roll D", "scalef": 1000},
]


def uint32ToFloat(value: int) -> float:
    """Convert a 32-bit unsigned integer to IEEE 754 float representation"""
    return struct.unpack('>f', struct.pack('>I', value))[0]


def read_signed_vb(reader) -> int:
    """Read a signed variable-length byte sequence"""
    value = 0
    shift = 0

    while True:
        byte = next(reader)
        value |= (byte & 0x7F) << shift
        if (byte & 0x80) == 0:
            break
        shift += 7

    # Handle sign extension for negative values
    if value & (1 << (shift + 6)):  # Check if sign bit is set
        value |= -(1 << (shift + 7))  # Sign extend

    return value


def read_u32(reader) -> int:
    """Read a 32-bit unsigned integer (big-endian)"""
    b1 = next(reader)
    b2 = next(reader)
    b3 = next(reader)
    b4 = next(reader)
    return (b1 << 24) | (b2 << 16) | (b3 << 8) | b4


@map_to(EventType.INFLIGHT_ADJUSTMENT, event_map)
def inflight_adjustment(reader: Reader) -> Optional[dict]:
    """
    Parse inflight adjustment event data.
    
    The first byte contains:
    - Lower 7 bits: function index
    - 8th bit: data type flag (0=integer, 1=float)
    """
    # Read the first byte which contains both function index and data type flag
    tmp = next(reader)
    
    # Extract function index from lower 7 bits
    func = tmp & 127
    
    # Read value based on the 8th bit flag
    if tmp < 128:
        # Integer data - read signed variable-length byte
        value = read_signed_vb(reader)
    else:
        # Float data - read 32-bit unsigned integer and convert to float
        value = uint32ToFloat(read_u32(reader))
    
    # Initialize result with default values
    result = {
        "name": "Unknown",
        "func": func,
        "value": value
    }
    
    # Look up function description if available
    if func < len(INFLIGHT_ADJUSTMENT_FUNCTIONS):
        descr = INFLIGHT_ADJUSTMENT_FUNCTIONS[func]
        result["name"] = descr["name"]
        
        # Apply scaling
        scale = 1
        
        # Use base scale if available
        if "scale" in descr:
            scale = descr["scale"]
        
        # Use different scale for float data if available
        if tmp >= 128 and "scalef" in descr:
            scale = descr["scalef"]
        
        # Apply scaling and round to 4 decimal places
        # This matches the JavaScript: Math.round(value * scale * 10000) / 10000
        result["value"] = round(value * scale, 4)
    
    return result


@map_to(EventType.LOGGING_RESUME, event_map)
def logging_resume(_: Reader) -> Optional[dict]:
    # TODO
    pass


@map_to(EventType.LOG_END, event_map)
def logging_end(data: Reader) -> Optional[dict]:
    if not data.has_subsequent(END_OF_LOG_MESSAGE):
        logging.error("Invalid 'End of log' message")
    return None
