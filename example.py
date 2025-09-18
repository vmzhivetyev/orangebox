from orangebox import Parser

# file_path = '/Users/vmzhivetev/git/betaflight_blackbox/8 feb (PP)/btfl_004.bbl'
file_path = '/Users/vmzhivetev/git/betaflight_blackbox/21 feb/roxy/btfl_005.bbl'

parser = Parser.load(file_path)
# or optionally select a log by index (1 is the default)
# parser = Parser.load("btfl_all.bbl", 1)

# Print headers
print("headers:", parser.headers)

# Print the names of fields
print("field names:", parser.field_names)

# Select a specific log within the file by index
print("log count:", parser.reader.log_count)

log_count = parser.reader.log_count

for log_idx in range(11, log_count + 1):
    print(f"reading log #{log_idx}/{log_count}...")
    parser.set_log_index(log_idx)
    frame_count = 0
    for frame in parser.frames():
        frame_count += 1
    print(f"total frames: {frame_count}")

# Complete list of events only available once all frames have been parsed
print("events:", parser.events)
