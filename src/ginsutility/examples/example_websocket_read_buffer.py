"""Read data from a stream through a websocket connection"""

import json
import argparse
import ginsutility.highspeedport as highspeedport

# Class to clean up output of -h in cli
from ginsutility.examples.CustomHelpFormatter import CustomHelpFormatter

parser = argparse.ArgumentParser(
    description="Write a value to one or more variables over an IP-Address",
    formatter_class=CustomHelpFormatter,
    add_help=False,
)

parser.add_argument(
    "-h",
    "--help",
    action="help",
    default=argparse.SUPPRESS,
    help="Show this help message and exit; \
                        All arguments are optional; \
                        Strings do not need to be quoted",
)
parser.add_argument(
    "-w",
    "--websocket_url",
    type=str,
    help="URL of websocket; Default is 127.0.0.1",
    required=False,
    default="127.0.0.1",
    metavar="",
)
parser.add_argument(
    "-:",
    "--port",
    type=int,
    help="Port of websocket; Default is 8090",
    required=False,
    default=8090,
    metavar="",
)
parser.add_argument(
    "-r",
    "--route",
    type=str,
    help="Route of anything connected to websocket; \
                        Default is an empty string",
    required=False,
    default="",
    metavar="",
)
parser.add_argument(
    "-u",
    "--username",
    type=str,
    help="Username for websocket (must be an admin user on Qcore!). "
         "Default is an empty string.",
    required=False,
    default="",
    metavar="",
)
parser.add_argument(
    "-p",
    "--password",
    type=str,
    help="Password for websocket (must be the admin password on Qcore!). "
         "Default is an empty string.",
    required=False,
    default="",
    metavar="",
)
parser.add_argument(
    "-t",
    "--timeout",
    type=int,
    help="Timeout for websocket connection initialisation in seconds; \
                        Default is 10 seconds",
    required=False,
    default=10,
    metavar="",
)
parser.add_argument(
    "-s",
    "--sample_rate",
    type=int,
    help="Sampling rate in Hz; Default is 1",
    required=False,
    default=1,
    metavar="",
)
parser.add_argument(
    "-d",
    "--stream_id",
    type=str,
    help="Stream ID of the stream; \
                    Default is 0. A controller has always a buffer with index 0 \
                    when using GI.bench you can also use the UUID",
    required=False,
    default="0",
    metavar="",
)
parser.add_argument(
    "-x",
    "--start_time",
    type=int,
    help=">1: Value explicitly set; \
                        1: Start at the very end, end must not be 0; \
                        -n: If start is negative, start will be n milli seconds before end; \
                        0: Start at the beginning; Default is 0;",
    required=False,
    default=0,
    metavar="",
)
parser.add_argument(
    "-y",
    "--end_time",
    type=int,
    help="Can be set to a specific value or 0 or -1 to deactivate; \
                        Default is 0;",
    required=False,
    default=0,
    metavar="",
)
parser.add_argument(
    "-b",
    "--buffer_type",
    type=str,
    help='Accepted values are "BUFFER", "HSP_ARCHIVES" and "HSP_FILES"; \
                        Default is "BUFFER"',
    required=False,
    default="BUFFER",
    metavar="",
)

args = parser.parse_args()

url = args.websocket_url
port = args.port
route = args.route
username = args.username
password = args.password
timeout_sec = args.timeout
sample_rate = args.sample_rate
stream_id = args.stream_id
start_time = args.start_time
end_time = args.end_time
buffer_type = args.buffer_type

sample_rate_ms = int(1000 / sample_rate)  # Sampling rate in milliseconds

# Create json object to pass configuration parameters
add_config = {}
add_config["SampleRate"] = 100
add_config["Variables"] = ["Analog_In1", "Analog_In2"]
add_config = json.dumps(add_config)

conn = highspeedport.HighSpeedPortClient()

conn.init_websocket_stream(
    url,
    port,
    route,
    username,
    password,
    stream_id,
    start_time,
    end_time,
    timeout_sec,
    buffer_type,
    # add_config, # Comment out (None) empty if all variables are wanted
)
data = conn.yield_buffer()
readbuffer = next(data)  # Only reads first buffer frame
print(readbuffer)
print(readbuffer.shape)
conn.close_connection()
