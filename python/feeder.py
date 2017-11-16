# --------------------------------------------------------------------------- #
# Importing section
# --------------------------------------------------------------------------- #
import logging
import pytz
import csv
import datetime
import calendar
import sys

from influxdb import InfluxDBClient

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
HOST = 'xxxxx'
PORT = 8086
USER = 'xxxxx'
PASSWORD = 'xxxxx'
DATABASE = 'xxxxx'
MEASUREMENT = 'DS'
TIME_PRECISION = 's'
INPUT_FILE = '../input/data.csv'
DELIMITER = ','
TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
MAX_LINES_PER_INSERT = 5000

tz_local = pytz.timezone('Europe/Zurich')

# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
if __name__ == "__main__":
    # --------------------------------------------------------------------------- #
    # Set logging object
    # --------------------------------------------------------------------------- #
    logging.basicConfig(format='%(asctime)-15s::%(levelname)s::%(funcName)s::%(message)s', level=logging.INFO,
                        filename=None)

    logging.info("Starting program")

    # --------------------------------------------------------------------------- #
    # Set InfluxDB instance
    # --------------------------------------------------------------------------- #
    logging.info('DB Connection')
    influxdb_client = InfluxDBClient(host=HOST, port=PORT, username=USER, password=PASSWORD, database=DATABASE)

    # --------------------------------------------------------------------------- #
    # Reading from file
    # --------------------------------------------------------------------------- #

    influxdb_data_points = []
    with open(INPUT_FILE) as csvfile:
        reader = csv.reader(csvfile, delimiter=DELIMITER)

        header = []
        for row in reader:
            if len(header) == 0:
                # Header handling
                header.append(row[0])
                header.append(row[1])
                header.append(row[2])
            else:
                # Time handling (local time with DST => UTC)
                naive_time = datetime.datetime.strptime(row[0], TIME_FORMAT)
                local_dt = tz_local.localize(naive_time, is_dst=True)
                utc_dt = local_dt.astimezone(pytz.utc)

                # Add a point for each signal
                for i in range(1, 3):
                    measurement = {
                                    'time': int(calendar.timegm(datetime.datetime.timetuple(utc_dt))),
                                    'measurement': MEASUREMENT,
                                    'fields': dict(value=float(row[i])),
                                    'tags': dict(signal=str(header[i]))
                                  }
                    influxdb_data_points.append(measurement)

                # Insert data in InfluxDB
                if len(influxdb_data_points) >= MAX_LINES_PER_INSERT:
                    try:
                        influxdb_client.write_points(influxdb_data_points, time_precision=TIME_PRECISION)
                    except Exception as e:
                        logging.error("EXCEPTION: %s" % str(e))
                    sys.exit(2)
                    logging.info('Inserted %i measurements in InfluxDB' % len(influxdb_data_points))

    # Insert remaining data in InfluxDB
    try:
        influxdb_client.write_points(influxdb_data_points, time_precision=TIME_PRECISION)
    except Exception as e:
        logging.error("EXCEPTION: %s" % str(e))
        sys.exit(2)
    logging.info('Inserted %i measurements in InfluxDB' % len(influxdb_data_points))
    logging.info("Ending program")
