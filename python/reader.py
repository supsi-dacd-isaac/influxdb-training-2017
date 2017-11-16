# --------------------------------------------------------------------------- #
# Importing section
# --------------------------------------------------------------------------- #
import logging

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
STR_QUERY = 'SELECT value, signal FROM %s WHERE signal=\'V2\' AND time>=\'2017-11-19T09:20:00Z\' AND ' \
            'time<=\'2017-11-19T09:40:00Z\'' % MEASUREMENT

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
    # Query the database
    # --------------------------------------------------------------------------- #

    result = influxdb_client.query(query=STR_QUERY)

    for returned_data in result:
        arr_data = returned_data
        logging.info('Found %i samples' % len(arr_data))
        # Cycling on the dataset and print out
        for data in arr_data:
            logging.info('ts=%s;signal=\'%s\';value=%.2f' % (data['time'], data['signal'], data['value']))
    logging.info("Ending program")
