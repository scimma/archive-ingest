#!/usr/bin/env python3

from global_vars import config, log
from importlib.metadata import metadata
import json
from hop import io
from dbconnector import DbConnector


# Get global instance of the job handler database interface
db = DbConnector(
    host=config['db']['host'],
    user=config['db']['user'],
    password=config['db']['pass'],
    database=config['db']['database'],
)
hop_kafka_url = config['hop']['source']['url']

# with open('/home/worker/.config/hop/auth.toml') as auth_file:
#     log.debug(auth_file.read())

# start_at = io.StartPosition.EARLIEST
# stream = io.Stream(auth=True, start_at=start_at, until_eos=False)
start_at = io.StartPosition.LATEST
stream = io.Stream(auth=False, start_at=start_at, until_eos=False)
# stream = io.Stream(until_eos=False)
with stream.open(hop_kafka_url, "r") as hop_stream:
    ## Track number of ingested alerts
    num_ingested = 0
    for message, metadata in hop_stream.read(metadata=True):
        ## Parse headers
        try:
            headers = {}
            for header in metadata.headers:
                headers[header[0]] = header[1].decode('utf-8')
            log.debug(json.dumps(headers))
        except Exception as e:
            log.error(f'''Error parsing headers: "{e}". metadata.headers: {metadata.headers}''')
        ## Construct alert object
        alert = {
            'headers': headers,
            'message': message,
        }
        log.debug(f'''\nAlert:\n{json.dumps(alert['message'])}''')
        # try:
        #     ## Insert alert data into the database
        #     if 'sender' in headers and 'schema' in headers \
        #         and headers['sender'] == 'alert-integration-demo' \
        #         and headers['schema'] == 'scimma.alert-integration-demo/source/v1':
        #         db.insert_photometry_data(message)
        #         ## Log output of every 100 alerts ingested
        #         num_ingested += 1
        #         if num_ingested % 100 == 0:
        #             log.info(f"Ingested {num_ingested} source alerts")
        #     else:
        #         log.info('Invalid alert message. Skipping...')
        #         continue
        # except Exception as e:
        #     log.error(f'''Error parsing alert: {e}. Alert: {json.dumps(alert['message'])}''')
