#!/usr/bin/env python3

from global_vars import config, log
from importlib.metadata import metadata
import json
from hop import io
from dbconnector import DbConnector

db = DbConnector(
    host=config['db']['host'],
    user=config['db']['user'],
    password=config['db']['pass'],
    database=config['db']['database'],
)


def main():
    # Get global instance of the job handler database interface
    hop_kafka_url = config['hop']['source']['url']
    log.debug(hop_kafka_url)

    # with open('/home/worker/.config/hop/auth.toml') as auth_file:
    #     log.debug(auth_file.read())

    # start_at = io.StartPosition.EARLIEST
    # stream = io.Stream(auth=True, start_at=start_at, until_eos=False)
    start_at = io.StartPosition.LATEST
    # stream = io.Stream(auth=False, start_at=start_at, until_eos=False)
    stream = io.Stream(until_eos=False)
    with stream.open(hop_kafka_url, "r") as hop_stream:
        ## Track number of ingested alerts
        num_ingested = 0
        for message, metadata in hop_stream.read(metadata=True):
            # ## Parse headers
            # try:
            #     headers = {}
            #     for header in metadata.headers:
            #         headers[header[0]] = header[1].decode('utf-8')
            #     log.debug(json.dumps(headers))
            # except Exception as e:
            #     log.error(f'''Error parsing headers: "{e}".''')
            # ## Construct alert object
            # alert = {
            #     'headers': headers,
            #     'message': message,
            # }
            # log.debug(f'''\nAlert:\n{alert['message']}''')
            log.debug(f'''{message.serialize()}''')
            message_serialized = message.serialize()
            if message_serialized['format'] == 'json':
                json_content = message_serialized['content'].decode('utf-8')
                log.debug(json_content)
                try:
                    ## Insert alert data into the database
                    db.insert_message(json_content)
                except Exception as e:
                    log.error(f'''Error parsing alert: {e}''')


if __name__ == "__main__":
    import time
    try:
        # Wait for database to come online if it is still starting
        waiting_for_db = True
        while waiting_for_db:
            try:
                db.open_db_connection()
                waiting_for_db = False
                db.close_db_connection()
            except:
                log.warning(
                    'Unable to connect to database. Waiting to try again...')
                time.sleep(5.0)
        ## Create/update database tables
        db.update_db_tables()
        main()
    except Exception as e:
        log.error(str(e).strip())
