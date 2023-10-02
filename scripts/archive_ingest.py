#!/usr/bin/env python3

'''
Log messages, metadata, and annotations to S3.
Record meta-data about stored data in relational DB.  Omit duplicate due to
more-than-once delivery, user error, or cursor resets.

Run time configuration is via command line options, corresponding environment
variables, or entries in a TOML configuration file. In general, CLI option
--foo-bar is associated with the environment variable FOO_BAR or the TOML key
foo_bar. Environment variables take precedence over built-in default values,
and command line arguments and values read from a configuration file take
precedence in the order they are encountered (i.e. last wins). 

Some important properties can be configured only via environment variable:
- HOP_PASSWORD is used to set a hop/Kafka credential password, used with 
               HOP_USERNAME/--hop-username
- DB_PASSWORD is used to set the password for connecting to an SQL database
            at --db-host/DB_HOST and --db-port/DB_PORT
- S3_ACCESS_KEY_ID and S3_SECRET_ACCESS_KEY are used for object store
                                            credentials

Support testing via
- reading limited to a test topic
- reading from a mock topic
- in-line comparison of as-sent test data
  to as-received test data.
- in-line comparison of as-received data
  to as-stored data.

@author: Mahmoud Parvizi (parvizim@msu.edu)
@author: Don Petravick (petravick@illinois.edu)

'''
import logging
import argparse
import asyncio
from archive import utility_api
from archive import consumer_api
from archive import access_api


##################################
#   environment
##################################


async def archive_ingest(config):
    """
    Acquire data from the specified consumer and record.

    - For testing compare as-sent to as-received.
    - Filter out duplicates.
    - Log to S3
    - Log to DB
    - Mark message processed in kafka.
    """
    consumer = consumer_api.ConsumerFactory(config)
    access   = access_api.Archive_access(config)
    consumer.connect()
    await access.connect()
    await access.db.make_schema()
    for payload, metadata in consumer.get_next():
        stored, reason = await access.store_message(payload, metadata)
        consumer.mark_done(metadata)

    consumer.close()

if __name__ == "__main__":
    import os

    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    
    utility_api.add_parser_options(parser)
    consumer_api.add_parser_options(parser)
    access_api.add_parser_options(parser)

    config = parser.parse_args().__dict__

    # intentionally 'parse' an empty list of arguments to set defaults
    raw_config = parser.parse_args([])
    if "CONFIG_FILE" in os.environ:
        utility_api.load_toml_config(os.environ["CONFIG_FILE"], raw_config)
    config = raw_config.__dict__

    utility_api.make_logging(config)
    logging.info(config)
    
    if config.get("read_only", False):
        logging.fatal("The archiver cannot operate in read-only mode")
        exit(1)

    asyncio.run(archive_ingest(config))
