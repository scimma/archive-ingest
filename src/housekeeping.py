#!/usr/bin/env python3

'''
Log messages, metadata, and annotatopns to S3.
Record meta-data about stored data in relational
DB.  Omit duplicate due to more-tnan-once delivery or
cursor resets.

Run time configuration is via command line options,
including options to select configuration stanzas
from a "toml" configuration file.

Support testing via
- reading limited to a test topic
- reading from a mock topic
- in-line comparison of as-sent test data
  to as-recieved test data.
- in-line comparision of as-recieved data
  to as-stored data.

@author: Mahmoud Parvizi (parvizim@msu.edu)
@author: Don Petravick (petravick@illinois.edu)

'''
import toml
import logging
import argparse
import consumer_api
import store_api
import database_api
import verify_api
import decision_api

from hop.io import Stream, StartPosition, list_topics
import hop

##################################
#   environment
##################################

def make_logging(args):
    """
    establish python logging

    use toml file stanza passed specified on command line.
    """

    # supply defaults to assure that logging of some ...
    # ... sort is setup no matter what.
    # Note that the production environment captures ..
    # ...stdout/stderr into logs for us.
    default_format  = '%(asctime)s:%(filename)s:%(levelname)s:%(message)s'
    toml_data = toml.load(args["toml_file"])
    config    =  toml_data[args["log_stanza"]]
    level    = config.get("level", "DEBUG")
    format   = config.get("format", default_format)
    logging.basicConfig(level=level, format=format)
    logging.info(f"Basic logging is configured at {level}")


##############
#
#  argparse functions
#
###############
def housekeep(args):
    """
    Acquire data from the specified consumer and record.

    - For testing compare as-sent to as-recieved.
    - Filter out duplicates.
    - Log to S3
    - Log to DB
    - Optionally compare as-recieved to as-stored.
    - Mark message processed in kafka.
    """
    db     = database_api.DbFactory(args).get_db()
    consumer = consumer_api.ConsumerFactory(args).get_consumer()
    store  = store_api.StoreFactory(args).get_store()
    db.connect()
    db.make_schema()
    consumer.connect()
    store.connect()
    for payload, metadata, annotations  in consumer.get_next():
        if decision_api.is_deemed_duplicate(annotations, metadata, db, store):
            logging.info(f"Duplicate not logged {annotations}")
            consumer.mark_done()
            continue
        if args["test_topic"]:
            if payload["content"] == b"end": exit(0)
            if args["verify"] and verify_api.is_known_test_data(metadata):
                verify_api.compare_known_data(payload, metadata)
        storeinfo = store.store(payload, metadata, annotations)
        db.insert(payload, metadata, annotations)
        verify_api.assert_ok(args, payload, metadata, annotations,  db, store)
        consumer.mark_done()


def list(args):
    "list the stanzas so I dont have to grep toml files"
    import pprint
    dict = toml.load(args["toml_file"])
    print (args["toml_file"])
    pprint.pprint(dict)

if __name__ == "__main__":

    #main_parser = argparse.ArgumentParser(add_help=False)
    main_parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    main_parser.set_defaults(func=None)
    main_parser.add_argument("-t", "--toml_file", help = "toml configuration file", default="housekeeping.toml")
    main_parser.add_argument("-l", "--log_stanza", help = "log config stanza", default="log")

    subparsers = main_parser.add_subparsers()
    #run -- perform housekeeping with
    parser = subparsers.add_parser('run', help= "house keep w/(defaults) all mocks")
    parser.set_defaults(func=housekeep)
    parser.add_argument("-D", "--database_stanza", help = "database-config-stanza", default="mock-db")
    parser.add_argument("-H", "--hop_stanza", help = "hopskotch config  stanza", default="mock-hop")
    parser.add_argument("-S", "--store_stanza", help = "storage config stanza", default="mock-store")
    parser.add_argument("-t", "--test_topic", help = "consume only from test_topic", default=False, action="store_true")
    parser.add_argument("-v", "--verify", help = "check after ingest", action='store_true' ,default=False)

    #list -- list stanzas
    parser = subparsers.add_parser('list', help="list stanzas")
    parser.set_defaults(func=list)


    args = main_parser.parse_args()

    make_logging(args.__dict__)
    logging.info(args)

    if not args.func:  # there are no subfunctions
        main_parser.print_help()
        exit(1)
    args.func(args.__dict__)
