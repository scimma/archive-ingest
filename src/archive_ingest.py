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
import mongo_api
import json
from typing import Union

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
def archive_ingest(args):
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
    mdb =  mongo_api.MongoFactory(args).get_mdb()
    db.connect()
    mdb.connect()
    db.make_schema()
    mdb.make_schema()
    consumer.connect()
    store.connect()
    max_messages = consumer.test_topic_max_messages
    num_topics = len(consumer.test_topic.split(','))
    msg_count = {}
    for payload, metadata, annotations in consumer.get_next():
        if max_messages > 0:
            msg_count_total = 0
            for topic in msg_count:
                msg_count_total += msg_count[topic]
            logging.debug(f'''Total message count: {msg_count_total}''')
            if msg_count_total > num_topics * max_messages:
                break
        try:
            if msg_count[metadata['topic']] > max_messages:
                continue
            else:
                msg_count[metadata['topic']] += 1
        except:
            msg_count[metadata['topic']] = 0
        ## If max_messages is set to zero, there is no maximum
        logging.info(metadata)
        logging.info(annotations)
        # logging.info(payload)
        #if decision_api.is_deemed_duplicate(annotations, metadata, db, store):
        #    logging.info(f"Duplicate not logged {annotations}")
        #    consumer.mark_done()
        #    continue
        if args["test_topic"]:
            if payload["content"] == b"end": exit(0)
            if args["verify"] and verify_api.is_known_test_data(metadata):
                verify_api.compare_known_data(payload, metadata)
        storeinfo = store.store(payload, metadata, annotations)
        logging.info(storeinfo)
        mdb.insert(payload, metadata, annotations)
        db.insert(payload, metadata, annotations)
        verify_api.assert_ok(args, payload, metadata, annotations,  db, store)
        ## TODO: Do we want to parse message data for known schemas prior to marking the ingest done?
        consumer.mark_done()
        ## Parse message data for known schemas, where for now the Hopskotch topic is a proxy for a
        ## message schema. 
        ## TODO: This should instead be declared in the metadata via a "schema" field in the headers.
        if metadata['topic'] == 'gcn.noticexxx':
            try:
                ## Attempt to parse the message 
                gcn_notice_data = parse_gcn_notice(payload)
                logging.debug(f'''Extracted GCN notice data\n{json.dumps(gcn_notice_data, indent=2)}''')
            except Exception as e:
                logging.warning(f'''Invalid GCN notice. Error: {e}''')
                continue
            try:
                db.insert_message_data(data=gcn_notice_data, annotations=annotations)
            except Exception as e:
                logging.error(f'''Failed to store GCN notice message data. Error: {e}''')

    consumer.close()

def list(args):
    "list the stanzas so I dont have to grep toml files"
    import pprint
    dict = toml.load(args["toml_file"])
    print (args["toml_file"])
    pprint.pprint(dict)


def get_json_property_by_path(dict_object: dict = {}, path: list = []) -> Union[float, int, str, list, bool]:
    assert isinstance(dict_object, dict)
    element = None
    value = dict_object
    for element in path:
        value = value[element]
    assert not isinstance(value, dict)
    return value


def parse_gcn_notice(payload: str = '') -> dict:
    '''Parse the GCN notice message from the Hopskotch topic'''
    assert payload['format'].lower() == 'json'
    message_content = payload['content'].decode('utf-8')
    gcn_notice = json.loads(message_content)
    ## Define paths to the desired data within the JSON object
    AstroCoords_path = ['WhereWhen', 'ObsDataLocation', 'ObservationLocation', 'AstroCoords']
    ISOTime_path = AstroCoords_path + ['Time', 'TimeInstant', 'ISOTime']
    RA_DEC_path = AstroCoords_path + ['Position2D', 'Value2']
    RA_val_path = RA_DEC_path + ['C1']
    DEC_val_path = RA_DEC_path + ['C2']

    ISOTime = get_json_property_by_path(dict_object=gcn_notice, path=ISOTime_path)
    RA_val = get_json_property_by_path(dict_object=gcn_notice, path=RA_val_path)
    DEC_val = get_json_property_by_path(dict_object=gcn_notice, path=DEC_val_path)

    '''
    ref: https://voevent.readthedocs.io/en/latest/reading.html#ivorns-and-identifiers
    ... IVORNs, or "IVOA Resource Names" ... provide unique 
    identifiers for entities known to the virtual observatory, where "entity" is 
    quite broadly defined. 
    '''
    ivorn = get_json_property_by_path(dict_object=gcn_notice, path=['ivorn'])
    data = {
        "ivorn": ivorn,
        "ra": RA_val,
        "dec": DEC_val,
        "time": ISOTime,
    }
    return data

if __name__ == "__main__":

    #main_parser = argparse.ArgumentParser(add_help=False)
    main_parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    main_parser.set_defaults(func=None)
    main_parser.add_argument("-t", "--toml_file", help = "toml configuration file", default="archive_ingest.toml")
    main_parser.add_argument("-l", "--log_stanza", help = "log config stanza", default="log")

    subparsers = main_parser.add_subparsers()
    #run -- perform archive_ingesting with
    parser = subparsers.add_parser('run', help= "archive_ingest w/(defaults) all mocks")
    parser.set_defaults(func=archive_ingest)
    parser.add_argument("-D", "--database_stanza", help = "database-config-stanza", default="mock-db")
    parser.add_argument("-H", "--hop_stanza", help = "hopskotch config  stanza", default="mock-hop")
    parser.add_argument("-S", "--store_stanza", help = "storage config stanza", default="mock-store")
    parser.add_argument("-M", "--mongo_stanza", help = "storage config stanza", default="mongo-demo")
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
