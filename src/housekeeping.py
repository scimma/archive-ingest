#!/usr/bin/env python3

'''Log messages and metadata to S3. provide metadate to a databse.

In production, log events from selected Hopscotch public
topics into S3 and log into a housekeeping postgres database.

to suport devleopment, Houskeeping.py  supports realized or mock
hopskotch, database and store elements.  These are defined in
toml files. Housekeeping.toml defines a variety of these sources. 

@author: Mahmoud Parvizi (parvizim@msu.edu)
@author: Don Petravick (petravick@illinois.edu)

'''
import toml
import logging
import argparse
import source_api
import store_api
import database_api
import verify_api

from hop.io import Stream, StartPosition, list_topics
import hop

##################################
#   environment
##################################

def make_logging(args):
    """
    establish python logging based on toms file stanza passed specifies on command line.
    """
    
    # supply defaults to assure than logging of some sort is setup no matter what.
    # Note that the production environment captures stdout/stderr into logs for us.
    default_format  = '%(asctime)s:%(filename)s:%(levelname)s:%(message)s'
    toml_data = toml.load(args.toml_file)
    config    =  toml_data[args.log_stanza]
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
    Acquire data from the specified source and log to specified DB
    """
    db     = database_api.DbFactory(args).get_db()
    source = source_api.SourceFactory(args).get_source()
    store  = store_api.StoreFactory(args).get_store()
    db.connect()
    db.make_schema()
    source.connect()
    store.connect()
    for x in source.get_next():
        payload = x[0]
        metadata = x[1]
        text_uuid = x[2]
        storeinfo = store.store(payload, metadata, text_uuid)
        db.insert(payload, metadata, text_uuid, storeinfo)
        verify_api.assert_ok(args, payload, metadata, text_uuid, storeinfo, db, store)
        if args.test_topic: 
            if payload["content"] == b"end": exit(0)
            verify_api.compare_known_data(payload, metadata)

        
def list(args):
    "list the stanzas so I dont have to grep toml files"
    import pprint
    dict = toml.load(args.toml_file)
    print (args.toml_file)
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
    make_logging(args)
    logging.info(args)
    
    if not args.func:  # there are no subfunctions
        main_parser.print_help()
        exit(1)
    args.func(args)

