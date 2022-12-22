#!/usr/bin/env python3

'''Log messages and metadata to a database.

In production, log events from selected Hopscotch public
topics into the AWS-resident production housekeeping database.

Events may be also logged to the AWS-resident development
database, to a local sqilite database, or a "mock" database
that just discards them.

Events can be sourced from hopskotch, or from a "mock" source.  The
development direction for the mock source is to allow for stressing
the database engines and database provisioning to ensure they form a
robust store that is critical to SCiMMA operations.

@author: Mahmoud Parvizi (parvizim@msu.edu)
@author: Don Petravick (petravick@illinois.edu)

'''
import toml
import logging
import argparse
import source_api
import store_api
import database_api

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
        storeinfo = store.store(payload, metadata)
        db.insert(payload, metadata, storeinfo)

def query_session(args):
    "get connect info and launch a query_session engine"
    DbFactory(args).get_db().launch_db_session()

def publish(args):
    "publish some test data to a topic"
    SourceFactory(args).get_source().publish(args.topic)

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
    parser.add_argument("-t", "--topic", help = "consume only this topic for consumption", default=None)

    #list -- list stanzas 
    parser = subparsers.add_parser('list', help="list stanzas")
    parser.set_defaults(func=list)

    #query_session --- launch a query_session tool against AWS 
    parser = subparsers.add_parser('query_session', help="Launch a query session shell against AWS databases")
    parser.set_defaults(func=query_session)
    parser.add_argument("-D", "--database_stanza", help = "database-config-stanza", default="aws-dev-db")

    #publish -- publish some test data
    parser = subparsers.add_parser('publish', help="publish some test data")
    parser.set_defaults(func=publish)
    parser.add_argument("-t", "--topic", help = "consume only this topic for consumption", default=None)
    parser.add_argument("-H", "--hop_stanza", help = "hopskotch config  stanza", default="mock-hop")
    
    args = main_parser.parse_args()
    make_logging(args)
    logging.info(args)
    
    if not args.func:  # there are no subfunctions
        main_parser.print_help()
        exit(1)
    args.func(args)

