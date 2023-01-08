#!/usr/bin/env python3

'''
Utilities for housekeeping applications.

@author: Mahmoud Parvizi (parvizim@msu.edu)
@author: Don Petravick (petravick@illinois.edu)

'''

import toml
import logging
import argparse
import publisher_api
import store_api
import database_api
import verify_api
import pprint

from hop.io import Stream, StartPosition, list_topics
import hop

##################################
#   utilities
##################################

def terse(object):
    text = object.__repr__()
    max_length = 30
    if len(text) > max_length:
        return text[:max_length-3] + '...'
    return text

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
#  argparse implementation functions 
#  
###############


def connect(args):
    "get connect info and launch a query_session engine"
    database_api.DbFactory(args).get_db().launch_db_session()

def publish(args):
    "publish some test data to a topic"
    import sys
    publisher = publisher_api.PublisherFactory(args).get_publisher()
    publisher.connect()
    message_dict, end_message  = verify_api.get_known_data()
    for key in message_dict.keys():
        message, header = message_dict[key]
        logging.info(f"about to publish {key}")
        logging.info(f"{terse(message)}, {terse(header)}")
        if args.ask:
            print (f"> p:pdb; q:quit_ask_mode; s:skip this messagee, anything_else: continue")
            sys.stdout.write(">> ")
            answer = sys.stdin.readline()
            if answer[0].lower() == 's' :
                continue
            elif answer[0].lower() == 'n' :
                self.ask = False
            elif   answer[0].lower() == 'p':
                import pdb ; pdb.set_trace()
            else:
                pass
        
        publisher.publish (message, header)
    publisher.publish (end_message[0], end_message[1])
    
def list(args):
    "list the stanzas so I dont have to grep toml files"
    import pprint
    dict = toml.load(args.toml_file)
    print (args.toml_file)
    pprint.pprint(dict)


def verify(args):
   db     = database_api.DbFactory(args).get_db()
   store  = store_api.StoreFactory(args).get_store()
   db.connect()
   store.connect()
   if args.all:
       limit_clause = ""
   else:
       limit_clause = "ORDER BY random() LIMIT 100"
   sql = f"select key, size from messages {limit_clause};"
   logging.info (sql)
   results = db.query(sql)
   for key, size  in results:
       summary = store.get_object_summary(key)
       if not summary["exists"] :
           print (f"** object does not exist ***, {key}")
       elif size != summary["size"]:
            print (f"** size mismatch, {key}")
       else:
           print (f"** key is ok, {key}")

def status(args):
    """
     First cut at a status

     - What topics have been colllected
     ? what's "recent"
     ? what topics are being archived?
     ? is the service up?
    """
    import tabulate 
    db     = database_api.DbFactory(args).get_db()
    db.connect()
    import time
    hour_msec = 60*60*1000
    day_msec  = 24 * hour_msec
    now_timestamp = int(time.time()*1000)
    last_day_timestamp  = now_timestamp - day_msec
    last_hour_timestamp = now_timestamp - hour_msec
    def summarize(since_msec, label):
        headers = ["Topic", "n messages", "bytes stored", "sample uuid", "earliest (UTC)", "latest(UTC)"]
        sql = f"""
        SELECT
          topic, count(topic),sum(size), max(uuid),
             date_trunc('seconds', to_timestamp(min(timestamp/1000))),
             date_trunc('seconds', to_timestamp(max(timestamp/1000)))
        FROM
          messages
        WHERE
          timestamp > {since_msec}
        GROUP by topic order by topic
        """
        results = db.query(sql)
        table = tabulate.tabulate(
            results,
            headers=headers,
            tablefmt="github")
        print(label)
        print(table)
        print()
    summarize(0, "since start of archiving")
    summarize(last_day_timestamp,  "last 24 hours")
    summarize(last_hour_timestamp, "last hour")

    latest =  db.query("SELECT  MAX(timestamp) FROM messages")[0][0]
    import datetime
    lastest_as_string = datetime.datetime.fromtimestamp(latest/1000.).isoformat()
    print (f"latest(utc:  {lastest_as_string}) ingested::")
    sql = f"select * from messages where timestamp = {latest}"
    result = db.query(sql)
    print (result)


def inspect(args):
    """inspect an object given a uuid"""
    import bson
    db     = database_api.DbFactory(args).get_db()
    store  = store_api.StoreFactory(args).get_store()
    db.connect()
    store.connect()
    uuid = args.uuid
    sql = f"select key from messages where uuid = '{uuid}'"
    key = db.query(sql)[0][0]
    bundle  = store.get_object(key)
    if args.write :
        with open(f"{uuid}.bson","wb") as f:
            f.write(bundle)
    bundle  = bson.loads(bundle)
    if not args.quiet:
        import pprint 
        pprint.pprint(bundle)

def uuids(args):
    "print a list of uuids consistent w user supplied where clause"
    db     = database_api.DbFactory(args).get_db()
    db.connect()
    sql = f"select uuid from messages {args.where}"
    logging.info(sql)
    
    uuids = db.query(sql)
    for uuid  in uuids:
        print (uuid[0])

        
if __name__ == "__main__":

    #main_parser = argparse.ArgumentParser(add_help=False)
    main_parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    main_parser.set_defaults(func=None)
    main_parser.add_argument("-t", "--toml_file", help = "toml configuration file", default="housekeeping.toml")
    main_parser.add_argument("-l", "--log_stanza", help = "log config stanza", default="log")

    subparsers = main_parser.add_subparsers()
    
    #list -- list stanzas 
    parser = subparsers.add_parser('list', help="list stanzas")
    parser.set_defaults(func=list)

    #connect --- launch a query_session tool against AWS 
    parser = subparsers.add_parser('connect', help="Launch a query session shell against AWS databases")
    parser.set_defaults(func=connect)
    parser.add_argument("-D", "--database_stanza", help = "database-config-stanza", default="aws-dev-db")

    # show  see what's happening  
    parser = subparsers.add_parser('status', help="get a sense of what's happpening")
    parser.set_defaults(func=status)
    parser.add_argument("-D", "--database_stanza", help = "database-config-stanza", default="aws-dev-db")

    #publish -- publish some test data
    parser = subparsers.add_parser('publish', help="publish some test data")
    parser.set_defaults(func=publish)
    parser.add_argument("-H", "--hop_stanza", help = "hopskotch config  stanza", default="hop-prod")
    parser.add_argument("-a", "--ask", help = "interactive prompt before each publish", default=False, action="store_true")

    #verify -- 
    parser = subparsers.add_parser('verify', help="check objects recored in DB exist")
    parser.set_defaults(func=verify)
    parser.add_argument("-D", "--database_stanza", help = "database-config-stanza", default="mock-db")
    parser.add_argument("-S", "--store_stanza", help = "storage config stanza", default="mock-store")
    parser.add_argument("-a", "--all", help = "do the whole archive (gulp)", default = False, action = "store_true" )

    #inspect display an object and or write it out.
    parser = subparsers.add_parser('inspect', help=inspect.__doc__)
    parser.set_defaults(func=inspect)
    parser.add_argument("-D", "--database_stanza", help = "database-config-stanza", default="mock-db")
    parser.add_argument("-S", "--store_stanza", help = "storage config stanza", default="mock-store")
    parser.add_argument("-w", "--write", help = "write object to <uuid>.bson ", default=False, action="store_true")
    parser.add_argument("-q", "--quiet", help = "dont print object to stdout ", default=False, action="store_true")
    parser.add_argument("uuid",  help = "uuid of object")

    #uuids get uuids consistent with a where clause.
    parser = subparsers.add_parser('uuids', help=uuids.__doc__)
    parser.set_defaults(func=uuids)
    parser.add_argument("-D", "--database_stanza", help = "database-config-stanza", default="mock-db")
    parser.add_argument("where",  help = "where clause")
    
    args = main_parser.parse_args()
    make_logging(args)
    logging.info(args)
    
    if not args.func:  # there are no subfunctions
        main_parser.print_help()
        exit(1)
    args.func(args)

