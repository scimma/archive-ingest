#!/usr/bin/env python3
'''
Utilities for archive_ingest module.

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
import utility_api
import decision_api
import consumer_api
import datetime
import boto3
##################################
#   utilities
##################################

def _delete_by_id(id, db, store=None):
    sql_get_S3_info = """
    SELECT
        bucket, key
    FROM
        messages
    WHERE
        id = {}
    """
    sql_delete = """
    DELETE FROM
        messages
    WHERE
        id = {}
    """
    if store:
        bucket, key = db.query(sql_get_S3_info.format(id))[0]
        store.deep_delete(bucket, key)
    db.query(sql_delete.format(id), expect_results=False)


##################################
#   environment
##################################

def make_logging(args):
    """
    establish python logging based on toms file stanza passed specifies on command line.
    """

    # supply defaults to assure than logging of some sort is setup no matter what.
    # Note that the production environment captures stdout/stderr into logs for us.
    default_format = '%(asctime)s:%(filename)s:%(levelname)s:%(message)s'
    toml_data = toml.load(args["toml_file"])
    config = toml_data[args["log_stanza"]]
    level = config.get("level", "DEBUG")
    format = config.get("format", default_format)
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
    
    message_dict, end_message = verify_api.get_known_data()
    for key in message_dict.keys():
        message, header = message_dict[key]
        logging.info(f"about to publish {key}")
        logging.info(f"{utility_api.terse(message)}, {utility_api.terse(header)}")
        if args["ask"]:
            print("> p:pdb; q:quit_ask_mode; s:skip this messagee, anything_else: continue")
            sys.stdout.write(">> ")
            answer = sys.stdin.readline()
            if answer[0].lower() == 's':
                continue
            elif answer[0].lower() == 'q':
                args["ask"] = False
            elif answer[0].lower() == 'p':
                import pdb; pdb.set_trace()
            else:
                pass
        publisher.publish(message, header)
    publisher.publish(end_message[0], end_message[1])


def list(args):
    "list the stanzas so I dont have to grep toml files"
    import pprint
    dict = toml.load(args["toml_file"])
    print(args["toml_file"])
    pprint.pprint(dict)


def verify_db_to_store(args):
    """
    Check that the DB has corresponding store objects.

    DB records  are anomolus if the S3 store does not
    agree with the DB record. Disagreement can be
    due to the top level verison of the object is
    absent or the size of the top level object is not
    the size recorded in DB record.

    Scan the topics passed in the SQL LIKE expression
    in --topics defaujlt all topics

    --sample causes the tool sample 100 DB
    records (def do ot sample)
    
    Exit with the number of defective objects
    """
    db = database_api.DbFactory(args).get_db()
    store = store_api.StoreFactory(args).get_store()
    db.connect()
    store.connect()
    
    topic = args["topic"]
    
    if args["sample"]:
        limit_clause = "ORDER BY random() Limit 100"
    else:
        limit_clause = ""
                
    sql = f"select key, size from messages WHERE topic LIKE  '{topic}' {limit_clause};"
    logging.info(sql)
    results = db.query(sql)
    anomolous_objects = []
    for key, size in results:
        summary = store.get_object_summary(key)
        if not summary["exists"]:
            print(f"{key} does not exist ***")
            anomolous_objects.append(key)
        elif size != summary["size"]:
            print(f"{key} size mismatch, {key}")
            anomolous_objects.append(key)
        else:
            if args["verbose"] :
                print(f"{key} is ok")
    print(f"anomolous entries {len(anomolous_objects)} detected")
    exit (len(anomolous_objects))

def verify_store_to_db(args):
    """
    Check that the store  has corresponding db objects.

    Store objets are anomolus if the S3 store does not
    agree with the DB record. Disagreements can be
    due to
    - the top-level verison of the object has no DB record
    - the size of the top-level object is not the ..
      size recorded in DB record
    - more than one version of the object exists.

    Scan the topics passed in the SQL LIKE expression
    in --topics default:all topics.

    Scan for the year/month indicated by -y  

    --sample causes the tool to sample 100 DB
    records (def do not sample)
    
    Exit with the number of defective S3 objects. 
    """
    db = database_api.DbFactory(args).get_db()
    store = store_api.StoreFactory(args).get_store()
    db.connect()
    store.connect()
    session = boto3.session.Session()
    s3_client = session.client('s3')
    
    like_clause = f""" where topic LIKE  '{args["topic"]}'"""

    # get all the topics.
    results = db.query(f"SELECT distinct(topic) FROM messages {like_clause};")
    topics = [r[0] for r in results]
    print("****", topics)
    # check objects from the month in question 
    for topic in topics:
       prefix = f"""{topic}/{args["year_month"]}"""
       logging.info(f"checking for objects under {prefix}")
       for result in store.list_object_versions(prefix):
           print(result)


def status(args):
    """
     First cut at a status

     - What topics have been colllected
     - what's "recent"
     - what topics are being archived
     - is the service up
    """
    import tabulate
    db = database_api.DbFactory(args).get_db()
    db.connect()
    import time
    hour_msec = 60*60*1000
    day_msec = 24 * hour_msec
    now_timestamp = int(time.time()*1000)
    last_day_timestamp = now_timestamp - day_msec
    last_hour_timestamp = now_timestamp - hour_msec

    def summarize(since_msec, label):
        headers = ["Topic", "n messages", "bytes stored", "sample uuid", "earliest (UTC)", "latest(UTC)"]
        sql = f"""
        SELECT
          topic,
          TO_CHAR(count(topic), 'fm999G999G999G999G999'),
          TO_CHAR(sum(size),    'fm999G999G999G999G999'),
          max(uuid),
          to_timestamp(min(timestamp/1000)),
          to_timestamp(max(timestamp/1000))
        FROM
          messages
        WHERE
          timestamp > {since_msec}
        GROUP by topic order by topic
        """
        results = db.query(sql)

        # get totals
        sql = f"""
         SELECT
            count(distinct(topic)),
            count(*),
            TO_CHAR(sum(size), 'fm999G999G999G999'),
            'total'
        FROM
            messages
        WHERE
           timestamp > {since_msec}"""
        totals = db.query(sql)[0]
        results.append(['_______', '_______', '_________', '_________'])
        results.append(totals)
        table = tabulate.tabulate(
            results,
            headers=headers,
            tablefmt="github")
        print(label)
        print(table)
        print()
    summarize(0, "since start of archiving")
    summarize(last_day_timestamp, "last 24 hours")
    summarize(last_hour_timestamp, "last hour")

    latest = db.query("SELECT  MAX(timestamp) FROM messages")[0][0]
    import datetime
    lastest_as_string = datetime.datetime.fromtimestamp(latest/1000.).isoformat()
    print(f"latest(utc:  {lastest_as_string}) ingested::")
    sql = f"select * from messages where timestamp = {latest}"
    result = db.query(sql)
    print(result)


def inspect(args):
    """inspect an object given a uuid"""
    import bson
    import access_api
    accessor = access_api.Archive_access(args)
    uuid = args["uuid"]
    sql = f"select key from messages where uuid = '{uuid}'"
    key = accessor.query(sql)[0][0]
    bundle = accessor.get_raw_object(key)
    if args["write"]:
        with open(f"{uuid}.bson", "wb") as f:
            f.write(bundle)
    bundle = bson.loads(bundle)
    if not args["quiet"]:
        import pprint
        pprint.pprint(bundle)


def uuids(args):
    "print a list of uuids consistent w user supplied where clause"
    db = database_api.DbFactory(args).get_db()
    db.connect()
    sql = f'select uuid from messages {args["where"]}'
    logging.info(sql)
    uuids = db.query(sql)
    for uuid in uuids:
        print(uuid[0])


def db_logs(args):
    "print recent db logs"
    db = database_api.DbFactory(args).get_db()
    logs = db.get_logs()
    print(logs)


def kube_logs(args):
    "print kube logs for devel or prod"
    import os
    cmd = "kubectl logs `kubectl get pods | grep archive_ingest | grep %s | awk '{print $1}'`" % args["kind"]
    os.system(cmd)


def clean_tests(args):
    """
    clean test messages from archive and hop

    purge the databse and store.
    read the test topic in HOP dry
    """
    test_group = 'cmb-s4-fabric-tests.housekeeping-test'
    db = database_api.DbFactory(args).get_db()
    db.connect()
    store = store_api.StoreFactory(args).get_store()
    store.connect()
    args["test_topic"] = True  #signal consumer to read test topic
    consumer = consumer_api.ConsumerFactory(args).get_consumer()
    consumer.until_eos  = True  #stop when toic is dry 
    consumer.connect()

    def delete_from_store(key):
        logging.info(f"about to delete {key}")
        store.deep_delete_object_from_store(key)
        logging.info(f"delete finished {key}")

    def delete_from_db(id): 
        sql = f"DELETE FROM messages WHERE id = {id}"
        logging.info(f"about to execute {sql}")
        db.query(sql, expect_results=False)
        logging.info(f"sql finished {sql}")

    sql = f"SELECT id, key FROM messages WHERE topic = '{test_group}';"
    for id, key in db.query(sql):
        delete_from_store(key)
        delete_from_db(id)

    sql = f"SELECT id, key FROM messages WHERE topic = 'mock.topic';"  
    for id, key in db.query(sql):
        delete_from_store(key)
        delete_from_db(id)

    sql = f"SELECT id, key FROM messages WHERE topic = 'sys.archive-ingest-test';"  
    for id, key in db.query(sql):
        delete_from_store(key)
        delete_from_db(id)

        
    logging.info(f"off to drain hop topic 'sys.archive-ingest-test'") 
    for payload, metadata, archiver_notes  in consumer.get_next():
        consumer.mark_done()
        logging.info(f"message_drained {metadata}")
    consumer.close()

def clean_duplicates(args):
    """
    detect, clean duplicates from the Db and store
    """

    db = database_api.DbFactory(args).get_db()
    db.connect()
    store = store_api.StoreFactory(args).get_store()
    store.connect()

    results = decision_api.get_client_uuid_duplicates(args, db)
    print ("uuid test:")
    for r  in results:
        print(r)
        id  = r[0]
        _delete_by_id(id, db)

    results = decision_api.get_server_uuid_duplicates(args, db)
    print ("server side test:")
    for r in results:
        print(r)
        utility_api.ask(args, key='quiet')
        id  = r[0]
        _delete_by_id(id, db, store=store)



if __name__ == "__main__":

    # main_parser = argparse.ArgumentParser(add_help=False)
    main_parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    main_parser.set_defaults(func=None)
    main_parser.add_argument("-t", "--toml_file", help="toml configuration file", default="archive_ingest.toml")
    main_parser.add_argument("-l", "--log_stanza", help="log config stanza", default="log")

    subparsers = main_parser.add_subparsers()

    # list -- list stanzas
    parser = subparsers.add_parser('list', help="list stanzas")
    parser.set_defaults(func=list)

    # connect --- launch a query_session tool against AWS
    parser = subparsers.add_parser('connect', help="Launch a query session shell against AWS databases")
    parser.set_defaults(func=connect)
    parser.add_argument("-D", "--database_stanza", help="database-config-stanza", default="aws-dev-db")

    # show  see what's happening
    parser = subparsers.add_parser('status', help="get a sense of what's happpening")
    parser.set_defaults(func=status)
    parser.add_argument("-D", "--database_stanza", help="database-config-stanza", default="aws-dev-db")

    # publish -- publish some test data
    parser = subparsers.add_parser('publish', help="publish some test data")
    parser.set_defaults(func=publish)
    parser.add_argument("-H", "--hop_stanza", help="hopskotch config stanza", default="hop-prod")
    parser.add_argument("-D", "--database_stanza", help="database-config-stanza", default="aws-dev-db")
    parser.add_argument("-a", "--ask", help="interactive prompt before each publish", default=False, action="store_true")
    parser.add_argument("-c", "--clean", help="purge old test data from archive before publish", default=False, action="store_true")

    # verify_db_to_store --
    parser = subparsers.add_parser('verify_db_to_store', help=verify_db_to_store.__doc__)
    parser.set_defaults(func=verify_db_to_store)
    parser.add_argument("-D", "--database_stanza", help="database-config-stanza", default="aws-dev-db")
    parser.add_argument("-S", "--store_stanza", help="storage config stanza", default="S3-dev")
    parser.add_argument("-t", "--topic", help="do topics LIKE this (SQL wildcard))", default="%")
    parser.add_argument("-s", "--sample", help="sample up to 100 random objects", default=False, action="store_true")
    parser.add_argument("-v", "--verbose", help="print more", default=False, action="store_true")

    # verify_store_to_db --
    parser = subparsers.add_parser('verify_store_to_db', help=verify_store_to_db.__doc__)
    parser.set_defaults(func=verify_store_to_db)
    current = datetime.datetime.utcnow().strftime("%Y/%m")
    parser.add_argument("-D", "--database_stanza", help="database-config-stanza", default="aws-dev-db")
    parser.add_argument("-S", "--store_stanza", help="storage config stanza", default="S3-dev")
    parser.add_argument("-t", "--topic", help="do topics LIKE this (SQL wildcard, def '%')", default="%")
    parser.add_argument("-y", "--year_month", help="constrian to this year/month (def: current year/month)",
                        default=f"{current}")
    parser.add_argument("-s", "--sample", help="sample up to 100 random objects", default=False, action="store_true")
    parser.add_argument("-v", "--verbose", help="print more", default=False, action="store_true")

    # inspect display an object and or write it out.
    parser = subparsers.add_parser('inspect', help=inspect.__doc__)
    parser.set_defaults(func=inspect)
    parser.add_argument("-D", "--database_stanza", help="database-config-stanza", default="aws-dev-db")
    parser.add_argument("-S", "--store_stanza", help="storage config stanza", default="S3-dev")
    parser.add_argument("-w", "--write", help="write object to <uuid>.bson ", default=False, action="store_true")
    parser.add_argument("-q", "--quiet", help="dont print object to stdout ", default=False, action="store_true")
    parser.add_argument("uuid",  help="uuid of object")

    # uuids get uuids consistent with a where clause.
    parser = subparsers.add_parser('uuids', help=uuids.__doc__)
    parser.set_defaults(func=uuids)
    parser.add_argument("-D", "--database_stanza", help="database-config-stanza", default="aws-dev-db")
    parser.add_argument("where",  help="where clause")

    # db_logs get recent database logs.
    parser = subparsers.add_parser('db_logs', help=db_logs.__doc__)
    parser.set_defaults(func=db_logs)
    parser.add_argument("-D", "--database_stanza", help="database-config-stanza", default="aws-dev-db")

    # kube_logs get recent kube logs and kube status
    parser = subparsers.add_parser('kube_logs', help=kube_logs.__doc__)
    parser.set_defaults(func=kube_logs)
    parser.add_argument("kind", help="filter for e.g 'prod' or 'devel'")

    # clean test data
    parser = subparsers.add_parser('clean_tests', help=clean_tests.__doc__)
    parser.set_defaults(func=clean_tests)
    parser.add_argument("-H", "--hop_stanza", help = "hopskotch config  stanza", default="hop-prod")
    parser.add_argument("-D", "--database_stanza", help="database-config-stanza", default="aws-dev-db")
    parser.add_argument("-S", "--store_stanza", help="storage config stanza", default="S3-dev")
    parser.add_argument("-q", "--quiet", help="don't ask before delete ", default=False, action="store_true")

    # clean_duplicates
    parser = subparsers.add_parser('clean_duplicates', help=clean_duplicates.__doc__)
    parser.set_defaults(func=clean_duplicates)
    parser.add_argument("-D", "--database_stanza", help="database-config-stanza", default="aws-dev-db")
    parser.add_argument("-S", "--store_stanza", help="storage config stanza", default="S3-dev")
    parser.add_argument("-l", "--limit", help="only consider limit number of matches (def 20)", type=int, default=20)
    parser.add_argument("-q", "--quiet", help="don't ask before delete ", default=False, action="store_true")
    args = main_parser.parse_args()
    make_logging(args.__dict__)
    logging.info(args)

    if not args.func:  # there are no subfunctions
        main_parser.print_help()
        exit(1)
    args.func(args.__dict__)
