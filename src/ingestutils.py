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
import json
import bson
import tabulate
#import pymongo
import access_api
import io
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import time 
##################################
#   Utilitie
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
    "publish test  data defined in verify.py to a topic"
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
        print ("******* Published one message")
        publisher.publish(message, header)
    publisher.publish(end_message[0], end_message[1])
    print ("******* Published end message")


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
    breakpoint()
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

def key_tree(d, indent = 0):
    t = type(d)
    if t != type({}) :
        s_all = d.__str__()
        if len(s_all) > 80 : s_all = s_all[1:27] + "..."
        print (">"*indent*3, indent, f"{s_all}")
        return
    for key in d.keys():
        print (">"*indent*3, indent,  key)
        key_tree(d[key], indent=indent+1)
        
def dog(filename):
    import avro.schema
    from avro.datafile import DataFileReader, DataFileWriter
    from avro.io import DatumReader, DatumWriter
    import io


def amon(args):
    """
    one off for icecube

    list dates of events in archive
    """
    sql = f"""
       SELECT
          timestamp, uuid
       FROM
          messages
       WHERE
          topic = 'amon.nuem'
       ORDER BY
         timestamp
       """
    config = utility_api.merge_config(args)
    db = database_api.DbFactory(config).get_db()
    db.connect()
    logging.info(sql)
    for (timestamp, uuid)  in  db.query(sql):
        time_published = timestamp/1000
        dt = datetime.datetime.fromtimestamp(time_published)
        isodate =  dt.isoformat()
        print (isodate, uuid)
    
       
def lvk(args):
    """ one-off for LVK """
    week_in_msec = 60*60*24*7*1000
    now_in_msec  =  time.time() * 1000
    last_week = now_in_msec - week_in_msec
    sql = f"""
       SELECT
          key
       FROM
          messages
       WHERE
          topic = 'igwn.gwalert'
       AND
          timestamp > {last_week}
       """
    print(args)
    config = utility_api.merge_config(args)
    accessor = access_api.Archive_access(config)
    db = database_api.DbFactory(config).get_db()
    db.connect()
    logging.info(sql)
    keys = db.query(sql)
    results = []
    for key in keys:
        (message, metadata) = accessor.get_all(key[0])
        f = io.BytesIO(message)
        reader = DataFileReader(f, DatumReader())
        for d in reader:
            #key_tree(d, indent = 3)
            alert_type = d.get('alert_type', None)
            time_created = d.get('time_created', None)
            created_timestamp = datetime.datetime.strptime(time_created, "%Y-%m-%dT%H:%M:%S%z").timestamp()
            published_timestamp = metadata["timestamp"]/1000
            latency = published_timestamp  -created_timestamp
            urls = d.get('urls', None)
            superevent_id = d.get('superevent_id', None)
            if "MS" in superevent_id : continue 
            results.append([alert_type, time_created, latency, superevent_id, urls])
            print (alert_type, time_created, latency, superevent_id, urls)
            continue
    headers = ["alert type", "time_created", "latency to publish(sec)", "superevent", "url"]
    table = tabulate.tabulate(
        results,
        headers=headers,
        tablefmt="github")
    print(table)

    
def inspect(args):
    """inspect an object given a uuid"""
    config = utility_api.merge_config(args)
    accessor = access_api.Archive_access(config)
    uuid = args["uuid"]
    sql = f"select key from messages where uuid = '{uuid}'"
    logging.info (f"{sql}")
    key = accessor.query(sql)[0][0]
    bundle = accessor.get_raw_object(key)
    if args["write"]:
        with open(f"{uuid}.bson", "wb") as f:
            f.write(bundle)
    bundle = bson.loads(bundle)
    if args["burst"]:
        m_format = bundle["message"]["format"]
        m_content = bundle["message"]["content"]
        m_metadata = bundle["metadata"]
        m_annotations = bundle["annotations"]
        if m_format not in ["avro", "json"] : print ("I do not yet know how to dump {m_format}")
        if m_format == "avro" :
            write_binary (f"{uuid}.content.avro", m_content)
            dog(f"{uuid}.content.avro")
        elif m_format == "json" : write_json (f"{uuid}.content.json", m_content)
        else :
            with open(f"{uuid}.content.{m_format}", "wb") as f:  f.write(m_content)
        write_json(f"{uuid}.annotations.json", m_annotations)
    if not args["quiet"]:
        import pprint
        pprint.pprint(bundle)


def  write_binary(filename, data):
     with open(filename, "wb") as f: f.write(data)
def  write_json(filename, data):
    with open(filename, "w") as f : json.dump(data, f)
        

def db_logs(args):
    "print recent db logs"
    db = database_api.DbFactory(args).get_db()
    logs = db.get_logs()
    print(logs)


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
    args["use_test_topic"] = True  #signal consumer to read test topic
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


def mongo_schema(args):
    """
    print an outline form for the dictionary structure for
    a document in the mongo database.

    specify ither a topic or a uuid
    for topics, the most recent event is chosen
    """
    import mongo_api
    topic = args["topic"]
    uuid  = args["uuid"] 
    if not uuid and not topic:
        logging.fatal("specify uuid or topic")
        exit(1)
    if uuid and  topic:
        logging.fatal("specify either uuid or topic")
        exit(1)
    if topic: filter = {"SciMMA_topic": topic}
    if uuid : filter = {"SciMMA_uuid":  uuid}
    mdb =  mongo_api.MongoFactory(args).get_mdb()
    mdb.connect()
    item = mdb.collection.find_one(filter, sort=[("SciMMA_topic", pymongo.DESCENDING)])
    
    def print_schema_tree(d, indent = 0):
        t = type(d)
        if t == type([]):
            for item in d : print_schema_tree(item, indent + 1)
            return
        if t != type({}) :
            s_all = d.__str__()
            if len(s_all) > 80 : s_all = s_all[1:27] + "..."
            print (">"*indent*3, indent, f"{s_all} {type(d)}")
            return
        for key in d.keys():
            print (">"*indent*3, indent,  key)
            print_schema_tree(d[key], indent=indent+1)

    print_schema_tree(item)

    
def mongo_status(args):
    "print a bried summary of the DB"
    import mongo_api
    mdb =  mongo_api.MongoFactory(args).get_mdb()
    mdb.connect()
    print(mdb.collection.distinct("SciMMA_topic"))
    print(mdb.collection.distinct("format"))
    data = []
    for topic in mdb.collection.distinct("SciMMA_topic"):
        filter = {"SciMMA_topic" : topic}
        item  = mdb.collection.find_one(filter, sort=[("SciMMA_topic", pymongo.DESCENDING)])
        count = "coming soon"
        count = len( [i for i in mdb.collection.find(filter)] )
        data.append( [item["SciMMA_topic"], item["SciMMA_uuid"], item["SciMMA_published_time"], count])
    headers = ["topic", "uuid", "newest", "count"]
    print(
         tabulate.tabulate(data, headers=headers)
         )

    print(); print()
    data = []
    for f in mdb.collection.distinct("format"):
        filter ={"format" : f}
        item  = mdb.collection.find_one(filter, sort=[("SciMMA_topic", pymongo.DESCENDING)])
        count = len( [i for i in mdb.collection.find(filter)] )
        data.append( [item["format"], item["SciMMA_uuid"], item["SciMMA_published_time"], count])
    headers = ["format", "uuid", "newest", "count"]
    print(tabulate.tabulate(data, headers=headers))


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
    parser.add_argument("-D", "--database_stanza", help="database-config-stanza", default="aws-prod-db")
    parser.add_argument("-S", "--store_stanza", help="storage config stanza", default="S3-dev")
    parser.add_argument("-w", "--write", help="write object to <uuid>.bson ", default=False, action="store_true")
    parser.add_argument("-b", "--burst", help="burst object into message, metadata, and annotation files  ", default=False, action="store_true")
    parser.add_argument("-q", "--quiet", help="dont print object to stdout ", default=False, action="store_true")
    parser.add_argument("uuid",  help="uuid of object")

    # one-off LVK this week.
    parser = subparsers.add_parser('lvk', help=lvk.__doc__)
    parser.set_defaults(func=lvk)
    parser.add_argument("-D", "--database_stanza",
                        help="database-config-stanza", default="aws-prod-db")
    parser.add_argument("-S", "--store_stanza", help="storage config stanza", default="S3-prod")

    # one-off amon this week.
    parser = subparsers.add_parser('amon', help=amon.__doc__)
    parser.set_defaults(func=amon)
    parser.add_argument("-D", "--database_stanza",
                        help="database-config-stanza", default="aws-prod-db")
    parser.add_argument("-S", "--store_stanza", help="storage config stanza", default="S3-prod")

    # db_logs get recent database logs
    parser = subparsers.add_parser('db_logs', help=db_logs.__doc__)
    parser.set_defaults(func=db_logs)
    parser.add_argument("-D", "--database_stanza", help="database-config-stanza", default="aws-dev-db")

    # mongo print schema 
    parser = subparsers.add_parser('mongo_schema', help=mongo_schema.__doc__)
    parser.set_defaults(func=mongo_schema)
    parser.add_argument("-t", "--topic", help="topic", default=None)
    parser.add_argument("-u", "--uuid", help="uuid", default=None)
    parser.add_argument("-M", "--mongo_stanza", help="mongo-config-stanza", default="mongo-demo")

    # mongo status
    parser = subparsers.add_parser('mongo_status', help=mongo_status.__doc__)
    parser.set_defaults(func=mongo_status)
    parser.add_argument("-M", "--mongo_stanza", help="mongo-config-stanza", default="mongo-demo")


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
