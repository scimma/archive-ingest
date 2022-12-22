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
import psycopg2
import psycopg2.extensions
import boto3
import toml
import logging
import argparse
import pdb
import datetime
import uuid
import bson
import time
import json
import source_api

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

##################################
# stores
##################################

class StoreFactory:
    """
    Factory class to create Mock, or S3 stores 
    """
    def __init__ (self, args):
        toml_data = toml.load(args.toml_file)
        stanza = args.store_stanza
        logging.info(f"using dabase stanza {stanza}")
        config    = toml_data.get(stanza, None)
        if not config:
            logging.fatal("could not find {stanza} in {args.toml_file}")

        type = config["type"]
        #instantiate, then return db object of correct type.
        if type == "mock" : self.store  =  Mock_store(args, config) ; return 
        if type == "S3"   : self.store  =  S3_store  (args, config) ; return 
        logging.fatal(f"store {type} not supported")
        exit (1)
        
    def get_store(self):
        return self.store

class Store_info:
    "a namespace for storage data"
    pass

class Base_store:
    def __init__(self, args, config):
        self.bucket = config["bucket"]
        self.n_stored = 0
        self.log_every = 100   #move to config file.
    
    def connect(self):
        pass

    def log(self, storeinfo):
        "log storage informmation, but not too often"
        msg1 = f"stored {self.n_stored} objects."
        msg2 = f"This object: {storeinfo.size} bytes to {storeinfo.bucket} {storeinfo.key}"
        if self.n_stored < 5 :
            logging.info(msg1)
            logging.info(msg2)
        elif self.n_stored % self.log_every == 0:
            logging.info(msg1)
            logging.info(msg2)

    def get_key(self, metadata):
        'compute the "path" to the object' 
        topic = metadata["topic"]
        uuid_ = self.get_uuid(metadata)
        t = time.gmtime(metadata["timestamp"]/1000)
        key = f"{topic}/{t.tm_year}/{t.tm_mon}/{t.tm_mday}/{t.tm_hour}/{uuid.UUID(bytes=uuid_)}.bson"
        return key

    def get_uuid(self, metadata):
        for label, value in metadata["headers"]:
            if label ==  "_id" : return value
        raise Exception ("Missing UUID header in headers")
    
    def get_storeinfo(self, key, size):
        storeinfo = Store_info()
        storeinfo.size = size
        storeinfo.key = key
        storeinfo.bucket = self.bucket
        return storeinfo

    def get_as_bson(self, payload, metadata):
        "return a blob of bson"
        ret = bson.dumps({"message" : payload, "metadata" : metadata})
        return ret
    
class S3_store(Base_store):
    "send things to s3, not a dbms"
    def __init__(self, args, config):
        super().__init__(args, config)
        
    def connect(self):
        "obtain an S3 Client"
        self.client = boto3.client('s3')

    def store(self, payload, metadata):
        """place data, metadata as an object in S3"""
        
        bucket = self.bucket
        key = self.get_key(metadata)
        b = self.get_as_bson(payload, metadata)
        size = len(b)
        self.client.put_object(Body=b, Bucket=bucket, Key=key)        
        self.n_stored += 1
        storeinfo = self.get_storeinfo(key, len(payload))
        self.log(storeinfo)
        return storeinfo

class Mock_store(Base_store):
    """
    a mock store that does nothing -- support debug and devel.
    """
    
    def __init__(self, args, config):
        self.bucket = "scimma-housekeeping"
        super().__init__(args, config)
        logging.info(f"Mock store configured")


    def store(self, payload, metadata):
        "mock operation of storing in s3"    
        self.n_stored += 1
        key = self.get_key(metadata)
        b = self.get_as_bson(payload, metadata)
        size = len(b)
        storeinfo = self.get_storeinfo(key, size)
        self.log(storeinfo)
        return storeinfo



##################################
# "databases"
##################################

    
class DbFactory:
    """
    Factory class to create Mock, MySQL, or AWS postgres DB objects 
    """
    def __init__ (self, args):
        toml_data = toml.load(args.toml_file)
        stanza = args.database_stanza
        logging.info(f"using dabase stanza {stanza}")
        config    = toml_data.get(stanza, None)
        if not config:
            logging.fatal("could not find {stanza} in {args.toml_file}")

        type = config["type"]
        #instantiate, then return db object of correct type.
        if type == "mock"   : self.db  =  Mock_db(args, config)  ; return 
        if type == "aws"    : self.db  =  AWS_db(args, config) ; return 
        logging.fatal(f"database {type} not supported")
        exit (1)
        
    def get_db(self):
        return self.db
    

class Base_db:

    def __init__(self, args, config):
        self.n_inserted = 0
        self.log_every  = 100 # get from config file 
         
    def launch_db_session(self):
        logging.fatal(f"Query_Session tool not supported for this database")
        exit(1)

    def make_schema(self):
        "no schema to make"
        pass 
    
    def connect(self):
        "nothing to connect to"
        pass
    
    def log(self):
        "log db informmation, but not too often"
        msg1 = f"inserted  {self.n_inserted} objects."
        if self.n_inserted < 5 :
            logging.info(msg1)
        elif self.n_inserted % self.log_every == 0:
            logging.info(msg1)
                         
    def get_uuid(self, metadata):
        for label, value in metadata["headers"]:
            if label ==  "_id" : return value
        raise Exception ("Missing UUID header in headers")

    
        
class Mock_db(Base_db):
    """
    a mock DB that does nothing -- support debug and devel.
    """
    def __init__(self, args, config):
        logging.info(f"Mock Database configured")
        super().__init__(args, config)
       

    def insert(self, payload, message, storeinfo):
        self.n_inserted += 1


class AWS_db(Base_db):
    """
    Logging to AWS postgres DBs
    """
    def __init__(self, args, config):
        # get these from the configuration file
        self.aws_db_name        = config["aws_db_name"] 
        self.aws_db_secret_name = config["aws_db_secret_name"]
        self.aws_region_name    = config["aws_region_name"]
        super().__init__(args, config)
        
        # go off and get the real connections  information from AWS
        self.set_password_info()
        self.set_connect_info()
        logging.info(f"aws db name, secret, region: {self.aws_db_name}, {self.aws_db_secret_name}, {self.aws_region_name } ")
        logging.info(f"aws database, user, port, address: {self.DBName}, {self.MasterUserName}, {self.Port} ,{self.Address}")
        
    def set_password_info(self):
        "retrieve postgress password from its AWS secret"
        import boto3
        from botocore.exceptions import ClientError

        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=self.aws_region_name
        )
        get_secret_value_response = client.get_secret_value(
            SecretId=self.aws_db_secret_name
        )
        password = get_secret_value_response['SecretString']
        self.password = password

    def set_connect_info(self):
        "set connection variabable from AWS"
        import boto3
        from botocore.exceptions import ClientError
        session = boto3.session.Session()
        client = session.client(
            service_name='rds',
            region_name=self.aws_region_name
        )
        result = client.describe_db_instances(
            DBInstanceIdentifier=self.aws_db_name)
        result = result['DBInstances'][0]
        self.MasterUserName = result['MasterUsername']
        self.DBName         = result['DBName']
        self.Address        = result['Endpoint']['Address']
        self.Port           = result['Endpoint']['Port']

    def connect(self):
        "make a seesion to postgres"
        self.conn = psycopg2.connect(
            dbname  = self.DBName,
            user    = self.MasterUserName,
            password = self.password,
            host     = self.Address
        )
        self.cur = self.conn.cursor()

    def make_schema(self):
        "Declare tables" 
        sql =  """
        CREATE TABLE IF NOT EXISTS
        messages(
          id  BIGSERIAL PRIMARY KEY,                                  
          topic     TEXT,
          timestamp BIGINT,
          uuid      BYTEA,
          size      INTEGER,
          key       TEXT
        );
        
        CREATE INDEX IF NOT EXISTS timestamp_idx ON messages (timestamp);
        CREATE INDEX IF NOT EXISTS topic_idx     ON messages (topic);
        COMMIT;
        
        """
        self.cur.execute(sql)

    def insert(self, payload, metadata, storeinfo):
        "insert one record into the DB"

        sql = f"""
        INSERT INTO messages 
          (topic, timestamp, uuid, size, key)
          VALUES (%s, %s, %s, %s, %s) ;
        COMMIT ; """

        uuid_ = self.get_uuid( metadata)
        self.cur.execute(sql, [metadata["topic"],
                               metadata["timestamp"],
                               uuid_,
                               storeinfo.size,
                               storeinfo.key])
        self.n_inserted +=1
        self.log()

    def launch_db_session(self):
        "lauch a query_session tool for AWS databases"
        print (f"use password: {self.password}")
        import subprocess 
        cmd = f"psql --dbname {self.DBName} --host={self.Address} --username={self.MasterUserName} --password"
        subprocess.run(cmd, shell=True)

        


##############
#
#  argparse functions 
#  
###############
def housekeep(args):
    """
    Acquire data from the specified source and log to specified DB
    """
    db     = DbFactory(args).get_db()
    source = source_api.SourceFactory(args).get_source()
    store =  StoreFactory(args).get_store()
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

