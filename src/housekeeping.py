#!/usr/bin/python3

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

from random import random
import toml
import logging
import argparse
import pdb
import time
import random

from sqlalchemy import Column, Integer, String, create_engine, exc, engine, MetaData, Table, Sequence
from sqlalchemy.orm import Session, declarative_base, sessionmaker
import sqlalchemy

from hop.io import Stream, StartPosition, list_topics

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
# schema
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

        #instantiate, then return db object of correct type.
        if config["type"] == "mock"  : self.db  =  Mock_db(args, config)  ; return 
        if config["type"] == "SQlite" : self.db =  Mysql_db(args, config) ; return 
        if config["type"] == "aws"   : self.db  =  AWS_db(args, config) ; return 
        logging.fatal(f"database {type} not supported")
        exit (1)
        
    def get_db(self):
        return self.db
    


class Base_db:
    

    def make_schema(self):
        "Make the housekeePing  schema, if not already present in db"
        import pdb; pdb.set_trace()
        Base = declarative_base()
        class Message(Base):
            __tablename__ = "Message",
            uid = Column(Integer, Sequence("message_sequence"), primary_key=True) #autoincremented unique id
            topic = Column(String, nullable=False)
            timestamp = Column(Integer)
            payload = Column(String)   

        Base.metadata.create_all(bind=engine)       
        self.Message = Message
            
    def insert(self, payload, metadata):

        import pdb; pdb.set_trace()
        """ message_data  = self.Message(
            uid = None,
            topic = {metadata.topic},
            timestamp = {metadata.timestamp},
            payload = {str(payload})
            )
        self.session.add(message_data)
            """ 
        self.n_insert += 1

    def launch_query(self):
        logging.fatal(f"Query tool not supported for this database")
        exit(1)
        
class Mock_db(Base_db):
    """
    a mock DB that does nothing -- support debug and devel.
    """
    def __init__(self, args, config):
        logging.info(f"Mock Database configured")
        self.n_insert = 0
        self.log_every = 1

    def make_schema(self):
        "no schema to make"
        pass 
    
    def connect(self):
        "nothing to connect to"
        pass


    def insert(self, payload, message):
        self.n_insert += 1
        if self.n_insert %  self.n_insert == 0:
            logging.info(f"inserted {self.n_insert} records so far.")
        pass

    
class SQLite_db(Base_db):
    """
    Logging to SQLlite to support development.
    """
    def __init__(self, args, config):
        # SQLite:///D:/Mar9.db
        self.file           = config['file']
        self.echo           = config["echo"]
        self.pool_pre_ping  = config["pool_pre_ping"]
        self.future         = config["future"]
        
        self.url = f"{config['rdms']}+" \
            f"{config['dbapi']}:///" \
            f"{config['file']}"
        
        self.n_insert = 0
        self.log_every = 1
        logging.info(f"MySQL Database: {self.url}")
        logging.info(f"engine option echo: {self.echo}")
        logging.info(f"engine option pre_ping: {self.pool_pre_ping}")
        logging.info(f"engine option future: {self.future}")

class AWS_db(Base_db):
    """
    Logging to AWS postgres DBs
    """
    def __init__(self, args, config):
        
 
        # get these from the configuration file
        self.aws_db_name        = "housekeeping-test-postgres" 
        self.aws_db_secret_name = "housekeeping-db-password"
        self.aws_region_name    = "us-west-2"
        self.secret_name        = "housekeeping-db-password"
        self.region_name        = "us-west-2"
        self.echo               = True
        self.pool_pre_ping      = True
        self.future             = False
        self.log_every          = 1
        self.n_insert           = 0

        # go off and get the real connections  information from AWS
        self.set_password_info()
        self.set_connect_info()
        self.n_insert = 0

        #"postgresql+pyscopg2://scott:tiger@hostname/dbname"        
        self.url = f"postgresql://" \
            f"{self.MasterUserName}:{self.password}@{self.Address}/{self.DBName}"
        
        logging.info(f"SQLlite Database: {self.url}")
        logging.info(f"engine option echo: {self.echo}")
        logging.info(f"engine option pre_ping: {self.pool_pre_ping}")
        logging.info(f"engine option future: {self.future}")

    def set_password_info(self):
        "retrieve postgress password from its AWS secret"
        import boto3
        from botocore.exceptions import ClientError

        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=self.region_name
        )
        get_secret_value_response = client.get_secret_value(
            SecretId=self.aws_db_secret_name
        )
        password = get_secret_value_response['SecretString']
        self.password = password

    def set_connect_info(self):
        "set connection variables fo connecting to AWS DB"
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
        self.conn = psycopg2.connect(
            dbname  = self.DBName,
            user    = self.MasterUserName,
            password = self.password,
            host     = self.Address
        )
        self.cur = self.conn.cursor()

    def make_schema(self):
        sql =  """
        CREATE SEQUENCE IF NOT EXISTS global_uid;
        
        CREATE TABLE IF NOT EXISTS
        text_messages(
          uid  BIGINT   PRIMARY KEY,                                  
          topic TEXT,
          timestamp INTEGER,
          payload TEXT
        );
        
        CREATE TABLE IF NOT EXISTS
        blob_messages(
          uid  BIGINT  PRIMARY KEY,                                  
          topic TEXT,
          timestamp INTEGER,
          payload bytea
        );
        COMMIT; ; 
        """
        self.cur.execute(sql)

    def insert(self, payload, metadata):

        sql = "SELECT nextval('global_uid')"
        pass
        self.cur.execute(sql,[])
        bigint = self.cur.fetchone()[0]

        blob = True
        try :
            utf8_payload = payload.decode('UTF-8')
        except UnicodeDecodeError:
            blob = True
        else :
            if utf8_payload.isprintable() : blob = False
            
        if blob:
            #Payload assessed as a blob.
            self.insert_blob(bigint, payload, metadata)
        else:
            #Payload assessed as a text string of some kind
            self.insert_text(bigint, utf8_payload, metadata)

        self.n_insert += 1

    def insert_text(self, bigint, payload, metadata):
        "insert text falsues into TEXT table using global sequence number"
        sql = f"""
        INSERT INTO text_messages 
          (uid, topic, timestamp, payload)
          VALUES (%s, %s, %s, %s) ;
        COMMIT ; """
        self.cur.execute(sql, [bigint, metadata.topic, metadata.timestamp, str(payload)])

    def insert_blob(self, bigint, payload, metadata):
        "insert blob into blob table, using global sequence number"  
        sql = f"""
        INSERT INTO blob_messages 
          (uid, topic, timestamp, payload)
          VALUES (%s, %s, %s, %s) ;
        COMMIT ; """
        self.cur.execute(sql, [bigint, metadata.topic, metadata.timestamp, payload])


    def launch_query(self):
        "lauch a query tool for AWS databases"
        print (f"use password: {self.password}")
        import subprocess 
        cmd = f"psql --dbname {self.DBName} --host={self.Address} --username={self.MasterUserName} --password"
        subprocess.run(cmd, shell=True)
        

##################################
#   Sources 
##################################

class MockMetadata:
    pass
    
class SourceFactory:
    """
    Factory class to create Mock, or HOP data sources. 
    """

    def __init__(self, args):
        toml_data = toml.load(args.toml_file)
        config    = toml_data.get(args.source_stanza, None)
        
        #instantiate, then return source object of correct type.
        if config["type"] == "mock" : self.source =  Mock_source(args, config) ; return
        if config["type"] == "hop"  : self.source =  Hop_source(args, config)  ; return
        logging.fatal(f"source {type} not supported")
        exit (1)
        
    def get_source(self):
        "return the srouce sppecified in the toml file"
        return self.source
    

class Mock_source:
    """
    a mock source  that will support capacity testing.
    """

    def __init__(self, args, config):
        logging.info(f"Mock Source configured")
        import os
        #Move these to config file once we are happy
        small_text      = b"500 Text  " * 50       #500 bytes
        large_text      = b"5K Text   " * 500      #5000 bytes
        xlarge_text     = b"50K text  " * 5000     #50000 bytes
        max_text       = b"50K text  " * 3000000   #3 meg
        
        small_binary    = os.urandom(500)       #500 bytes
        large_binary    = os.urandom(5000)      #5000 bytes
        xlarge_binary   = os.urandom(50000)     #50000 byte
        max_binary      = os.urandom(3000000)   #3 meg
        self.messages  = [small_text, large_text, xlarge_text, small_binary, large_binary, xlarge_binary, max_binary,max_text]
        self.n_sent    = 0
        self.n_events  = 1000
        self.total_message_bytes = 0
        self.t0 =  time.time()
        
    def connect(self):
        pass
    
    def is_active(self):
        if self.n_events > 0 :
            self.n_events = self.n_events-1
            return True
        else:
            return False

    def get_next(self):
        "get next mock message"
        
        metadata = MockMetadata()
        metadata.topic     = """mock "' topic"""
        metadata.timestamp = int(time.time())
        payload = self.messages[random.randrange(0,len(self.messages))]
        self.total_message_bytes += len(payload)
        self.record()
        return (payload, metadata)
    
    def record(self):
        if self.n_sent % 100 == 0 :
            delta = time.time() - self.t0
            logging.info(f"{self.n_sent} in {delta} total:{self.total_message_bytes:,}")
            self.t0 = time.time()

        self.n_sent += 1
        

class Hop_source:
    def __init__(self, args, config):
        toml_data    =   toml.load(args.toml_file)
        config       =   toml_data[args.source_stanza]
        admin_topics =   config["admin_topics"]
        self.username =  config["username"]
        self.groupname = config["groupname"]
        self.until_eos = config["until_eos"]

        self.url = (
                f"kafka://"   \
                f"{config['username']}@" \
                f"{config['hostname']}:" \
                f"{config['port']}/"
            )
        
        # Read the available topics from the given broker
        topic_dict = list_topics(url=self.url)
        
        # Concatinate the avilable topics with the broker address
        # omit "adminstrative topics
        topics = ','.join([t for t in topic_dict.keys() if t not in admin_topics])
        self.url = (f"{self.url}{topics}")

        logging.info(f"Hop Source configured: {self.url}")
  
    def connect(self):

        # Instance :class:"hop.io.Stream" with auth configured via 'auth=True'
        start_at = StartPosition.EARLIEST
        stream = Stream(auth=True, start_at=start_at, until_eos=self.until_eos)
        
        # Return the connection to the client as :class:"hop.io.Consumer" instance
        # :meth:"random" included for pre-Aplha development only
        # Remove :meth:"random" for implemented versions
        group_id = f"{self.username}-{self.groupname}{random()}" 
        self.current =stream.open(url=self.url, group_id=group_id)

    def is_active(self):
        return True

    def get_next(self):
        pdb.set_trace()
        message, metadata = next(self.current.read(metadata=True, autocommit=False))
        print(message)
        print(metadata)
        return (message, metadata)

def housekeep(args):
    """
    Acquire data from the specified source and log to specified DB
    """
    make_logging(args)
    db     = DbFactory(args).get_db()
    source = SourceFactory(args).get_source()
    db.connect()
    db.make_schema()
    source.connect()
    while source.is_active():
        payload, metadata  = source.get_next()
        db.insert(payload, metadata)

def query(args):
    "get connect info and launch a query engine"
    DbFactory(args).get_db().launch_query()

if __name__ == "__main__":

    #main_parser = argparse.ArgumentParser(add_help=False)
    main_parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    main_parser.set_defaults(func=None)
    main_parser.add_argument("-t", "--toml_file", help = "toml configuration file", default="housekeeping.toml")
    main_parser.add_argument("-l", "--log_stanza", help = "log config stanza", default="log")

    subparsers = main_parser.add_subparsers()   
    #mock -- use local mock source and sinks as defaults 
    parser = subparsers.add_parser('mock', help= "house keep w/(defaults) all mocks")
    parser.set_defaults(func=housekeep)
    parser.add_argument("-d", "--database_stanza", help = "database-config-stanza", default="mock-db")
    parser.add_argument("-s", "--source_stanza", help = "source config  stanza", default="mock-source")

    #local -- use local SQLlite, hop as defaults  (eventually)
    parser = subparsers.add_parser('local', help="house keep w/(defaultsqllite and hop")
    parser.set_defaults(func=housekeep)
    parser.add_argument("-d", "--database_stanza", help = "database-config-stanza", default="mysql")
    parser.add_argument("-s", "--source_stanza", help = "source config  stanza", default="hop")

    #dev --- use Scimma AWS development 
    parser = subparsers.add_parser('dev', help="house keep w/(default postgres and mock hop")
    parser.set_defaults(func=housekeep)
    parser.add_argument("-d", "--database_stanza", help = "database-config-stanza", default="aws-dev-db")
    parser.add_argument("-s", "--source_stanza", help = "source config  stanza", default="mock-source")

    #prod -- use scimma prod infra

    #query --- launch a query tool against AWS 
    parser = subparsers.add_parser('query', help="Launch a query tool against AWS databases")
    parser.set_defaults(func=query)
    parser.add_argument("-q", "--database_stanza", help = "database-config-stanza", default="aws-dev-db")

    pass

    args = main_parser.parse_args()
    
    if not args.func:  # there are no subfunctions
        main_parser.print_help()
        exit(1)
    args.func(args)

