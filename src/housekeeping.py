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
import boto3
from random import random
import toml
import logging
import argparse
import pdb
import time
import random
import datetime
import uuid

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
        topic = metadata.topic
        t = time.gmtime(metadata.timestamp/1000)
        key = f"{topic}/{t.tm_year}/{t.tm_mon}/{t.tm_mday}/{t.tm_hour}/{uuid.uuid4().urn}"
        return key

    def get_storeinfo(self, key, size):
        storeinfo = Store_info()
        storeinfo.size = size
        storeinfo.key = key
        storeinfo.bucket = self.bucket
        return storeinfo
        
class S3_store(Base_store):
    "send things to s3, not a dbms"
    def __init__(self, args, config):
        self.bucket = "scimma-housekeeping"
        super().__init__(args, config)
        
    def connect(self):
        "obtain an S3 Client"
        pdb.set_trace()
        self.client = boto3.client('s3')

    def store(self, payload, metadata):
        """place data, metadata as an object in S3"""
        
        bucket = self.bucket
        key = self.get_key(metadata)
        self.client.put_object(Body=payload, Bucket=bucket, Key=key)        
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
        size = len(payload) + 132
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
         
    def launch_query(self):
        logging.fatal(f"Query tool not supported for this database")
        exit(1)

    def make_schema(self):
        "no schema to make"
        pass 
    
    def connect(self):
        "nothing to connect to"
        pass
    
    def log(self):
        "log db informmation, but not too often"
        msg1 = f"insterted  {self.n_inserted} objects."
        if self.n_inserted < 5 :
            logging.info(msg1)
        elif self.n_inserted % self.log_every == 0:
            logging.info(msg1)

    
        
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
        self.aws_db_name        = "housekeeping-test-postgres" 
        self.aws_db_secret_name = "housekeeping-db-password"
        self.aws_region_name    = "us-west-2"
        self.secret_name        = "housekeeping-db-password"
        self.region_name        = "us-west-2"
        super().__init__(args, config)
        
        # go off and get the real connections  information from AWS
        self.set_password_info()
        self.set_connect_info()
        logging.info(f"aws db name, secret, region: {self.aws_db_name}, {self.aws_db_secret_name}, {self.region_name } ")
        logging.info(f"aws database, user, port, address: {self.DBName}, {self.MasterUserName}, {self.Port} ,{self.Address}")
        
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
          uuid      TEXT,
          size      INTEGER,
          key       TEXT
        );
        
        CREATE INDEX IF NOT EXISTS timestamp_idx ON messages (timestamp);
        CREATE INDEX IF NOT EXISTS topic_idx     ON messages (topic);
        CREATE INDEX IF NOT EXISTS uuid_idx      ON messages (uuid);
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
        pdb.set_trace
        self.cur.execute(sql, [metadata.topic, metadata.timestamp, metadata.uuid, storeinfo.size, storeinfo.key])
        self.n_inserted +=1
        self.log()
        
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
        config    = toml_data.get(args.hop_stanza, None)

        type = config["type"]
        #instantiate, then return source object of correct type.
        if type == "mock" : self.source =  Mock_source(args, config) ; return
        if type == "hop"  : self.source =  Hop_source(args, config)  ; return
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
        small_text     =  b"500 Text " * 50      #500 bytes
        medium_text    =  b"5K Text   " * 500    #5000 bytes
        large_text     =  b"50K text  " * 5000     #50000 bytes
        xlarge_text    =  b"50K text  " * 50000   #500000 bytes
        max_text       =  b"50K text  " * 3000000  #3 meg
        
        small_binary   = os.urandom(500)       #500 bytes
        medium_binary  = os.urandom(5000)      #5000 bytes
        large_binary   = os.urandom(50000)      #50000 byte
        xlarge_binary  = os.urandom(500000)    #50000 byte
        max_binary     = os.urandom(3000000)   #3 meg
        self.messages  = [small_text, medium_text, large_text, xlarge_text, max_text, small_binary, medium_binary, large_binary, xlarge_binary, max_binary]
        self.n_sent    = 0
        self.n_events  = 200
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
        
        #put spread in times for S3 testing.
        early_time = int(time.time()) - (60*60*24*5)  
        late_time  = int(time.time()) + (60*60*24*5)
        #vary names near the root for S3 testing
        anumber  = random.randrange(0,20)
        for message in self.messages:
            #do fewer 
            import math
            message_size = len(message)
            #hack to scal down # interations with message size.
            n_iter = int(2000/math.log(message_size,1.8))
            total_b = 0
            t0 = time.time()
            for i in range(n_iter):
                metadata = MockMetadata()
                anumber  = random.randrange(0,20)
                metadata.topic     = f"mockgroup{anumber}.mocktopic"
                metadata.timestamp = random.randrange(early_time,late_time)*1000
                metadata.uuid = uuid.uuid4().urn
                payload = message
                total_b += len(payload)
                yield (payload, metadata)
            duration = int(time.time() - t0) 
            logging.info(f"msize, niter duration, totalb :{message_size}, {n_iter}, {duration}, {total_b}" )

    def record(self):
        if self.n_sent % 100 == 0 :
            delta = time.time() - self.t0
            logging.info(f"{self.n_sent} in {delta} total:{self.total_message_bytes:,}")
            self.t0 = time.time()

        self.n_sent += 1
        

class Hop_source:
    def __init__(self, args, config):
        toml_data    =   toml.load(args.toml_file)
        config       =   toml_data[args.hop_stanza]
        self.admin_topics =   config["admin_topics"]
        self.username =  config["username"]
        self.groupname = config["groupname"]
        self.until_eos = config["until_eos"]
        self.base_url = (
                f"kafka://"   \
                f"{config['username']}@" \
                f"{config['hostname']}:" \
                f"{config['port']}/"
            )

        self.refresh_url_every =  1000  # make this a config
        self.n_recieved = 0
        self.refresh_url()          #set ural with topics.
        
    def refresh_url(self):
        "initalize/refresh the list of public topics to record PRN"

        #return if not not needed.
        if self.n_recieved  % self.refresh_url_every != 0: return
            
        # Read the available topics from the given broker
        topic_dict = list_topics(url=self.base_url)
        
        # Concatinate the avilable topics with the broker address
        # omit "adminstrative topics
        topics = ','.join([t for t in topic_dict.keys() if t not in self.admin_topics])
        self.url = (f"{self.base_url}{topics}")
        logging.info(f"Hop Url (re)configured: {self.url} excluding {self.admin_topics}")
    
  
    def connect(self):
        # Instance :class:"hop.io.Stream" with auth configured via 'auth=True'
        start_at = StartPosition.EARLIEST
        stream = Stream(auth=True, start_at=start_at, until_eos=self.until_eos)
        
        # Return the connection to the client as :class:"hop.io.Consumer" instance
        # :meth:"random" included for pre-Aplha development only
        # Remove :meth:"random" for implemented versions
        group_id = f"{self.username}-{self.groupname}{random.randint(0,10000)}" 
        self.client = stream.open(url=self.url, group_id=group_id)

    def is_active(self):
        return True

    def get_next(self):
        for result in self.client.read(metadata=True, autocommit=False):
            # What happens on error? GEt nothing back? None?
            # -- seems to stall in self.client.read
            # Cannot log an  python object -- message, metatdata are obncet
            # have to serializes -- going with  hack below for now.

            #message = result[0].serialize().__repr__()
            message = result[1]._raw.value()      #fixme
            metadata =  result[1]
            metadata.uuid = uuid.uuid4().urn  #replace w/logic that sets this if absent
            self.n_recieved  += 1
            self.refresh_url()
            #logging.info(f"topic, msg size, meta size: {metadata.topic} {len(message)}")
            yield (message, metadata)

def housekeep(args):
    """
    Acquire data from the specified source and log to specified DB
    """
    make_logging(args)
    db     = DbFactory(args).get_db()
    source = SourceFactory(args).get_source()
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

def query(args):
    "get connect info and launch a query engine"
    DbFactory(args).get_db().launch_query()

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
    parser.add_argument("-X", "--hop_stanza", help = "hopskotch config  stanza", default="mock-hop")
    parser.add_argument("-S","--store_stanza", help = "storage config stanza", default="mock-store")
    #list -- list stanzas 
    parser = subparsers.add_parser('list', help="list stanzas")
    parser.set_defaults(func=list)

    #query --- launch a query tool against AWS 
    parser = subparsers.add_parser('query', help="Launch a query tool against AWS databases")
    parser.set_defaults(func=query)
    parser.add_argument("-q", "--database_stanza", help = "database-config-stanza", default="aws-dev-db")


    args = main_parser.parse_args()
    
    if not args.func:  # there are no subfunctions
        main_parser.print_help()
        exit(1)
    args.func(args)

