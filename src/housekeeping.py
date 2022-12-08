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
from random import random
import toml
import logging
import argparse
import pdb
import time
import random
import datetime
import uuid
import bson
import json

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

    def launch_query(self):
        "lauch a query tool for AWS databases"
        print (f"use password: {self.password}")
        import subprocess 
        cmd = f"psql --dbname {self.DBName} --host={self.Address} --username={self.MasterUserName} --password"
        subprocess.run(cmd, shell=True)

        

##################################
#   Sources 
##################################

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

class Base_source:

    def __init__(self, args, config):
        pass
    

    def add_missing_headers(self, headers):
        "add the _id header (uuid) if its not present"
        if not headers :
            headers = [("_id", uuid.uuid4().bytes)]
        else:
            labels, values = zip(headers)
            if "_id" not in labels:
                headers.append(("_id", uuid.uuid4().bytes))
        return headers
    
class Mock_source(Base_source):
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
        super().__init__(args, config)
        
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
                anumber  = random.randrange(0,20)
                topic     = f"mockgroup{anumber}.mocktopic"
                timestamp = random.randrange(early_time,late_time)*1000
                headers = self.add_missing_headers([])

                metadata = {"timestamp" :timestamp,
                        "headers" : headers,
                        "topic" : topic
                        }

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
        

class Hop_source(Base_source):
    def __init__(self, args, config):
        self.args    = args
        toml_data    =   toml.load(args.toml_file)
        config       =   toml_data[args.hop_stanza]
        self.vetoed_topics = config["vetoed_topics"]
        #self.username      = config["username"]
        self.groupname     = config["groupname"]
        self.until_eos     = config["until_eos"]
        self.secret_name   = config["aws-secret-name"]
        self.region_name   = config["aws-secret-region"]
        self.authorize()
        self.base_url = (
                f"kafka://"   \
                f"{self.username}@" \
                f"{config['hostname']}:" \
                f"{config['port']}/"
            )

        self.refresh_url_every =  1000  # make this a config
        self.n_recieved = 0
        self.refresh_url()          #set ural with topics.
        super().__init__(args, config)
        
    def refresh_url(self):
        "initalize/refresh the list of topics to record PRN"
        #return if not not needed.
        if self.n_recieved  % self.refresh_url_every != 0: return
        if self.args.topic:
            #this implementation suposrt test and debug.
            topics = args.topic
        else: 
            # Read the available topics from the given broker
            topic_dict = list_topics(url=self.base_url)
        
            # Concatinate the avilable topics with the broker address
            # omitvetoed  topics
            topics = ','.join([t for t in topic_dict.keys() if t not in self.vetoed_topics])
        self.url = (f"{self.base_url}{topics}")
        logging.info(f"Hop Url (re)configured: {self.url} excluding {self.vetoed_topics}")
    
  
    def connect(self):
        # Instance :class:"hop.io.Stream" with auth configured via 'auth=True'
        start_at = StartPosition.EARLIEST
        stream = Stream(auth=self.auth, start_at=start_at, until_eos=self.until_eos)
        
        # Return the connection to the client as :class:"hop.io.Consumer" instance
        # THe commented uot group ID (below)) migh tbe useful in some development
        # environments it allows for re-consumption of all the existing events in all
        # the topics. 
        #group_id = f"{self.username}-{self.groupname}{random.randint(0,10000)}" 
        group_id = f"{self.username}-{self.groupname}" 
        self.client = stream.open(url=self.url, group_id=group_id)

    def authorize(self):
        "authorize using AWS secrets"
        from hop.auth import Auth
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=self.region_name
        )
        resp = client.get_secret_value(
            SecretId=self.secret_name
        )['SecretString']
        resp = json.loads(resp)
        self.username = resp["username"]
        logging.info (f"hopskotch username is: {self.username}")
        self.auth = Auth(resp["username"], resp["password"])
        
    def is_active(self):
        return True

    def get_next(self):
        for result in self.client.read(metadata=True, autocommit=False):
            # What happens on error? GEt nothing back? None?
            # -- seems to stall in self.client.read
            # -- lack a full udnerstanding fo thsi case. 
 
            message = result[0].serialize()
            if result[1].headers is None :
                headers = []
            else:
                headers = [h for h in result[1].headers]
            #correct this code when we see the implemenatipn of headers
            #
            headers = self.add_missing_headers([])

            #copy out the metadata of interest to what we save
            metadata = {"timestamp" : result[1].timestamp,
                        "headers" : headers,
                        "topic" : result[1].topic
                        }
            self.n_recieved  += 1
            self.refresh_url()
            #logging.info(f"topic, msg size, meta size: {metadata.topic} {len(message)}")
            yield (message, metadata)

    def publish(self, topic):
        """publish a few messages as a test fixture

           this is (obvously) underdeveloped
        """
        import subprocess
        for n in range(2):
            json = '{{"message" : "This is Blob %s"}}'.format(n)
            cmd = f"echo '{json}' | hop publish -f BLOB -t {self.base_url}{topic}"
            cmd = f"hop publish -f JSON -t {self.base_url}{topic} dog.json"
            logging.info(f"{cmd}")
            subprocess.run(cmd, shell=True)
        pass
    
    pass

def housekeep(args):
    """
    Acquire data from the specified source and log to specified DB
    """
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

    #query --- launch a query tool against AWS 
    parser = subparsers.add_parser('query', help="Launch a query shell against AWS databases")
    parser.set_defaults(func=query)
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

