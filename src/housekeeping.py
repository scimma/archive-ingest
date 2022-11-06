#!/Usr/bin/python3

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

from random import random
import toml
import logging
import argparse
import pdb

from sqlalchemy import Column, Integer, String, create_engine, exc
from sqlalchemy.orm import Session, declarative_base

from hop.io import Stream, StartPosition, list_topics

##################################
#   Data Base 
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


Base = declarative_base()  
class MessagesTable(Base):
    
    """Defines the database table where message, metadata are inserted.

    """
    
    __tablename__ = 'Streamed_Messages'
    
    uid = Column(Integer, primary_key=True) #autoincremented unique id
    topic = Column(String, nullable=False)
    timestamp = Column(Integer)
    payload = Column(String)
    
    # def __repr__(self):
    #     return (
    #         f"Streamed_Messages(uid={self.uid!r}, "
    #         f"topic={self.topic!r}, "
    #         f"timestamp={self.timestamp!r}, "
    #         f"payload={self.payload!r})"
    #     )
        


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
            exit (1)

        #instantiate, then return db object of correct type.
        if config["type"] == "mock"  : self.db =  Mock_db(args, config)  ; return 
        if config["type"] == "mysql" : self.db =  Mysql_db(args, config) ; return 
        if config["type"] == "aws"   : self.db   =  AWS_db(args, config) ; return 
        logging.fatal(f"database {type} not supported")
        exit (1)
        
    def get_db(self):
        return self.db
    


class Base_db:
    
    def connect(self):
        logging.info(f"connecting to: {self.url}")
        self.engine = create_engine(self.url,
                                    echo=self.echo,
                                    pool_pre_ping=self.pool_pre_ping,
                                    future=self.future)
        # Create the table(s) defined via sqlalchemy.orm.declarative_base()
        # Observed behavios -- onlyif tables do ot exist?
        Base.metadata.create_all(self.engine)
        self.session = Session(self.engine)

    
    def insert(self, payload, metadata):

        # Create 'mapped' values to be inserted into the database
        values = MessagesTable(
            uid = None, #autoincremented integer as primary key
            topic = metadata.topic,
            timestamp = metadata.timestamp,
            payload = str(payload) 
        )  
        
        # insert values into the database
        try:
            self.session.add(values)
            self.session.commit() 
        except exc.OperationalError as database_error:
            session.rollback()  
            logging.critical(f"database_error = {database_error}")  
            raise RuntimeError(database_error)
            
        self.n_insert += 1
        if self.n_insert %  self.n_insert == 0:
            logging.info(f"inserted {self.n_insert} records so far.")
        pass

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
    
    def connect(self):
        pass


    def insert(self, payload, message):
        self.n_insert += 1
        if self.n_insert %  self.n_insert == 0:
            logging.info(f"inserted {self.n_insert} records so far.")
        pass

    
class Mysql_db(Base_db):
    """
    Logging to Mysql to support development.
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
        self.set_password()
        self.set_connect_info()
        self.n_insert = 0

        #"postgresql+pyscopg2://scott:tiger@hostname/dbname"        
        self.url = f"postgresql://" \
            f"{self.MasterUserName}:{self.password}@{self.Address}/{self.DBName}"
        
        logging.info(f"MySQL Database: {self.url}")
        logging.info(f"engine option echo: {self.echo}")
        logging.info(f"engine option pre_ping: {self.pool_pre_ping}")
        logging.info(f"engine option future: {self.future}")

    def set_password(self):
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
        self.n_events = 3

    def connect(self):
        pass
    
    def is_active(self):
        if self.n_events > 0 :
            self.n_events = self.n_events-1
            return True
        else:
            return False

    def get_next(self):
        import time
        metadata = MockMetadata()
        metadata.topic     = "mock topic"
        metadata.timestamp = int(time.time())
        return ("mock  event payload", metadata)


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

