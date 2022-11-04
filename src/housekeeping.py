#!/Usr/bin/python3

'''
Pr-Alpha, Created on Jul 8, 2021

@author: Mahmoud Parvizi (parvizim@msu.edu)
'''

from random import random
import toml
import logging
import argparse
import pdb

from sqlalchemy import Column, Integer, String, create_engine, exc
from sqlalchemy.orm import Session, declarative_base

#from hop.io import Stream, StartPosition, list_topics

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

    
    def insert(self, metadata, message):
        
        # Create 'mapped' values to be inserted into the database
        values = MessagesTable(
            uid = None, #autoincremented integer as primary key
            topic = metadata.topic,
            timestamp = metadata.timestamp,
            payload = str(message) 
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


    def insert(self, topic, payload):
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
        logging.fatal(f"source {type} not supported")
        exit (1)
        
    def get_source(self):
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
        return (metadata, "mock  event payload")
    
class Hop:
    def __init__(self, args):
        toml_data = toml.load(args.toml_file_name)
        config    = toml_data[args.source_stanza]    
        self.url = (
                f"kafka://"
                f"{self.hop_data['username']}@"
                f"{self.hop_data['hostname']}:"
                f"{self.hop_data['port']}/"
            )
        logging.info(f"Hop Source configured: {self.url}")

    def connect(self):
        pass

    def is_active(self):
        return True

    def get_next(self):
        "get mock event"
        #jsut ashell of what we need for capacityh testing.
        return (["fake topic", "fake event payload"])
 
def housekeep(args):
    """
    Acquire data from the specified source and log to specified DB
    """
    import pdb
    make_logging(args)
    db     = DbFactory(args).get_db()
    source = SourceFactory(args).get_source()
    db.connect()
    source.connect()
    while source.is_active():
        metadata, payload  = source.get_next()
        db.insert(metadata, payload)

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
    parser = subparsers.add_parser('local', help="house keep w/(defulat postgres and mock hop")
    parser.set_defaults(func=housekeep)
    parser.add_argument("-d", "--database_stanza", help = "database-config-stanza", default="mysql")
    parser.add_argument("-s", "--source_stanza", help = "source config  stanza", default="mock-source")

    #dev --- use Scimma AWS development 
    parser = subparsers.add_parser('dev', help="house keep w/(defulat postgres and mock hop")
    parser.set_defaults(func=housekeep)
    parser.add_argument("-d", "--database_stanza", help = "database-config-stanza", default="aws-dev-db")
    parser.add_argument("-s", "--source_stanza", help = "source config  stanza", default="mock-source")

    #prod -- use scimma prod infra
    pass

    args = main_parser.parse_args()
    
    if not args.func:  # there are no subfunctions
        main_parser.print_help()
        exit(1)
    args.func(args)

