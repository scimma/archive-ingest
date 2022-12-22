"""
Provide classes and API to housekeepnig relational databse 

There are two types homeomorphic classes

One class accesses postgres in AWS. This class can be configured
via housekeeping.toml. It can access the production or
development versions of psotgres via different configurations. DB
credentials  are stored in AWS secrets.

The other class  is "mock" databse useful for
development and test. It discards data.

The DbFactory class supports choosing which class is
used at run-time.

All classes use a namespace object (args), such
as provided by argparse, as part of their interface.

"""

import psycopg2
import psycopg2.extensions
import boto3
import toml
import logging
import uuid



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
        "return the selected database "
        return self.db
    

class Base_db:
    "Base class holding common methods"
    def __init__(self, args, config):
        self.n_inserted = 0
        self.log_every  = 100 # get from config file 
         
    def launch_db_session(self):
        "lauch a sheel level query session given credentials"
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
        "get uuid from metadata payload"
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
        "accepot and discard data"
        self.n_inserted += 1
        self.log()


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
        "create  a session to postgres"
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

        
