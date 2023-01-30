"""
Provide classes and API to the housekeeping
relational databse

Access postgres in AWS. This class can be configured
via housekeeping.toml and command line options. The
class can be configured  to access the production or
development versions of Psotgres via different
configurations.

DB credentials and configuation information
are stored in AWS secrets.

The DbFactory class supports choosing which class is
used at run-time. The framework supports a not-implemented
extention to SQLite.

"""

import psycopg2
import psycopg2.extensions
import boto3
import toml
import logging
import utility_api


##################################
# "databases"
##################################


class DbFactory:
    """
    Factory class to create Mock, MySQL, or AWS postgres DB objects
    """
    def __init__ (self, config):

        config = utility_api.merge_config(config)
        type = config["db-type"]
        #instantiate, then return db object of correct type.
        if type == "mock"   : self.db  =  Mock_db(config)  ; return
        if type == "aws"    : self.db  =  AWS_db (config) ; return
        logging.fatal(f"database {type} not supported")
        exit (1)

    def get_db(self):
        "return the selected database "
        return self.db


class Base_db:
    "Base class holding common methods"
    def __init__(self, config):
        self.n_inserted = 0
        self.log_every  = 100 # get from config file

    def launch_db_session(self):
        "lauch a shell level query session given credentials"
        logging.fatal(f"Query_Session tool not supported for this database")
        exit(1)

    def make_schema(self):
        "no schema to make"

    def connect(self):
        "nothing to connect to"

    def log(self):
        "log db informmation, but not too often"
        msg1 = f"inserted  {self.n_inserted} objects."
        if self.n_inserted < 10 :
            logging.info(msg1)
        elif self.n_inserted == 10:
            logging.info(msg1)
            logging.info(f"reverting to logging every {self.log_every}")
        elif self.n_inserted % self.log_every == 0:
            logging.info(msg1)

    def query(self, q):
        logging.fatal("query not supported for this Db")
        exit(1)



class Mock_db(Base_db):
    """
    a mock DB that discards. -- support debug and devel.
    """
    def __init__(self, config):
        logging.info(f"Mock Database configured")
        super().__init__(config)


    def insert(self, payload, message, annotations):
        "accept and discard data"
        self.n_inserted += 1
        self.log()


class AWS_db(Base_db):
    """
    Logging to AWS postgres DBs
    """
    def __init__(self, config):
        # get these from the configuration file
        self.aws_db_name        = config["db-name"]
        self.aws_db_secret_name = config["db-secret-name"]
        self.aws_region_name    = config["db-aws-region"]
        super().__init__(config)

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
        self.conn.autocommit = True
        self.cur = self.conn.cursor()

    def make_schema(self):
        "Declare tables"
        sql =  """
        CREATE TABLE IF NOT EXISTS
        messages(
          id  BIGSERIAL  PRIMARY KEY,
          topic          TEXT,
          timestamp      BIGINT,
          uuid           TEXT,
          size           INTEGER,
          key            TEXT,
          bucket         TEXT,
          crc32          BIGINT,
          is_client_uuid BOOLEAN,
          message_crc32  BIGINT
        );

        CREATE INDEX IF NOT EXISTS timestamp_idx ON messages (timestamp);
        CREATE INDEX IF NOT EXISTS topic_idx     ON messages (topic);
        CREATE INDEX IF NOT EXISTS key_idx       ON messages (key);
        CREATE INDEX IF NOT EXISTS uuid_idx      ON messages (uuid);
        """
        self.cur.execute(sql)

    def insert(self, payload, metadata, annotations):
        "insert one record into the DB"

        sql = f"""
        INSERT INTO messages
          (topic, timestamp, uuid, size, key, bucket, crc32, is_client_uuid, message_crc32)
          VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ;
        COMMIT ; """
        values = [metadata["topic"],
                  metadata["timestamp"],
                  annotations['con_text_uuid'],
                  annotations['size'],
                  annotations['key'],
                  annotations['bucket'],
                  annotations['crc32'],
                  annotations['con_is_client_uuid'],
                  annotations['con_message_crc32']
                  ]
        self.cur.execute(sql,values)
        self.n_inserted +=1
        self.log()

    def query(self, sql, expect_results=True):
        "executer SQL, return results if expected"
        self.cur.execute(sql)
        if expect_results:
            results = self.cur.fetchall()
            return results

    def launch_db_session(self):
        "lauch a query_session tool for AWS databases"
        print (f"use password: {self.password}")
        import subprocess
        cmd = f"psql --dbname {self.DBName} --host={self.Address} --username={self.MasterUserName} --password"
        print (cmd)
        subprocess.run(cmd, shell=True)

    def make_readonly_user(self):
        """
        tested model SQL -- not importemented, though
        CREATE USER test_ro_user WITH PASSWORD ‘skjfdkjfd’;
        GRANT CONNECT ON DATABASE housekeeping TO test_ro_user ;
        GRANT USAGE ON SCHEMA public TO test_ro_user;
        GRANT SELECT ON ALL TABLES IN SCHEMA public TO test_ro_user;
        ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO test_ro_user;
        REVOKE CREATE ON SCHEMA public FROM test_ro_user;
        """
        pass #not implemented this version

    def get_logs(self):
        "return the latest postgres logs"
        session = boto3.session.Session()
        client = session.client(
            service_name='rds',
            region_name=self.aws_region_name
        )
        result = client.describe_db_log_files(DBInstanceIdentifier=self.aws_db_name)
        all_logs = ""
        for idx in [-2,-1]:
            logfile = result['DescribeDBLogFiles'][idx]['LogFileName']
            log_text_result =  client.download_db_log_file_portion(
            DBInstanceIdentifier=self.aws_db_name,
            LogFileName = logfile
            )
            all_logs += log_text_result["LogFileData"]
        return all_logs
