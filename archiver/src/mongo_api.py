"""
Provide classes and API to the mongodb docoument databse. 

Access comminith verion of mongo dm.

"""

import pymongo
import logging
import utility_api
import os
import json
import io
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

##################################
# "databases"
##################################


class MongoFactory:
    """
    Factory class to create Mock, MySQL, or AWS postgres DB objects
    """
    def __init__ (self, config):

        config = utility_api.merge_config(config)
        type = config["mongo-type"]
        #instantiate, then return db object of correct type.
        if type == "local" : self.mdb = Mongo_db(config) ; return
        logging.fatal(f"database {type} not supported")
        exit (1)

    def get_mdb(self):
        "return the selected database "
        return self.mdb


class Mongo_db:
    "Base class holding common methods"
    def __init__(self, config):
        self.config = config
        self.mongo_host = config["mongo-ip"]
        self.mongo_port = config["mongo-port"]
        self.n_inserted = 0
        self.log_every  = 100 # get from config file

    def launch_db_session(self):
        "lauch a shell level query session given credentials"
        logging.fatal(f"Query_Session tool not supported for this database")
        exit(1)

    def make_schema(self):
        "no schema to make"
        pass 
      
    def connect(self):
        "conenct to server"
        "for local dev,  start server according to https://www.mongodb.com/docs/manual/tutorial/install-mongodb-on-os-x/"
        logging.info(f"about to connect to mongodb at {self.mongo_host}, {self.mongo_port}")
        
        from urllib.parse import quote_plus
        user = os.environ.get('MONGO_INITDB_ROOT_USERNAME', '')
        password = os.environ.get('MONGO_INITDB_ROOT_PASSWORD', '')
        uri = f'''mongodb://{quote_plus(user)}:{quote_plus(password)}@{self.mongo_host}:{self.mongo_port}'''
        self.client = pymongo.MongoClient(uri)
        # self.client = pymongo.MongoClient(self.mongo_ip, self.mongo_port)
        self.db = self.client["SCiMMA_archive"]
        self.collection = self.db["SCiMMA_public"]

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

    def insert(self, payload, metadata, annotations):
        "insert one record into the DB  for just the event."
        import datetime
        topic = metadata["topic"]
        published_timestamp = metadata["timestamp"]/1000.
        published_time = datetime.datetime.fromtimestamp(published_timestamp).strftime('%c')
        uuid = annotations['con_text_uuid']
        """
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
        """
        payload["SciMMA_topic"] = topic
        payload["SciMMA_timestamp"] = published_timestamp
        payload["SciMMA_published_time"] = published_time
        payload["SciMMA_uuid"] = uuid
        logging.info("about to insert")
        logging.info(f"{payload['format']}")
        if payload["format"] == "json" :
            # load and flatten user content into main record
            user_json = json.loads(payload["content"])
            t = type(user_json)
            if t == type(""):
                # double encoded string
                user_json = json.loads(user_json)
                t = type(user_json)
            if t == type({}):
                payload.update(user_json)
            else:
                logging.info(f"end json type is {t}")
        elif payload["format"] == "avro" :
            #import pdb; pdb.set_trace()
            # right now, handle avro encoding jsist one dictionary
            # mearge that into the main payload
            f = io.BytesIO(payload["content"])
            reader  = DataFileReader(f, DatumReader())
            array = [ r for r in reader]
            if len(array) == 1 and type(array[0]) == type({}):
                 payload.update(array[0])     
        else:
            # blob or unknown user format
            # don't touch
            pass
        
        self.collection.insert_one(payload)
        self.n_inserted +=1
        self.log()
        
    def query(self, exxpression, expect_results=True):
        "executer db operation , return results if expected"
        for element in self.collection.find():
            print(element) #hack

    def launch_db_session(self):
        "lauch a query_session tool for AWS databases"
        print ("NOT_IMPLEMENTED")

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
        print ("NOT_IMPLEMENTED")

    def get_logs(self):
        "return the latest postgres logs"
        print ("NOT_IMPLEMENTED")


        
