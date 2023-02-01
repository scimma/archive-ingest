"""
softare supporting API access to a scimma data store.

This is a small librare  meant for AWS-enabled turused internal
users. and as a shim for trusted co-developers.

"""
import database_api
import store_api
import bson

###########
# storage 
###########

class Archive_access():
    def __init__(self, config):
        """
        Instantiate DB objects and store objects

        replace hard-codede config items wiht user supplied
        configuration dictionary. for hardcoded defaults
        supply an empty dictionary for config.
        """
        hard_config =         {
        "db-type" : "aws",
        "db-name" : "hopdevel-archive-ingest-db"",
        "db-secret-name" : "hopDevel-archive-ingest-db-password", 
        "db-aws-region"   : "us-west-2",
        "store-type"      :  "S3",
        "store-bucket"    :  "hopdevel-scimma-archive-ingest",
        "store-log-every" :  20
                          }
        hard_config.update(config)
        self.db     = database_api.DbFactory(hard_config).get_db()
        self.store  = store_api.StoreFactory(hard_config).get_store()
        self.db.connect()
        self.db.query(
            "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY;",
            expect_results = False
            )
        self.store.connect()

    def query(self, sql):
        "perform a select query in the databse."
        return self.db.query(sql)
    
    def get_raw_object(self, key):
        "get tth raw object -- bson bundle)"
        bundle = self.store.get_object(key)
        return bundle

    def get_all(self, key):
        "get message and all retained metadata (includes header)"
        bundle = self.get_raw_object(key)
        bundle =  bson.loads(bundle)
        message = bundle["message"]["content"]
        metadata = bundle["metadata"]
        return (message, metadata)

    def get_as_sent(self, key):
        "get message and header as sent to kafka"
        message, metadata =  self.get_all(key)
        return (message, metadata["headers"])

    def get_object_summary(self, key):
        "check if object is present, and return size if so"
        return self.store_get_object_summary()
