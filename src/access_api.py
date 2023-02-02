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

        """

        """"
        Save this. the idea is to nu require toml, if use case is just prod.
        bu tdemad for thsi at v 1.0.0 is null.
        if not config:
            config =         {
                "db-type" : "aws",
                "db-secret-name"  : "hopProd-archive-ingest-db-password", 
                "db-name"         : "hopprod-archive-ingest-db",
                "db-aws-region"   : "us-west-2",
                "store-type"      :  "S3",
                "store-bucket"    :  "hopprod-scimma-archive-ingest",
                "store-log-every" :  20
                          }
            
            config["db-name"] = "hopdevel-archive-ingest-db"
            config["db-secret-name"] =  "hopDevel-archive-ingest-db-password" 
            config["store-bucket"] =  "hopdevel-scimma-archive-ingest"
        """
            
        self.db     = database_api.DbFactory(config).get_db()
        self.store  = store_api.StoreFactory(config).get_store()
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
