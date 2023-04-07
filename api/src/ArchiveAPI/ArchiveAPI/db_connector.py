import psycopg2
import pymongo
import logging
import os
import simple_bson
import json
import io
from avro.datafile import DataFileReader
from avro.io import DatumReader
import boto3

# Configure logging
logging.basicConfig(format='%(asctime)s [%(name)-12s] %(levelname)-8s %(message)s')
log = logging.getLogger(__name__)
try:
    log.setLevel(os.environ.get('LOG_LEVEL'))
except:
    log.setLevel('WARNING')

s3_config = {
    'store-primary-bucket': os.environ.get('S3_PRIMARY_BUCKET', "hop-messages"),
    'store-backup-bucket': os.environ.get('S3_BACKUP_BUCKET', "hop-messages-backup"),
    'store-endpoint-url': os.environ.get('S3_ENDPOINT_URL', "http://object-store:9000"),
    'store-region-name': os.environ.get('S3_REGION_NAME', ""),
    'store-log-every': os.environ.get('S3_LOG_EVERY', 20),
}

class DbConnector:
    def __init__(self, hostname, username, password, database):
        self.hostname = hostname
        self.username = username
        self.password = password
        self.database = database
        self.cur = None
        self.cnx = None
        self.s3_client = None

    def open_db_connection(self):
        if self.cnx is None or self.cur is None:
            # Open database connection
            self.cnx = psycopg2.connect(
                user=self.username,
                password=self.password,
                host=self.hostname,
                port="5432",
                database=self.database,
            )
            # Get database cursor object
            self.cur = self.cnx.cursor()

    def close_db_connection(self):
        if self.cnx != None and self.cur != None:
            try:
                # Commit changes to database and close connection
                self.cnx.commit()
                self.cur.close()
                self.cnx.close()
                self.cur = None
                self.cnx = None
            except Exception as e:
                error = str(e).strip()
                self.cur = None
                self.cnx = None
                return False, error

    def list_topic(self, topic):
        self.open_db_connection()
        messages = []
        try:
            self.cur.execute('''
                SELECT uuid, timestamp, key
                FROM messages
                WHERE topic = %(topic)s
            ''', {
                'topic': topic,
            })
            for (uuid, timestamp, key) in self.cur:
                messages.append({
                    'uuid': uuid,
                    'timestamp': timestamp,
                    'key': key,
                })
        except Exception as e:
            log.error(str(e).strip())
        self.close_db_connection()
        return messages

    def list_topics(self):
        self.open_db_connection()
        topics = []
        try:
            self.cur.execute('''
                SELECT topic
                FROM messages
            ''')
            for (topic,) in self.cur:
                if topic not in topics:
                    topics.append(topic)
        except Exception as e:
            log.error(str(e).strip())
        self.close_db_connection()
        return topics

    def message_details(self, id=None):
        self.s3_connect()
        self.open_db_connection()
        details = {
            'metadata': "",
            'message': "",
        }
        try:
            self.cur.execute('''
                SELECT key, topic
                FROM messages
                WHERE uuid = %(uuid)s
            ''', {
                'uuid': id,
            })
            for (key, topic,) in self.cur:
                if topic not in ['gcn.notice','gcn.circular']:
                    details = {
                        'metadata': "Rendering message detail is not supported for this topic",
                        'message': "",
                    }
                    break
                msg_object = self.s3_get_object(key)
                bundle =  simple_bson.loads(msg_object)
                details['message'] = json.loads(bundle["message"]["content"].decode('utf-8'))
                details['metadata'] = bundle["metadata"]
                log.debug(f'''metadata:\n{details['metadata']}''')
                log.debug(f'''message:\n{details['message']}''')
        except Exception as e:
            log.error(str(e).strip())
            details = {
                'metadata': "",
                'message': "",
            }
        self.close_db_connection()
        return details

    def s3_connect(self):
        self.s3_client = boto3.client('s3',
            endpoint_url=s3_config['store-endpoint-url'],
            region_name = s3_config['store-region-name'],
            aws_access_key_id=os.environ['S3_ACCESS_KEY_ID'],
            aws_secret_access_key=os.environ['S3_SECRET_ACCESS_KEY'],
        )
        self.s3_primary_bucket = s3_config['store-primary-bucket']

    def s3_get_object(self, key):
        response = self.s3_client.get_object(Bucket=self.s3_primary_bucket, Key=key)
        data = response['Body'].read()
        return data

class MongoDbConnector:
    def __init__(self, hostname, username, password, database, collection, port):
        self.hostname = hostname
        self.username = username
        self.password = password
        self.database = database
        self.default_collection = collection
        self.port = port
        self.collection = None
        self.db = None
        self.client = None

    def connect(self):
        from urllib.parse import quote_plus
        uri = f'''mongodb://{quote_plus(self.username)}:{quote_plus(self.password)}@{self.hostname}:{self.port}'''
        log.debug(uri)
        self.client = pymongo.MongoClient(uri)
        self.db = self.client[self.database]
        log.debug(self.db)
        self.collection = self.db[self.default_collection]
        log.debug(self.collection)

    def message_details(self, id=None):
        self.connect()
        details = {
            'metadata': "",
            'message': "",
        }


        class CustomEncoder(json.JSONEncoder):
            def default(self, obj):
                if isinstance(obj, bytes):
                    return "[BINARY DATA]"
                # Let the base class default method raise the TypeError
                return json.JSONEncoder.default(self, obj)


        try:
            message = self.collection.find_one({"SciMMA_uuid": id})
            metadata = {
                'SciMMA_topic': message['SciMMA_topic'],
                'SciMMA_timestamp': message['SciMMA_timestamp'],
                'SciMMA_published_time': message['SciMMA_published_time'],
                'format': message['format'],
            }
            if message["format"] == "avro":
                reader  = DataFileReader(io.BytesIO(message['content']), DatumReader())
                content = [ r for r in reader][0]
            else:
                content = json.loads(message['content'])
            content = json.loads(json.dumps(content, indent=2, cls=CustomEncoder))
            details = {
                'metadata': metadata,
                'message': content,
            }
            log.debug(json.dumps(details, indent=2))
        except Exception as e:
            log.error(str(e).strip())
            details = {
                'metadata': "There was an error parsing the message data.",
                'message': "There was an error parsing the message data.",
            }
        return details
