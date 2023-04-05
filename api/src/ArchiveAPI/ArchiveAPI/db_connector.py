import psycopg2
import logging
import os

# Configure logging
logging.basicConfig(format='%(asctime)s [%(name)-12s] %(levelname)-8s %(message)s')
log = logging.getLogger(__name__)
try:
    log.setLevel(os.environ.get('LOG_LEVEL'))
except:
    log.setLevel('WARNING')

class DbConnector:
    def __init__(self, hostname, username, password, database):
        self.hostname = hostname
        self.username = username
        self.password = password
        self.database = database
        self.cur = None
        self.cnx = None

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
