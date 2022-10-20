import os
# from psycopg2 import connect
import mysql.connector
from global_vars import log

class DbConnector:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.cur = None
        self.cnx = None
        self.db_schema_version = 1

    def open_db_connection(self):
        if self.cnx is None or self.cur is None:
            # Open database connection
            self.cnx = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
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

    def parse_sql_commands(self, sql_file):
        msg = ''
        commands = []
        try:
            with open(sql_file) as f:
                dbUpdateSql = f.read()
            #Individual SQL commands must be separated by the custom delimiter '#---'
            for sql_cmd in dbUpdateSql.split('#---'):
                if len(sql_cmd) > 0 and not sql_cmd.isspace():
                    commands.append(sql_cmd)
        except Exception as e:
            msg = str(e).strip()
            log.error(msg)
        return commands

    def update_db_tables(self):
        self.open_db_connection()
        try:
            current_schema_version = 0
            try:
                # Get currently applied database schema version if tables have already been created
                self.cur.execute(
                    "SELECT `schema_version` FROM `meta` LIMIT 1"
                )
                for (schema_version,) in self.cur:
                    current_schema_version = schema_version
                log.info("schema_version taken from meta table")
            except:
                log.info("meta table not found")
            log.info('current schema version: {}'.format(current_schema_version))
            # Update the database schema if the versions do not match
            if current_schema_version < self.db_schema_version:
                # Sequentially apply each DB update until the schema is fully updated
                for db_update_idx in range(current_schema_version+1, self.db_schema_version+1, 1):
                    sql_file = os.path.join(os.path.dirname(__file__), "db_schema_update", "db_schema_update.{}.sql".format(db_update_idx))
                    commands = self.parse_sql_commands(sql_file)
                    for cmd in commands:
                        log.info('Applying SQL command from "{}":'.format(sql_file))
                        log.info(cmd)
                        # Apply incremental update
                        self.cur.execute(cmd)
                    # Update `meta` table to match
                    log.info("Updating `meta` table...")
                    try:
                        self.cur.execute(
                            "INSERT INTO `meta` (`schema_version`) VALUES ({})".format(db_update_idx)
                        )
                    except:
                        self.cur.execute(
                            "UPDATE `meta` SET `schema_version`={}".format(db_update_idx)
                        )
                    self.cur.execute(
                        "SELECT `schema_version` FROM `meta` LIMIT 1"
                    )
                    for (schema_version,) in self.cur:
                        log.info('Current schema version: {}'.format(schema_version))
        except Exception as e:
            log.error(str(e).strip())
        self.close_db_connection()

    def insert_message(self, message_text):
        self.open_db_connection()
        try:
            fields = {
                'message': message_text,
            }
            placeholders = ', '.join(['%s'] * len(fields))
            columns = ', '.join(fields.keys())
            self.cur.execute(
                (f"INSERT INTO messages ({columns}) VALUES ({placeholders})"),
                tuple(fields.values())
            )
            self.close_db_connection()
        except Exception as e:
            log.error(e)
            self.close_db_connection()
            raise
        