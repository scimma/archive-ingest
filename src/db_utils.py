import os
import mysql.connector
import numpy as np
import utils


log = utils.get_logger(__name__)

class DbConnector:
    photometry_table_cols = {'time', 'magnitude', 'e_magnitude', 'band', 'candidate', 'ra', 'dec'}
    result_table_cols = {'time', 'kn_score', 'other_score', 'candidate', 'ra', 'dec'}

    def __init__(self, mysql_host, mysql_user, mysql_password, mysql_database):
        self.host = mysql_host
        self.user = mysql_user
        self.password = mysql_password
        self.database = mysql_database
        self.cur = None
        self.cnx = None
        self.db_schema_version = 8

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
            # log.info("Opened database connection")

    def close_db_connection(self):
        if self.cnx != None and self.cur != None:
            try:
                # Commit changes to database and close connection
                self.cnx.commit()
                self.cur.close()
                self.cnx.close()
                self.cur = None
                self.cnx = None
                # log.info("Closed database connection")
            except Exception as e:
                error = str(e).strip()
                self.cur = None
                self.cnx = None
                return False, error

    def insert_photometry_data(self, data):
        assert self.cnx, "No database connection"
        assert set(data) - self.photometry_table_cols == set()
        # log.info(f'''insert_photometry_data data: {data}''')
        insert_query = ('''
            INSERT INTO photometry (
                `time`,
                `magnitude`,
                `e_magnitude`,
                `band`,
                `candidate`,
                `ra`,
                `dec`
            ) VALUES (
                %(time)s,
                %(magnitude)s,
                %(e_magnitude)s,
                %(band)s,
                %(candidate)s,
                %(ra)s,
                %(dec)s
            )
        '''
        )
        self.cur.execute(insert_query, data)
        self.cnx.commit()


    def get_photometry_latest_id(self):
        assert self.cnx, "No database connection"
        select_query = '''
            SELECT id FROM photometry ORDER BY id DESC
        '''
        self.cur.execute(select_query)
        id = self.cur.fetchone()
        if isinstance(id, tuple) and len(id) > 0:
            return id[0]
        else:
            return 0


    def insert_results_data(self, data):
        assert self.cnx, "No database connection"
        insert_query = ('''
            INSERT INTO results (
                `time`,
                `kn_score`,
                `other_score`,
                `candidate`,
                `ra`,
                `dec`,
                `uuid`
            ) VALUES (
                %(time)s,
                %(kn_score)s,
                %(other_score)s,
                %(candidate)s,
                %(ra)s,
                %(dec)s,
                %(uuid)s
            )
        ''')
        if not isinstance(data, list):
            data = [data]
        for datum in data:
            self.cur.execute(insert_query, datum)
        self.cnx.commit()


    def get_results_data(self, min_id=0, max_id=None):
        assert self.cnx, "No database connection"
        select_query = f'''SELECT * FROM results WHERE id >= {min_id}'''
        if max_id:
            select_query += f''' AND id <= {max_id}'''
        self.cur.execute(select_query)
        data = self.cur.fetchall()
        return data


MARIADB_HOSTNAME = os.getenv('MARIADB_SERVICE_NAME')
MARIADB_DATABASE = os.getenv('MARIADB_DATABASE')
MARIADB_USER = os.getenv('MARIADB_USER')
MARIADB_PASSWORD = os.getenv('MARIADB_PASSWORD')
