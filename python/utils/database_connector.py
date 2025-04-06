import mysql.connector
import logging
import pandas as pd
logger = logging.getLogger(__name__)


class DatabaseConnector:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = None

    def connect(self):
        try:
            logger.info("Connecting to the database")
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
            logger.info("Database connection established")
        except mysql.connector.Error as err:
            logger.error("Error: %s", err)
            raise

    def disconnect(self):
        if self.connection:
            self.connection.close()

    def select_from_table(self, table_name, columns='*', where_clause=None):
        try:
            cursor = self.connection.cursor()
            query = f"SELECT {columns} FROM {table_name}"
            if where_clause:
                query += f" WHERE {where_clause}"
            cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()
            return results
        except mysql.connector.Error as err:
            logger.error("Error: %s", err)
            raise

    def get_runtime_info(self, table_name) -> tuple:
        """ returns the reload_type and last runtime for the given table, required for fetching data"""
        try:
            cursor = self.connection.cursor()
            query = f"SELECT MAX(runtime), reload_type FROM {table_name} GROUP BY reload_type ORDER BY MAX(runtime) DESC LIMIT 1"
            cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()
            if results and len(results[0]) >= 2:
                last_runtime = results[0][0]
                reload_type = results[0][1]
                return last_runtime, reload_type
            else:
                raise ValueError("Invalid results structure or empty results.")
        except mysql.connector.Error as err:
            logger.error("Error: %s", err)
            raise

    def get_last_full_reload(self, table_name) -> str:
        """ returns the last full reload for the given table, required for fetching data"""
        try:
            cursor = self.connection.cursor()
            query = f"SELECT MAX(runtime) FROM {table_name} WHERE reload_type = 'full'"
            cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()
            if results and len(results[0]) >= 1:
                last_full_reload = results[0][0]
                return last_full_reload
            else:
                raise ValueError("Invalid results structure or empty results.")
        except mysql.connector.Error as err:
            logger.error("Error: %s", err)
            raise
    
    def log_data_events(self, db_dict, table_name):
        """ logs the data events to the database"""
        try:
            cursor = self.connection.cursor()
            query = f"INSERT INTO {table_name} ({', '.join(db_dict[0].keys())}) VALUES ({', '.join(['%s'] * len(db_dict[0]))})"
            for record in db_dict:
                cursor.execute(query, tuple(record.values()))
            self.connection.commit()
            cursor.close()
            logger.info("Data events logged successfully")
        except mysql.connector.Error as err:
            logger.error("Error: %s", err)
            raise
        
    def log_archive(self, archive_dict, table_name):
        """Logs the archive to the database."""
        try:
            cursor = self.connection.cursor()
            if not archive_dict or not isinstance(archive_dict, dict):
                raise ValueError(
                    "archive_dict must be a non-empty dictionary. "
                    "Cannot log archive."
                )
            query = (
                f"INSERT INTO {table_name} "
                f"({', '.join(archive_dict.keys())}) "
                f"VALUES ({', '.join(['%s'] * len(archive_dict))})"
            )
            cursor.execute(query, tuple(archive_dict.values()))
            self.connection.commit()
            cursor.close()
            logger.info("Archive logged successfully.")
        except mysql.connector.Error as err:
            logger.error("Error: %s", err)
            raise

    def update_archive_record(self, table_name)-> dict:
        """ updates the archive record in the database"""
        try:
            cursor = self.connection.cursor()
            query = f"SELECT version, doi, reload_type, DATE(archive_date) as time, archive_public_url as file_url FROM {table_name} "
            cursor.execute(query)
            results = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            results = pd.DataFrame(results, columns=columns)
            cursor.close()
            logger.info("Archive fetched successfully")
            return results
        except mysql.connector.Error as err:
            logger.error("Error: %s", err)
            raise

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()


