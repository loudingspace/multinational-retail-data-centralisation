import os
import yaml
from sqlalchemy import create_engine, inspect, exc


class DatabaseConnector:
    '''
    This will connect with and upload data to the database
    '''
    DATABASE_TYPE = 'postgresql'
    DBAPI = 'psycopg2'
    SYSTEMPATH = os.getcwd()

    def __init__(self):
        ''' Create an engine to connect to the local database
        '''
        try:
            pathname = self.SYSTEMPATH + '/info/postgresdb_creds.yaml'
            with open(pathname) as file:
                db_creds = yaml.safe_load(file)
                HOST, USER, PASSWORD, DATABASE, PORT = db_creds.values()

        except FileNotFoundError:
            print('Sorry, the db config file is not currently available. Please check you have this in the info directory as postgresdb_creds.yaml')

        try:
            self.engine = create_engine(
                f"{DatabaseConnector.DATABASE_TYPE}+{DatabaseConnector.DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")

        except exc.SQLAlchemyError as e:
            print('There was a problem connecting to the local postgres database. \n', e)

    def __read_db_creds(self) -> dict:  # PRIVATE method
        '''read the credentials yaml file and return a dictionary of the credentials

        Returns:
        dict: dictionary version of the yaml file
        '''
        try:
            with open(self.SYSTEMPATH + '/info/db_creds.yaml') as file:
                db_creds = yaml.safe_load(file)
            return db_creds

        except FileNotFoundError:
            print('Sorry, the db config file is not currently available.')

    def _init_db_engine(self):  # PROTECTED for reasons of encapsulation
        '''read the credentials from the return of read_db_creds and initialise and return an sqlalchemy database engine.

        Returns:
        sqlalchemy database engine
        '''

        try:
            database_credentials = self.__read_db_creds()
            # destructure dictionary values
            RDS_HOST, RDS_PASSWORD, RDS_USER, RDS_DATABASE, RDS_PORT = database_credentials.values()
            return create_engine(
                f"{DatabaseConnector.DATABASE_TYPE}+{DatabaseConnector.DBAPI}://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{RDS_DATABASE}")

        except exc.SQLAlchemyError as e:
            print('There was a problem connecting to the database. \n', e)

    def list_db_tables(self):
        '''list all the tables in the database
        '''
        engine = self._init_db_engine()
        with engine.connect() as conn:
            inspector = inspect(engine)
            table_names = inspector.get_table_names()
            return table_names

    def upload_to_db(self, df, table_name):
        '''
        Takes a dataframe and converts this to SQL and then uploads it to the database with the table_name
        '''

        try:
            with self.engine.connect() as connection:
                print('Creating ', table_name)
                df.to_sql(table_name, self.engine,
                          if_exists='replace', index=False)
        except Exception as e:
            print('Something has gone wrong: ', e)
