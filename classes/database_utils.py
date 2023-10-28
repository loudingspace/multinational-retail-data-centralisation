import yaml
from sqlalchemy import create_engine, inspect, exc, text
import pandas as pd  # not sure if we need this, but it was int he example files

# I think this should probably be private, at the most protected


class DatabaseConnector:
    '''
    This will connect with and upload data to the database
    '''
    DATABASE_TYPE = 'postgresql'
    DBAPI = 'psycopg2'

    def read_db_creds(self) -> dict:  # THIS SHOULD BE PRIVATE
        '''read the credentials yaml file and return a dictionary of the credentials

        Returns:
        dict: dictionary version of the yaml file
        '''
        try:
            with open('../info/db_creds.yaml') as file:
                db_creds = yaml.safe_load(file)
            return db_creds

        except FileNotFoundError:
            print('Sorry, the db config file is not currently available.')

    def init_db_engine(self):  # not sure how to specify engine as a type
        '''read the credentials from the return of read_db_creds and initialise and return an sqlalchemy database engine.

        Returns:
        sqlalchemy database engine
        '''

        try:
            # is this a class or an instance method? (might want to make this a class method later)
            database_credentials = self.read_db_creds()
            # destructure dictionary values
            RDS_HOST, RDS_PASSWORD, RDS_USER, RDS_DATABASE, RDS_PORT = database_credentials.values()
            return create_engine(
                f"{DatabaseConnector.DATABASE_TYPE}+{DatabaseConnector.DBAPI}://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{RDS_DATABASE}")

        except exc.SQLAlchemyError as e:
            print('There was a problem connecting to the database. \n', e)

    def list_db_tables(self):
        '''list all the tables in the database
        '''
        engine = self.init_db_engine()
        with engine.connect() as conn:
            inspector = inspect(engine)
            table_names = inspector.get_table_names()
            # print('Table name: ', table_names)
            return table_names

        # DATABASE_TYPE = 'postgresql'
        # DBAPI = 'psycopg2'
        # HOST = '127.0.0.1'
        # USER = 'postgres'
        # PASSWORD = 'ronald22'
        # DATABASE = 'pagila'
        # PORT = 5432
        # engine = create_engine(
        #     f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")

        # RDS_HOST: data-handling-project-readonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com
        # RDS_PASSWORD: AiCore2022
        # RDS_USER: aicore_admin
        # RDS_DATABASE: postgres
        # RDS_PORT: 5432

        # engine.execution_options(isolation_level='AUTOCOMMIT').connect()
        # from sqlalchemy import inspect
        # inspector = inspect(engine)
        # inspector.get_table_names()

        # from sqlalchemy import text
        # with engine.connect() as connection:
        #     result = connection.execute(text("SELECT * FROM actor"))
        #     for row in result:
        #         print(row)

    def upload_to_db(self, df, table_name):
        '''
        Takes a dataframe and converts this to SQL and then uploads it to the database with the table_name
        '''

        try:
            with open('../info/postgresdb_creds.yaml') as file:
                db_creds = yaml.safe_load(file)

        except FileNotFoundError:
            print('Sorry, the db config file is not currently available.')

        # # TODO: Need to change this to make it more secure
        # #  this needs tidying up
        # HOST = '127.0.0.1'
        # USER = 'postgres'
        # PASSWORD = 'ronald22'
        # DATABASE = 'sales_data'
        # PORT = 5432

        HOST, PASSWORD, USER, DATABASE, PORT = db_creds.values()

        engine = create_engine(
            f"{DatabaseConnector.DATABASE_TYPE}+{DatabaseConnector.DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")

        with engine.connect() as connection:
            # result = connection.execute(
            #     text("SELECT * FROM information_schema.schemata"))
            # for row in result:
            #     print(row)

            df.to_sql(table_name, engine, if_exists='replace', index=False)


#######
# Testing
#######
a = DatabaseConnector()
# a.read_db_creds()
# a.init_db_engine()
a.list_db_tables()
