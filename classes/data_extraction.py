import boto3
import json
import os
import pandas as pd
import requests
import tabula

from .data_cleaning import DataCleaning
# need to prefix with a dot to access modules in same directory
from .database_utils import DatabaseConnector
from sqlalchemy import text


class DataExtraction:
    '''
    This class will work as a utility class that extracts data from different data sources.

    The methods contained will be fit to extract data from a particular data source, 
    these sources will include CSV files, an API and an S3 bucket.
    '''

    SYSTEMPATH = os.getcwd()

    def read_api_key(self):
        pathname = self.SYSTEMPATH + '/info/api_key.json'

        try:
            with open(pathname)as f:
                header_file = f.read()
            return json.loads(header_file)
        except FileNotFoundError as e:
            print(
                e, '\nAPI key not found. Make sure you have a api_key.json file in your /info directory with the x-api-key information.')

    def extract_from_s3(self):
        ''' uses the boto3 package to download and extract the information returning a pandas DataFrame.

        Returns dataframe of object retrieved
        '''

        s3 = boto3.client('s3')
        s3.download_file('data-handling-public', 'products.csv',
                         self.SYSTEMPATH + '/temp/products.csv')
        df = pd.read_csv(self.SYSTEMPATH + '/temp/products.csv', index_col=0)

        return df

    def extract_date_events(self, header):
        ''' Extracts the date events in json from a supplied url and returns a dataframe.

        Argument: header(dict)

        Returns: dataframe of date details
        '''

        header = {
            "Content-Type": "application/json",
            "X-API-Key": header['x-api-key']
        }

        response = requests.get(
            'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json', headers=header)

        date_events_dict = response.json()
        df = pd.DataFrame.from_dict(
            date_events_dict, orient='columns', dtype=None, columns=None)  # convert to df using reverse syntax for to_dict(orient='dict, into=dict)
        # Effective Pandas, Matt Harrison p323
        return df

    def read_rds_database(self):
        '''reads in the database from the RDS connection

        Prints a query. This was used for testing purposes only.
        '''

        data_connect = DatabaseConnector()
        table_names = data_connect.list_db_tables()
        engine = data_connect.init_db_engine()

        with engine.connect() as connection:
            for table in table_names:
                result = connection.execute(text(f"SELECT * FROM {table}"))
                for row in result:
                    print(row)

    def read_rds_table(self, database_connector, table_name):
        '''take in an sqlalchemy engine and a table name as an argument and return a pandas DataFrame.

        Arguments:
        database_connector (sqlalechemy engine): the db_engine from DatabaseConnector
        table_name (str): table name of database

        Returns:
        dataframe (pandas dataframe): Pandas dataframe
        '''
        try:
            df = pd.read_sql_table(table_name, database_connector)
            return df
        except ValueError as e:
            print("There was a problem reading the table from the database\n\t\t", e)

    def retrieve_pdf_data(self, link):
        ''' takes a pdf and returns a dataframe

        Argument: link where a pdf is hosted

        Returns: dataframe
        '''
        df_list = tabula.read_pdf(
            link, pages="all")  # https://pypi.org/project/tabula-py/

        result = pd.concat(df_list)
        return result

    def list_number_stores(self, stores_endpoint, header):
        ''' returns the number of stores to extract. 

        Parameter: number of stores endpoint 
        Parameter: header dictionary as an argument.

        Returns: number of stores       
        '''

        header = {
            "Content-Type": "application/json",
            "X-API-Key": header['x-api-key']
        }

        response = requests.get(stores_endpoint, headers=header).json()
        return response['number_stores']

    def retrieve_stores_data(self, stores_endpoint, header):
        '''
        take the retrieve a store endpoint as an argument and extracts all the stores from the API saving them in a pandas DataFrame.

        Parameter: stores_endpoint
        Returns: dataframe
        '''

        header = {
            "Content-Type": "application/json",
            "X-API-Key": header['x-api-key']
        }

        for store_number in range(0, 451):
            endpoint = f'{stores_endpoint}/{store_number}'
            response = requests.get(endpoint, headers=header).json()

            print(response['lat'])

            if store_number == 0:
                print('creating dataframe')
                index = response.pop('index')
                df = pd.DataFrame(response, index=[index])
                continue  # for the first pass we don't need to concatenate - this is the base case

            index = response.pop('index')
            df = pd.concat(
                [df, pd.DataFrame(response, index=[index])], ignore_index=True)

        return df
