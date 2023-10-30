from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
from sqlalchemy import text, exc
import pandas as pd
import tabula
import requests
import boto3
import json


class DataExtraction:
    '''
    This class will work as a utility class, in it you will be 
    creating methods that help extract data from different data sources.

    The methods contained will be fit to extract data from a particular data source, 
    these sources will include CSV files, an API and an S3 bucket.
    '''

    def extract_from_s3(self):
        ''' uses the boto3 package to download and extract the information returning a pandas DataFrame.

        Returns dataframe of object retrieved
        '''

        # TODO: have to use the AWS CLI

        s3 = boto3.client('s3')
        s3.download_file('data-handling-public', 'products.csv',
                         '../temp/products.csv')

        df = pd.read_csv('../temp/products.csv', index_col=0)

        # print(df.info())
        return df

    def extract_date_events(self):
        header = {
            "Content-Type": "application/json",
            "X-API-Key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"
        }

        response = requests.get(
            'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json', headers=header)

        # with open("../temp/date_details.json", "w") as f:
        #     json.dump(response.json(), f)

        # so it's a dictionary and we need to extract the columns as dictionary key, values, where inside they are of the format index : value

        date_events_dict = response.json()

        # for column_name in date_events_dict.keys():
        #     # they are all the same length
        #     print(column_name, len(date_events_dict[column_name]))

        df = pd.DataFrame.from_dict(
            date_events_dict, orient='columns', dtype=None, columns=None)  # convert to df using reverse syntax for to_dict(orient='dict, into=dict)
        # Effective Pandas, Matt Harrison p323

        return df

    def read_rds_database(self):
        '''reads in the database from the RDS connection

        Returns: object with database
        '''
        # so we get the table names from DatabaseConnect
        # and then we read the data in these tables into memory
        data_connect = DatabaseConnector()
        table_names = data_connect.list_db_tables()
        engine = data_connect.init_db_engine()

        with engine.connect() as connection:
            for table in table_names:
                result = connection.execute(text(f"SELECT * FROM {table}"))
                for row in result:
                    print(row)

    def read_rds_table(self, database_connector, table_name):
        '''take in an instance of DatabaseConnector and a table name as an argument and return a pandas DataFrame.

        Parameters:
        database_connector (DatabaseConnector): instance of DatabaseConnector
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
        print('Length of list is ', len(df_list))

        result = pd.concat(df_list)

        # so we want to concatenate the list, ignoring the top column

        return result

    def list_number_stores(self, stores_endpoint, header):
        ''' returns the number of stores to extract. 

        Parameter: number of stores endpoint 
        Parameter: header dictionary as an argument.

        Returns: number of stores       
        '''
        KEY = header['x-api-key']

        header = {
            "Content-Type": "application/json",
            "X-API-Key": KEY
        }

        response = requests.get(stores_endpoint, headers=header).json()
        return response['number_stores']

        # api_dict = {"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}

    def retrieve_stores_data(self, stores_endpoint):
        '''
        take the retrieve a store endpoint as an argument and extracts all the stores from the API saving them in a pandas DataFrame.

        Parameter: stores_endpoint
        Returns: dataframe
        '''
        header = {
            "Content-Type": "application/json",
            "X-API-Key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"
        }

        ''''
        # create original dataframe
        index = a.pop('index')
        df = pd.DataFrame(a, index=[index])

        # add to dataframe
        index = b.pop('index')
        df = pd.concat( [df, pd.DataFrame(b, index=[index]) ], ignore_index=True )
        '''

        # we know there are 451 stores from the list_number_stores function
        # i guess range doesn't go up to the final number?
        for store_number in range(0, 451):
            endpoint = f'{stores_endpoint}/{store_number}'
            # print(endpoint)
            response = requests.get(endpoint, headers=header).json()

            if store_number == 0:
                print('creating dataframe')
                index = response.pop('index')
                df = pd.DataFrame(response, index=[index])
                # print(df)
                continue

            # print(store_number, response)
            index = response.pop('index')
            df = pd.concat(
                [df, pd.DataFrame(response, index=[index])], ignore_index=True)
            # print(df)

        '''
        number = 0
        stores_endpoint = f'{stores_endpoint}/{number}'
        response = requests.get(stores_endpoint, headers=header).json()
        print(response)
        '''
        print(df.head(), df.info())

        # to save time going to save this
        # df.to_csv('../temp/stores.csv', index=False)

        return df


##### Test #####
dc = DatabaseConnector()
de = DataExtraction()
clean = DataCleaning()


'''

# Task 3
# Use your list_db_tables method to get the name of the table containing user data.
table_list = dc.list_db_tables()
# Use the read_rds_table method to extract the table containing user data and return a pandas DataFrame.
df = de.read_rds_table(dc.init_db_engine(), table_list[1])
clean.clean_user_data(df)
dc.upload_to_db(df, 'dim_users')  # for testing datacleaning

# Task 4
pdf_link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
pdf_df = de.retrieve_pdf_data(pdf_link)
pdf_df = clean.clean_card_data(pdf_df)
dc.upload_to_db(pdf_df, 'dim_card_details')
'''

'''
# Task 5
# should probably put this in a file somewhere
api_dict = {"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
a = de.list_number_stores(
    "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores", api_dict)

stores_df = de.retrieve_stores_data(
    'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details')

# stored the df temporarily so we can clean the data without having to wait
# stores_df = pd.read_csv('../temp/stores.csv')
stores_df = clean.clean_store_data(stores_df)
dc.upload_to_db(stores_df, 'dim_stores_details')

'''

''''
# Task6

products_df = de.extract_from_s3()
products_df = clean.convert_product_weight(products_df)
products_df = clean.clean_products_data(products_df)
dc.upload_to_db(products_df, 'dim_products')
'''

'''
# Task 7
table_list = dc.list_db_tables()
print(table_list)
order_df = de.read_rds_table(dc.init_db_engine(), table_list[2])
print(order_df.info())
order_df = clean.clean_orders_table(order_df)
dc.upload_to_db(order_df, 'orders_table')
'''

date_events_df = de.extract_date_events()
date_events_df = clean.clean_date_events(date_events_df)
dc.upload_to_db(date_events_df, 'dim_date_times')
