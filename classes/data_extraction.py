from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
from sqlalchemy import text, exc
import pandas as pd
import tabula


class DataExtraction:
    '''
    This class will work as a utility class, in it you will be 
    creating methods that help extract data from different data sources.

    The methods contained will be fit to extract data from a particular data source, 
    these sources will include CSV files, an API and an S3 bucket.
    '''

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


##### Test #####
dc = DatabaseConnector()
de = DataExtraction()
clean = DataCleaning()

# Use your list_db_tables method to get the name of the table containing user data.
table_list = dc.list_db_tables()

# Use the read_rds_table method to extract the table containing user data and return a pandas DataFrame.
df = de.read_rds_table(dc.init_db_engine(), table_list[1])

clean.clean_user_data(df)

dc.upload_to_db(df, 'dim_users')  # for testing datacleaning

pdf_link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
pdf_df = de.retrieve_pdf_data(pdf_link)

# print(len(pdf_df))
pdf_df = clean.clean_card_data(pdf_df)

dc.upload_to_db(pdf_df, 'dim_card_details')
