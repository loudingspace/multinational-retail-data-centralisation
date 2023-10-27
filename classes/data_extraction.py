from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
from sqlalchemy import text, exc
import pandas as pd


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


##### Test #####
dc = DatabaseConnector()
de = DataExtraction()
clean = DataCleaning()

# Use your list_db_tables method to get the name of the table containing user data.
table_list = dc.list_db_tables()

# Use the read_rds_table method to extract the table containing user data and return a pandas DataFrame.
df = de.read_rds_table(dc.init_db_engine(), table_list[1])
# print(df.head(10))

# for n in [360, 697, 752, 1046, 1629, 1996, 2995, 3066, 3536, 3613, 3797, 4205,
#           4592,
#           5306,
#           5350,
#           5423,
#           5531,
#           6108,
#           6221,
#           6420,
#           7168,
#           7259,
#           8117,
#           8273,
#           8386,
#           8524,
#           9013,
#           9934,
#           10211,
#           10245,
#           10360,
#           11203,
#           11366,
#           12177,
#           13045,
#           13111,
#           13159,
#           14101,
#           14105,
#           14499,
#           14546,
#           15302]:
#     print(df.iloc[n])


clean.clean_user_data(df)

# print(df.date_of_birth[temp])
