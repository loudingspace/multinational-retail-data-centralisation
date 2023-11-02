import pandas as pd
from classes.data_cleaning import DataCleaning
from classes.data_extraction import DataExtraction
from classes.database_utils import DatabaseConnector
# is there a way to do this in one line?

dc = DatabaseConnector()
de = DataExtraction()
clean = DataCleaning()


'''

# Task 3
# Use your list_db_tables method to get the name of the table containing user data.
table_list = dc.list_db_tables()
print(table_list)


# Use the read_rds_table method to extract the table containing user data and return a pandas DataFrame.
df = de.read_rds_table(dc.init_db_engine(), table_list[1])
# strangely we don't need to return this. It just keeps it in memory.
df = clean.clean_user_data(df)
dc.upload_to_db(df, 'dim_users')


# Temp change to sort users table
# Task 4
pdf_link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
pdf_df = de.retrieve_pdf_data(pdf_link)
pdf_df = clean.clean_card_data(pdf_df)
dc.upload_to_db(pdf_df, 'dim_card_details')
'''

# Task 5
# should probably put this in a file somewhere
# should read this from a file
# api_dict = {"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
# a = de.list_number_stores(
#     "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores", api_dict)

# # stores_df = de.retrieve_stores_data(
# #     'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details', api_dict)

# # stored the df temporarily so we can clean the data without having to wait
# stores_df = pd.read_csv('./temp/stores.csv')
# stores_df = clean.clean_store_data(stores_df)
# print(stores_df.head())
# dc.upload_to_db(stores_df, 'dim_stores_details')


# # Task6
# products_df = de.extract_from_s3()
# products_df = clean.convert_product_weight(products_df)
# products_df = clean.clean_products_data(products_df)
# dc.upload_to_db(products_df, 'dim_products')

'''
# Task 7
table_list = dc.list_db_tables()
print(table_list)
order_df = de.read_rds_table(dc.init_db_engine(), table_list[2])
print(order_df.info())
order_df = clean.clean_orders_table(order_df)
dc.upload_to_db(order_df, 'orders_table')

'''
# # Task 8
# date_events_df = de.extract_date_events(api_dict)
# date_events_df = clean.clean_date_events(date_events_df)
# dc.upload_to_db(date_events_df, 'dim_date_times')

# TODO: I am going to have to add a column to the dim_date with the timestamp
