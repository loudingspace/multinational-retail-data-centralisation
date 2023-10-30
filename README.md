# multinational-retail-data-centralisation

# Table of Contents, if the README file is long

# A description of the project:

The scenario for this project is the following: a multinational company sells various goods across the globe.

Problem: their sales data is spread across many different data sources making it not easily accessible or analysable by current members of the team.

Solution: In an effort to become more data-driven, they would like to make its sales data accessible from one centralised location.

Your first goal will be to produce a system that stores the current company data in a database so that it's accessed from one centralised location and acts as a single source of truth for sales data.

# Method for Milestone 2

A postgres database, 'sales_data' was created locally. The relevant details for this are stored in a YAML file in the /info directory (this is not in this repo). Three classes were created in the classes folder.

# Task 3

DataExtractor works as a utility class, and contains methods for extracting data from different sources.
DatabaseConnector is used to connect with and upload data to the database.
DataCleaning contains methods for cleaning the data.

DatabaseConnector.init_db_engine() reads the returned dictionary from read_db_creads and returns an sqlalchemy engine.
This engine is then used in the list_db_tables method which returns the names of tables that are stored in the RDS database.
DatabaseConnector.read_rds_table() extracts a dataframe from an RDS database. We use this to return a pandas dataframe of the user data.
DataCleaning.clean_user_data() performs cleaning of the user data. This includes looking for NULL values, making dates datetime64 objects, removing garbage values. The procedure for this was achieved after careful sifting of the information in the tables, including creating temporary functions to return unique values in a column, to mask regular columns such as dates with a regular expression to look for values that did not conform to the format, and creating a data processing method which returns a cleaned date column. This was then used in subsequent tasks.
DatabaseConnector.upload_to_db(dataframe, table_name) takes in a dataframe and a table name and creates a table in the local postgreSQL database with the table name.

# Task 4

DataExtractor.retrieve_pdf_data(external_link): Using the tabula-py python package, a pdf file was extracted from an external link. A dataframe is returned.
DataCleaning.clean_card_data(dataframe) cleans the data from the pdf. Again, null values were removed, dates were put into a uniform format and other issues were dealt with. The work on the previous data cleaning task informed this one.

# Task 5

## User Data

Historical data of users is stored on an AWS database. We created methods to extract the info from an RDS database. The method read_db_creds reads the credentials of the RDS file that are stored in the /info directory and returns a dictionary.

# File structure of the project

There are three folders: classes, info and temp and a main.py file to run everything.

- classes contains: data_cleaning.py - data_extraction.py - database_utils.py -

- info contains .yaml credentials files

- temp contains any temporary files that are downloaded during the course of the project

# Installation instructions

# Usage instructions

# License information
