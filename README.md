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

For example, using a mask we discovered the following non-standard dates:

        # 360       1968 October 16  Year Month DD      1  %Y %B #d
        # 1629      January 1951 27  Month Year DD      2
        # 1996     November 1958 11  Month Year DD      2  %B %Y #d
        # 3066      1946 October 18  Year Month DD      1
        # 4205     1979 February 01  Year Month DD      1
        # 5350         June 1943 28  Month Year DD      2
        # 5423     November 1963 06  Month Year DD      2
        # 6108     February 2005 05  Month Year DD      2
        # 6221         July 1966 08  Month Year DD      2
        # 7259      1948 October 24  Year Month DD      1
        # 8117     December 1946 09  Month Year DD      2
        # 9934      2005 January 27  Year Month DD      1
        # 10245        July 1961 14  Month Year DD      2
        # 11203        July 1939 16  Month Year DD      2
        # 13045     1951 January 14  Year Month DD      1
        # 14546         May 1996 25  Month Year DD      2

DatabaseConnector.upload_to_db(dataframe, table_name) takes in a dataframe and a table name and creates a table in the local postgreSQL database as 'dim_users'.

# Task 4

DataExtractor.retrieve_pdf_data(external_link): Using the tabula-py python package, a pdf file was extracted from an external link. A dataframe is returned.
DataCleaning.clean_card_data(dataframe) cleans the data from the pdf. Again, null values were removed, dates were put into a uniform format and other issues were dealt with. The work on the previous data cleaning task informed this one.
The data is then uploaded to the local database as 'dim_card_details'.

# Task 5

In this task, we needed the use of an api key. This was used in the headers for the requests we made.

DataExtractor.list_number_of_stores() retrieves the number of stores the company has
DataExtractor.retrieve_stores_data takes a supplied endpoint and concatenates data from each store into a dataframe, which is returns.
DataCleaning.clean_store_data() cleans the data from the supplied dataframe and returns a cleaned dataframe. Once again, this deals with issues with dates, null values and other elements.
The data is then uploaded to the local database as 'dim_store_details'

# Task 6

In this task we extract product details from an S3 bucket on AWS>

DataExtractor.extract_from_s3 uses the boto3 package to download and extract the information from an s3 address and returns a pandas dataframe.
DataCleaning.convert_product_weights normalises the quantities in the weights column so they are all in kg. This involves converting amounts from g, ml and oz and returning a unified dataframe.
DataCleaning.clean_products_data performs other cleaning of the data.
This is then uploaded as 'dim_products'.

# Task 7

We retrieve the product orders database by using DatabaseConnect.read_rds_table() after obtaining the right file name from DatabaseConnect.list_db_tables, and return a dataframe.
DataCleaning.clean_orders_data() removes columns that are not necessary and performs other cleaning on the dataframe. This is then uploaded to the database as 'orders_table'.

A temporary function, clean_explore used a regex we defined to sift a column. We went through each column one by one

    def clean_explore(regex, column):
            mask = df[column].str.contains(
                regex, regex=True, na=False)
            print(column, " : ", df[column][~mask].count(), df[column][~mask])

         We would try a df.info() to see what we needed to apply the code to:

         0   level_0            120123 non-null  int64
         1   index             120123 non-null  int64
         2   date_uuid         120123 non-null  object ✅
         3   user_uuid         120123 non-null  object ✅
         4   card_number       120123 non-null  int64 ✅ (they are all int64 anyways - should convert other credit card columns to this format)
         5   store_code        120123 non-null  object ✅
         6   product_code      120123 non-null  object ✅
         7   product_quantity  120123 non-null  int64 ✅

     mask = df.user_uuid.str.contains(
         '\w{8}-\w{4}-\w{4}-\w{4}-\w{12}', regex=True, na=False)
    print('user_uuid ', df.user_uuid[~mask].count(), df.user_uuid[~mask])

    clean_explore('[a-zA-Z]+', 'card_number')
    clean_explore('^[A-Z]+-\w{8}$', 'store_code')
    clean_explore('\w{2}-\d+\w{1}', 'product_code')
    print(df.product_quantity.min(), df.product_quantity.max())
    print(df.level_0.min(), df.level_0.max())
    print(df.index.min(), df.index.max())

# Task 8

The data events table was obtained by downloading a json file from a public S3 bucket with DataExtraction.extract_date_events(). We then clean this by creating a new column just called date that amalgamates the other columns in the table. We then return this for uploading to the local database as 'dim_date_times'.

# Milestone 3

This is where a star-based schema of the database was created. This involved the casting of types, and the creation of limited VARCHARs.

We used the following code:

> SELECT max(length(CAST(card_number AS VARCHAR))) FROM orders_table;

This ensures that the column is considered to be a string, which is essential for the length() function to work. A variant of this was used for all calculations.

To alter the datatypes we used the following:

> ALTER TABLE orders_table
> ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID

In the case of certain types we needed to make the case explicit with USING, such as dates, floats and uuids.

In order to make a column nullable, we used the following:

> ALTER COLUMN store_type DROP NOT NULL

To create a new column based on values from other columns was a two pronged affair. First we needed to create the column and then we used a CASE statement that would feed into the SET argument of UPDATE, thus:

> UPDATE dim_products
> SET weight_class =
> CASE
> WHEN
> weight < 2 THEN 'Light'
> WHEN  
>  weight >= 2 AND weight < 40 THEN 'Mid_Sized'
> WHEN  
>  weight >= 40 AND weight < 140 THEN 'Heavy'
> WHEN
> weight > 120 THEN 'Long'
> END;

Initially I had thought we could insert this using INSERT, but this appeared to cause issues with the database and often resulted in crashing. Not sure why.

During the process of ensuring the integrity of the new datatypes, I realised there were some problems with my cleaning code from Milestone 2. We refactored accordingly - this affected the dim_users table.

To create a BOOL type, we needed to ensure that the table included True or False values instead of the strings. A silly but useful issue was that there was a spelling mistake in one of the strings, which I had not picked up on, and which caused much concern until I realised that I had been looking for the incorrect string. A useful reminder to actually cut and paste the actual string rather than assuming it to be what you think it is.

> UPDATE
> dim_products
> SET
> removed = REPLACE(removed, 'Still_avaliable', 'True');

Note that it is "avaliable" not "available".

TODO: In Milestone 2, we created a date field for the dim_date_times. I now realise that this will remove the time information, so we need to revisit this and make sure that a separate time column exists.

We then created primary keys in each of the tables that are references in orders_table, namely:

> ALTER TABLE dim_date_times ADD PRIMARY KEY (date_uuid);
> ALTER TABLE dim_users ADD PRIMARY KEY (user_uuid);
> ALTER TABLE dim_card_details ADD PRIMARY KEY (card_number);
> ALTER TABLE dim_stores_details ADD PRIMARY KEY (store_code);
> ALTER TABLE dim_products ADD PRIMARY KEY (product_code);

And then ensured these were foreign keys in their respective tables. This is an example of the template we used:

> ALTER TABLE orders_table
> ADD CONSTRAINT fk_orders_table_dim_date_times
> FOREIGN KEY (date_uuid)
> REFERENCES dim_date_times(date_uuid);

We haven't added an ON DELETE CASCADE option yet. We may do this in the future.

# File structure of the project

There are four folders: classes, notebooks, info and temp and a main.py file to run everything.

- classes contains: data_cleaning.py - data_extraction.py - database_utils.py

- notebooks contains the work for Milestone 3

- info contains .yaml credentials files

- temp contains any temporary files that are downloaded during the course of the project

# Installation instructions

You need to have sqlalchemy, pyyaml, pandas, boto3 installed. Use pip install <package> if you don't have these.

# Usage instructions

Currently you need to create an info directory. In this place a yaml file with your postgres details and your RDS database details in the following format:

db_creds.yaml:
RDS_HOST: data-handling-project-readonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com
RDS_PASSWORD: \***\*\*\*\*\*\*\***
RDS_USER: **\*\***\*\***\*\***
RDS_DATABASE: postgres
RDS_PORT: 5432

postgres_db_creds.yaml:
HOST: "127.0.0.1"
USER: "postgres"
PASSWORD: \***\*\*\*\*\***
DATABASE: "sales_data"
PORT: 5432

# License information

TBC
