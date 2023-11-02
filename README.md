# Multinational Retail Data Centralisation

# A description of the project:

The scenario for this project is the following: a multinational company sells various goods across the globe.

**Problem**: their sales data is spread across many different data sources making it not easily accessible or analysable by current members of the team.
**Solution**: In an effort to become more data-driven, they would like to make its sales data accessible from one centralised location.

We wish to produce a system that stores the current company data in a database so that it's accessed from one centralised location and acts as a single source of truth for sales data.

The project consists of three milestones:

- Milestone 2: store the current company data in a database so that it's accessed from one centralised location and acts as a single source of truth for sales data.

  - Extract the data from a multitude of sources in json, csv, pdf formats on hosted on different platforms.
  - Clean the data using pandas.
  - Store the data in a local postgres database using sqlalchemy

- Milestone 3: develop a star-based schema of the database, ensuring the columns are of the correct data types

  - properly cast the individual tables to the correct datatypes, performing further cleaning if required
  - create primary and foreign keys for the tables

- Milestone 4: querying the data
  - we write queries of the database to answer business questions that a centralised database can answer.

# Milestone 2

We wish to bring together all the data into one place, clean this data and then store it in a local database. For each of these tasks a class has been written:

- DatabaseConnector is used to connect with and upload data to the database.
- DataExtractor works as a utility class, and contains methods for extracting data from different sources.
- DataCleaning contains methods for cleaning the data.

Our general method was to first extract the data, and develop methods in the DataExtractor class for this.

Once data was extracted we would engage in cleaning. We looked at the following issues: Null Values, Dates, garbage data, additional characters added erroneously to fields.

## Connecting to the local and remote database using DatabaseConnector

A postgres database, 'sales_data' was created locally. The relevant login details for this are stored in a YAML file in the /info directory as postgresdb_creds.yaml. Another file for RDS credentials is also stored locally as info/db.creds.yaml. These files are in the .gitignore file and must be present for the project to work. A dummy version of these is supplied for reference.

When DatabaseConnector is instantiated, a sqlalchemy engine is created as self.engine in the **init** method. This means we only create one engine per session, rather than making multiple connections.

The following methods were written:

    __read_db_creds(self): reads the credentials of the remote RDS database from a file, db_creds.yaml. This is a private method.
    _init_db_engine(self): initialised a sqlalchemy engine connection to the RDS database. This is a protected method.
    list_db_tables(self): lists the table names of the RDS database
    upload_to_db(self, df, table_name): uploads a table to the postgres database using the self.engine

## Extracting data from the various sources

We use the DataExtraction class to extract data from different data sources. The methods extract data from a particular data source, these sources will include CSV and json files from a wesbite that requires an api authentication, from a S3 bucket, and parsing a pdf file. Each of the main extraction methods returns a dataframe of the data which will then be cleaned.

    read_rds_table(database_connector, table_name)': this reads a table from the RDS database and returns a dataframe. This is used to extract the user and orders tables of the database.

    retrieve_pdf_data(link): this reads in a pdf file, which we convert page by page using tabula-py package and the concatenate the results to return a dataframe.

    list_number_stores(stores_endpoint, header): returns an integer of the number of stores using an api-key in the header
    retrieve_stores_data(stores_endpoint, header): this retrieves data from each store which is concatenated into dataframe. We hard coded the number here, although this might be changed to included the output of list_number_stores().

    extract_from_s3: uses boto package to download a .csv file of product values.from a public S3 bucket. We use the system to store this temporarily in the /temp folder and the read this to convert to a dataframe.

    extract_date_events(header): downloads a .json file using the api-key and returns a dataframe

## Cleaning data and uploading to the database

The main.py file creates instances of the DataExtraction and DatabaseConnector classes. We use these with an instantiation of the DataCleaning class to perform the cleaning of the data for uploading to our local postgres database.

The process of cleaning was performed by exploring the data to see where cleaning might need to occur. We began exploration by looking for unique values, which would inform our future explorations. For example:

    df.COLUMNAME.nunique() # gives the number of unique values
    df.COLUMNAME.unique() # shows all the unique values

Obviously, this varied depending on what kind of data we were approaching. But it would tell us if categorical data was consistent. For example, the 'GB' value in country code had some extra letters which we identified using this sifting process. Or we would identify random letters inserted in numbers. An example of this is how we performed the cleaning for the staff_numbers column in the store details:

    df.staff_numbers = df.staff_numbers.str.replace(
            '[a-zA-Z]', '', regex=True)

However, if we needed further work we explored a table with a temporary internal function which we used to sift through the data. This survices in the clean_orders_table method, but was used on all the tables and then deleted once we had produced the required results:

    def clean_explore(regex, column):
            mask = df[column].str.contains(
                regex, regex=True, na=False)
            print(column, " : ", df[column][~mask].count(), df[column][~mask])

Effectively we created a mask using a supplied regular expression for the type of data we were looking for. So, for example, if the column was to be numbers only, a regular expression of the kind: [\d]+ would be used, which would then create a mask we could then use to print where the values didn't adhere to this format.

We would try a df.info() to see what we needed to apply the code at each stage. Here is an example from the card details dataframe:

         0   level_0            120123 non-null  int64
         1   index             120123 non-null  int64
         2   date_uuid         120123 non-null  object ✅
         3   user_uuid         120123 non-null  object ✅
         4   card_number       120123 non-null  int64 ✅ (they are all int64 anyways - should convert other credit card columns to this format)
         5   store_code        120123 non-null  object ✅
         6   product_code      120123 non-null  object ✅
         7   product_quantity  120123 non-null  int64 ✅

We would then devise methods to clean based on the results of this exploration.

Nulls values were dealt with by reassigning the dataframe to exclude rows which included NULL viz.

    df = df[df.COLUMN_NAME != 'NULL']

There were also a number of rows in each table which contained garbage values. These were removed on each table by finding a regularly formatted column and identifying these with a mask, as per the 'clean_explore()' process.

I appear to get errors from my use of this, so I am sure there is a better way to do it. However, it does have the intended effect, and so it remains until a better solution is offered.

For dates, we also explored with masks. Here is an example of the output we got which informed the creation of the static `process_date` method.

For example, using a mask we discovered the following non-standard dates:

        # 360       1968 October 16  Year Month DD      1  %Y %B #d
        # 1629      January 1951 27  Month Year DD      2
        # 1996     November 1958 11  Month Year DD      2  %B %Y #d
        # 3066      1946 October 18  Year Month DD      1
        # ????           2012/10/21  Year/Month/DD      3  %Y/%m/%d

This then informed the processing which occurs to convert these three formats. The crux of this is a `dt.strptime` function which extracts the information in these non standard formats and returns an appropriate object eg.

    formatted_date = dt.strptime(
                    unusual_date, '%Y/%m/%d')

If a date does not conform, probably because it is not a date, we use the numpy NaN value to insert into the column.

In the case of the products table, we needed to perform a conversion of values into all metric kg. This involves converting amounts from g, ml and oz and returning a unified dataframe.

Once we had performed exploration and were satisfied with the data in a column, we would cast it as a type. For example here is the cleaning and casting of the continent column in 'clean_store_data'

        df.continent = df.continent.str.replace('ee', '')
        df.continent = df.continent.astype('string')

# Milestone 3

This is where a star-based schema of the database was created. This involved the casting of types, and the creation of limited VARCHARs.

We used the following code:

    SELECT max(length(CAST(card_number AS VARCHAR))) FROM orders_table;

This ensures that the column is considered to be a string, which is essential for the length() function to work. A variant of this was used for all calculations.

To alter the datatypes we used the following:

     ALTER TABLE orders_table
     ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID

In the case of certain types we needed to make the case explicit with USING, such as dates, floats and uuids.

In order to make a column nullable, we used the following:

     ALTER COLUMN store_type DROP NOT NULL

To create a new column based on values from other columns was a two pronged affair. First we needed to create the column and then we used a CASE statement that would feed into the SET argument of UPDATE, thus:

     UPDATE dim_products
     SET weight_class =
        CASE
            WHEN
                weight < 2 THEN 'Light'
            WHEN
                weight >= 2 AND weight < 40 THEN 'Mid_Sized'
            WHEN
                weight >= 40 AND weight < 140 THEN 'Heavy'
            WHEN
                weight > 120 THEN 'Long'
        END;

Initially I had thought we could insert this using INSERT, but this appeared to cause issues with the database and often resulted in crashing. Not sure why.

During the process of ensuring the integrity of the new datatypes, I realised there were some problems with my cleaning code from Milestone 2. We refactored accordingly - this affected the dim_users table.

To create a BOOL type, we needed to ensure that the table included True or False values instead of the strings. A silly but useful issue was that there was a spelling mistake in one of the strings, which I had not picked up on, and which caused much concern until I realised that I had been looking for the incorrect string. A useful reminder to actually cut and paste the actual string rather than assuming it to be what you think it is.

    UPDATE
        dim_products
    SET
        removed = REPLACE(removed, 'Still_avaliable', 'True');

Note that it is "avaliable" not "available".

TODO: In Milestone 2, we created a date field for the dim_date_times. I now realise that this will remove the time information, so we need to revisit this and make sure that a separate time column exists.

We then created primary keys in each of the tables that are references in orders_table, namely:

    ALTER TABLE dim_date_times ADD PRIMARY KEY (date_uuid);
    ALTER TABLE dim_users ADD PRIMARY KEY (user_uuid);
    ALTER TABLE dim_card_details ADD PRIMARY KEY (card_number);
    ALTER TABLE dim_stores_details ADD PRIMARY KEY (store_code);
    ALTER TABLE dim_products ADD PRIMARY KEY (product_code);

And then ensured these were foreign keys in their respective tables. This is an example of the template we used:

    ALTER TABLE orders_table
    ADD CONSTRAINT fk_orders_table_dim_date_times
    FOREIGN KEY (date_uuid)
    REFERENCES dim_date_times(date_uuid);

We haven't added an ON DELETE CASCADE option yet. We may do this in the future.

## Milestone 4

We deal with the answers to the in the milestone4.ipynb. This requires a connection to the database to be set up in the editor you use to read the markup file.

# File structure of the project

There are four folders: classes, notebooks, info and temp and a main.py file to run everything.

- /classes contains:

  - data_cleaning.py
  - data_extraction.py
  - database_utils.py

- /notebooks contains the work for Milestone 3 and Mileston 4

  - milestone3.ipynb
  - milestone4.ipynb

- /info contains .yaml credentials files. Make sure you rename the files with \_DUMMY at the end with the correct details.

  - api_key.json
  - postgresdb_creds.yaml
  - db_creds.yaml

- temp contains any temporary files that are downloaded during the course of the project

# Installation instructions

You need to have sqlalchemy, pyyaml, pandas, boto3 installed. Use pip install <package> if you don't have these.

You need to have a postgresql database set up, called "sales_data". The details for this should be in the postgresdb_cred.yaml file.

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

# Usage instructions

From the main downloaded directory:

    python main.py

Will run the data extraction, data cleaning and database creation.

For the star-based schema and SQL processing, this can be found in the `milestone3.ipynb` file.
For the SQL queries for the business case, this can be found in the `milestone4.ipynb`.
