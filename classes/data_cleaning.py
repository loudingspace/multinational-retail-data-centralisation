import pandas as pd
from dateutil.parser import parse
from datetime import datetime as dt
import numpy as np
import re
import math

# TODO: the clean_user_data() needs refactoring big time. But we will leave this to later as want to get the functionality there first, then refactor when things are operational
# TODO: don't need to return a df in clean_card_date
# TODO: remember to change the postgresdb_creds back after experimentation


class DataCleaning:
    '''
    This will have methods to clean data from each of the data sources
    '''

    def clean_date_events(self, df):
        df.info()

        df = df[df['timestamp'] != 'NULL']

        # remove garbage
        regex = '\d{2}:\d{2}:\d{2}'
        mask = df.timestamp.str.contains(regex, na=False, regex=True)
        print(df.timestamp[~mask], '\nCount: ', df.timestamp[~mask].count())
        df = df[mask]  # remove garbage values
        print(df.timestamp[~mask], '\nCount: ', df.timestamp[~mask].count())

        # want to consolidate the date info into one object.
        df.timestamp = df.timestamp.astype('string')
        df[['h', 'm', 's']] = df.timestamp.str.extract(
            '(\d{2}):(\d{2}):(\d{2})')
        # https://pandas.pydata.org/docs/reference/api/pandas.to_datetime.html
        df['date'] = pd.to_datetime({
            'year': df.year,
            'month': df.month,
            'day': df.day,
            # 'minute': df.timestamp.str.split(':')[0]
            'hour': df.h,
            # df.timestamp.str.extract('(\d{2}):\d{2}:\d{2}').to_frame(),
            'minute': df.m,
            'second': df.s
        })

        df.drop(['month', 'year', 'day',
                'h', 's', 'm'], axis=1, inplace=True)

        df.date_uuid = df.date_uuid.astype('string')
        df.time_period = df.time_period.astype('string')

        return df

    def clean_orders_table(self, df):
        ''' Cleans the orders database stored in the RDS database

        Argument: df

        Returns: df
        '''

        df.drop(['first_name', 'last_name', '1'], axis=1, inplace=True)
        df = df[df.level_0 != 'NULL']

        def clean_explore(regex, column):
            mask = df[column].str.contains(
                regex, regex=True, na=False)
            print(column, " : ", df[column][~mask].count(), df[column][~mask])

        #  i've been dropping the indexes, and as it appear index and level_0 are the same, I'm going to drop these too
        df.drop(columns=['index', 'level_0'], inplace=True)

        # i'm not sure I need to return this as it references the same object in memory I then use later...
        return df

    def process_date(df, column):

        df.reset_index(inplace=True)  # THINK THE INDEX IS ISSUE

        date_regex = '^\d{4}\-(0[1-9]|1[012])\-(0[1-9]|[12][0-9]|3[01])$'
        YYYYMONTHDATE = '\d{4}\s[a-zA-Z]+\s\d{2}'  # 2012 October 21
        YYYYMMDD = '\d{4}\/\d{2}\/\d{2}'  # 2012/10/21
        MONTHYEARDATE = '[a-zA-Z]+\s\d{4}\s\d{2}'  # October 2012 21

        regular_dates = df[column].str.contains(date_regex, na=False)

        for index, unusual_date in df[column][~regular_dates].items():
            # 1968 October 16 \d{4}\s[a-zA-Z]+\s\d{2}
            if isinstance(unusual_date, float):
                continue

            if re.search(YYYYMONTHDATE, unusual_date):
                formatted_date = dt.strptime(
                    unusual_date, '%Y %B %d')

            # 1971/10/23
            elif re.search(YYYYMMDD, unusual_date):
                formatted_date = dt.strptime(
                    unusual_date, '%Y/%m/%d')

            # January 1951 27
            elif re.search(MONTHYEARDATE, unusual_date):
                formatted_date = dt.strptime(
                    unusual_date, '%B %Y %d')

            else:
                formatted_date = np.nan

            df.loc[[index], [column]] = formatted_date

        # i changed orded of mask and column for the following 2 lines:
        df[regular_dates][column] = df[regular_dates][column].apply(
            parse)

        df[regular_dates][column] = pd.to_datetime(
            df[regular_dates][column], errors='coerce')

        df[column] = df[column].astype(
            'datetime64[ns]')  #  why do i need to do this???

        return df

    def clean_store_data(self, df):
        '''cleans the data retrieve from the API and returns a pandas DataFrame.

        Parameters: dataframe

        Returns: dataframe
        '''

        # print(df.isna().values.sum())
        df = df[df.opening_date != 'NULL']

        # print(df.info())
        # print(df.head())

        # print(df.lat[~df.lat.isna()])

        # clean up addresses by replacing \n for ', '
        df.address = df.address.str.replace('\n', ', ')
        df.address = df.address.astype('string')

        # longitude - replace None with null values

        # print(df.longitude.unique())  # these need to be NN.NN+
        # I think store code is a better on for dispensing of garbage values, as we get rid of a real value here
        # mask = df.longitude.str.contains('\d{2}\.\d{1}', na=False)
        # print(df[~mask])

        mask = df.store_code.str.contains(
            '[A-Z]+-\w{7}\w+', na=False, regex=True)
        # print(df[~mask])

        df = df[mask]  # is this where we dispense of the garbage values?
        df.reset_index(inplace=True)

        # print(df[~mask])

        '''
        # let's not convert these as this might make concatenation easier for the Milestone 3 tasks
        df.longitude = df.longitude.astype('float')
        df.latitude = df.latitude.astype('float')
        '''
        # print(df.info())
        print(df.head())
        # mask = df.longitude.str.contains('[N][/][A]', regex=True)
        # print(mask)

        # this will replace NaN will None I hope
        df.longitude = df.longitude.where(pd.notnull(df.longitude), None)

        print(df.head())

        # print(df.lat[~df.lat.isna()])

        # let's drop the column Lat as it is empty
        df.drop(['lat'], axis=1, inplace=True)

        # check any abnormalities with locality
        df.locality = df.locality.astype('string')

        # check store type
        # print(df.store_type.unique()) # seems ok, there are 4 categories

        # check for any abnormalities with store code
        column = 'store_code'  # this should be a function as I use this a lot
        regex = '^[A-Z]+-\w{8}$'
        mask = df[column].str.contains(regex, na=False)

        # print('The number of non-compliant store codes is ',
        #      len(df[column][~mask]))
        df.store_code = df.store_code.astype('string')

        # make sure staff numbers are all numbers - they aren't!!
        # df.staff_numbers = df.staff_numbers.astype('int16')
        # these need to be NN.NN+
        # print('STAFF NUMBERS', df.staff_numbers.unique())
        # mask = df.staff_numbers.str.contains('\d+', na=False)
        # print(df[mask])
        # df = df[mask]
        # There appear to be random letters inserted in the numbers. Let's assume these need removing so we can have a proper number
        df.staff_numbers = df.staff_numbers.str.replace(
            '[a-zA-Z]', '', regex=True)
        df.staff_numbers = df.staff_numbers.astype('int16')

        # process the opening date
        DataCleaning.process_date(df, 'opening_date')

        df.store_type = df.store_type.astype('string')

        # print(df.country_code.unique()) - there are three country codes
        # print(df.continent.unique()) ## there is a mistake in the continents with the addition of 'ee'. Let's replace that

        df.country_code = df.country_code.astype('string')
        df.continent = df.continent.str.replace('ee', '')
        df.continent = df.continent.astype('string')

        df.drop(columns=['index'], inplace=True)
        return df

    def clean_card_data(self, df):
        print(df.iloc[50:70, 0:1])

        df = df[df.card_number != 'NULL']  # remove null values
        df = DataCleaning.process_date(df, 'date_payment_confirmed')

        column = 'expiry_date'
        date_regex = '^\d{2}\/\d{2}$'
        regular_dates = df[column].str.contains(date_regex)

        df[column] = df[column][regular_dates]

        # Try using .loc[row_indexer,col_indexer] = value instead
        df.drop(df[column][~regular_dates].index, inplace=True)

        # now convert the dates

        df[column] = pd.to_datetime(
            df[column], format='%d/%y', errors='ignore')

        # check consistency in card_provider

        print(df.card_provider.unique())  # seems to be consistent.
        df.card_provider = df.card_provider.astype(
            'string')  # convert to string

        # check for consistency in card number

        # are there any non numbers in this?
        number_regex = '[^0-9]'
        mask = df['card_number'].str.contains(
            number_regex, regex=True, na=False)

        df.card_number[mask] = df.card_number[mask].str.replace(
            '\?', '', regex=True)  # need to escape a ?

        df.drop(columns=['index'], inplace=True)
        return df

    def convert_product_weight(self, df):
        ''' Converts the weights column to a decimal value representing their weight in kg. Use a 1:1 ratio of ml to g as a rough estimate for the rows containing ml.

        Parameter: dataframe

        Returns: dataframe
        '''

        def process_product_weight(regex, lambda_formula):  # internal function
            mask = df['weight'].str.contains(
                regex, regex=True, na=False)
            df.weight[mask] = df['weight'][mask].apply(lambda_formula)
            print(df.weight[mask])  # for debugging

        # kg
        process_product_weight(
            '(\d+)[k][g]', lambda_formula=lambda x: x.split('kg')[0])

        # ml
        process_product_weight(
            '(\d+)[m][l]', lambda x: float(x.split('ml')[0]) / 1000)

        # n x g
        process_product_weight('\d+\s[x]\s\d+[g]', lambda x: float(
            float(x.split(' x ')[0]) * float(x.split(' x ')[1].split('g')[0])) / 1000)

        # g
        process_product_weight(
            '\d+[g]', lambda x: float(x.split('g')[0]) / 1000)

        # oz 1 is 0.02834952kg
        process_product_weight(
            '\d+[o][z]', lambda x: (float(x.split('oz')[0]) * 0.02834952))

        # finally, the junk. Let's convert those to nan, then convert whole lot to float
        process_product_weight('[A-Z]', lambda x: np.nan)

        df.weight = df.weight.astype(float)

        return df

    def clean_products_data(self, df):
        '''Clean remaining columns of products df

        Argument: dataframe

        Returns: dataframe

        '''

        # remove nulls
        df = df[df.product_name != 'NULL']

        for column in df.columns:
            print(column, df[column].nunique())

        # product_name 1021
        print(df.product_name.unique())
        # 828 values are duplicated but with different added dates
        print(df.product_name[df.product_name.duplicated()].count())
        print(df[df.product_name.duplicated()].head(30))
        print(df.product_name.groupby(
            df.product_name[df.product_name.duplicated()]).count())  # The date added values are different, as well as the uuid, so going to leave in for now

        # product_price 132
        # get rid of the £ signs. But delete any values that don't have pound signs beforehand!
        print(df.product_price.unique())

        regex = '[£]\d+.\d+'
        mask = df.product_price.str.contains(regex, regex=True, na=False)
        df = df[mask]  # this drops the garbage data rows

        df.product_price = df.product_price.apply(
            lambda x: float(x.split('£')[1]))

        df = DataCleaning.process_date(df, 'date_added')
        df.category = df.category.astype('string')
        df.removed = df.removed.astype('string')

        df.drop(columns=['index'], inplace=True)
        return df

    def clean_user_data(self, df):
        '''performs the cleaning of the user data.

            Parameter: dataframe: panda dataframe

            Returns: panda dataframe

        Looks out for NULL values, 
        errors with dates, 
        incorrectly typed values and 
        rows filled with the wrong information.
        '''

        # TODO use process_date in this method

        for column in df.columns:
            index = df[column][df[column] == 'NULL'].index

        for row in index:
            df.drop(row, inplace=True)  # should replace this with df = df[df]

        df.first_name = df.first_name.astype('string')
        df.last_name = df.last_name.astype('string')
        df.company = df.company.astype('string')
        df.email_address = df.email_address.astype('string')
        df.address = df.address.str.replace('\n', ', ')
        df.address = df.address.astype('string')
        df.country = df.country.astype('string')
        df.country_code = df.country_code.astype('string')

        # replace country code as GGB which should be GB
        df.country_code.replace(['GGB'], 'GB', inplace=True)

        mask = df['country_code'].isin(['GB', 'US', 'DE'])
        # df = df[mask]  # should get rid of the garbage

        # this removes all garbage values as they are consistent across columns
        df = df[mask]

        print(df[~mask])
        print(df.iloc[750:755])

        date_regex = '^\d{4}\-(0[1-9]|1[012])\-(0[1-9]|[12][0-9]|3[01])$'
        YYYYMONTHDATE = '\d{4}\s[a-zA-Z]+\s\d{2}'
        YYYYMMDD = '\d{4}\/\d{2}\/\d{2}'
        MONTHYEARDATE = '[a-zA-Z]+\s\d{4}\s\d{2}'

        # TODO: refactor all of the date stuff to use the

        # join_date

        regular_dates = df.join_date.str.contains(date_regex)
        # print(df.join_date[~regular_dates])

        for index, unusual_date in df.join_date[~regular_dates].items():
            # 1968 October 16 \d{4}\s[a-zA-Z]+\s\d{2}
            if re.search(YYYYMONTHDATE, unusual_date):
                formatted_date = pd.to_datetime(
                    unusual_date, format='%Y %B %d', errors='coerce')

                formatted_date = dt.strptime(
                    unusual_date, '%Y %B %d')

            # 1971/10/23
            elif re.search(YYYYMMDD, unusual_date):
                formatted_date = pd.to_datetime(
                    unusual_date, format='%Y/%m/%d', errors='coerce')

                formatted_date = dt.strptime(
                    unusual_date, '%Y/%m/%d')

            # January 1951 27
            elif re.search(MONTHYEARDATE, unusual_date):
                formatted_date = pd.to_datetime(
                    unusual_date, format='%B %Y %d', errors='coerce')

                formatted_date = dt.strptime(
                    unusual_date, '%B %Y %d')

            else:
                formatted_date = np.nan

            df.loc[[index], ['join_date']] = formatted_date

        df.join_date[regular_dates] = df.join_date[regular_dates].apply(
            parse)
        df.join_date[regular_dates] = pd.to_datetime(
            df.join_date[regular_dates], infer_datetime_format=True, errors='coerce')

        df.join_date = df.join_date.astype(
            'datetime64[ns]')  #  why do i need to do this???

        # date_of_birth
        regular_dates = df.date_of_birth.str.contains(date_regex)

        # https://pandas.pydata.org/pandas-docs/version/1.5/reference/api/pandas.Series.items.html#pandas.Series.items
        for index, unusual_date in df.date_of_birth[~regular_dates].items():
            # 1968 October 16 \d{4}\s[a-zA-Z]+\s\d{2}
            if re.search(YYYYMONTHDATE, unusual_date):
                formatted_date = pd.to_datetime(
                    unusual_date, format='%Y %B %d', errors='coerce')

                formatted_date = dt.strptime(
                    unusual_date, '%Y %B %d')

            # 1971/10/23
            elif re.search(YYYYMMDD, unusual_date):
                formatted_date = pd.to_datetime(
                    unusual_date, format='%Y/%m/%d', errors='coerce')

                formatted_date = dt.strptime(
                    unusual_date, '%Y/%m/%d')

            # January 1951 27
            elif re.search(MONTHYEARDATE, unusual_date):
                formatted_date = pd.to_datetime(
                    unusual_date, format='%B %Y %d', errors='coerce')

                formatted_date = dt.strptime(
                    unusual_date, '%B %Y %d')

            else:
                formatted_date = np.nan

            df.loc[[index], ['date_of_birth']] = formatted_date

        df.date_of_birth[regular_dates] = df.date_of_birth[regular_dates].apply(
            parse)
        df.date_of_birth[regular_dates] = pd.to_datetime(
            df.date_of_birth[regular_dates], infer_datetime_format=True, errors='coerce')

        df.date_of_birth = df.date_of_birth.astype(
            'datetime64[ns]')  #  why do i need to do this???

        df.drop(columns=['index'], inplace=True)

        return df

        '''
        
        TODO: Phone Cleaning
        # Our regular expression to match THIS IS FOR BRITISH NUMBERS
        regex_expression = '^(?:(?:\(?(?:0(?:0|11)\)?[\s-]?\(?|\+)44+\)?[\s-]?(?:\(?0\)?[\s-]?)?)|(?:\(?0))(?:(?:\d{5}\)?[\s-]?\d{4,5})|(?:\d{4}\)?[\s-]?(?:\d{5}|\d{3}[\s-]?\d{3}))|(?:\d{3}\)?[\s-]?\d{3}[\s-]?\d{3,4})|(?:\d{2}\)?[\s-]?\d{4}[\s-]?\d{4}))(?:[\s-]?(?:x|ext\.?|\#)\d{3,4})?$'
        # For every row  where the Phone column does not match our regular expression, replace the value with NaN
        df.loc[~df['phone_number'].str.match(
            regex_expression), 'phone_number'] = np.nan
        # df.Phone = df.Phone.astype('string')
        
        '''
