import pandas as pd
from dateutil.parser import parse
from datetime import datetime as dt
import numpy as np
import re
import math

# TODO: the clean_user_data() needs refactoring big time. But we will leave this to later as want to get the functionality there first, then refactor when things are operational
# TODO: don't need to return a df in clean_card_date


class DataCleaning:
    '''
    This will have methods to clean data from each of the data sources
    '''

    def clean_date_events(self, df):

        # remove nulls

        df.info()

        df = df[df['timestamp'] != 'NULL']

        # remove garbage

        regex = '\d{2}:\d{2}:\d{2}'
        mask = df.timestamp.str.contains(regex, na=False, regex=True)
        print(df.timestamp[~mask], '\nCount: ', df.timestamp[~mask].count())
        df = df[mask]  # remove garbage values
        print(df.timestamp[~mask], '\nCount: ', df.timestamp[~mask].count())

        # ok, so I think we want to create a dateobject from the date values here
        # https://www.programiz.com/python-programming/datetime#google_vignette
        # https://www.programiz.com/python-programming/datetime/strftime

        # df['test'] = dt.strptime(
        #     f"{df['timestamp']} {df['month']} {df['year']} {df['day']}", "%H:%M:%S %m %Y %d")

        # TODO: need to use .apply

        # df['test'] = f"{df['timestamp']} {df['month']} {df['year']} {df['day']}".apply(
        #     lambda x: x)

        # let's see if converting to string type might help us here
        df.timestamp = df.timestamp.astype('string')

        # For some reason I can't do this is one go
        # df['hour'] = df.timestamp.str.extract('(\d{2}):\d{2}:\d{2}')
        # df['minute'] = df.timestamp.str.extract('\d{2}:(\d{2}):\d{2}')
        # df['second'] = df.timestamp.str.extract('\d{2}:\d{2}:(\d{2})')

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
            # 'ms'
        })

        df.drop(['timestamp', 'month', 'year', 'day',
                'h', 's', 'm'], axis=1, inplace=True)

        # print('timestamp: ', type(
        #     df.timestamp.str.extract('(\d{2}):\d{2}:\d{2}')))
        # print('year ', type(df.year))
        # # print(df.timestamp.str.extract('\d{2}:(\d{2}):\d{2}'))

        # print(df.time_period.unique())

        df.date_uuid = df.date_uuid.astype('string')
        df.time_period = df.time_period.astype('string')

        # mask = df.date_uuid.str.contains(
        #     '[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}', na=False, regex=True)

        # print(mask)
        # print('Number of valid date_uuid: ', df.date_uuid[mask].count(
        # ), '\nNumber of invalid date_uuid: ', df.date_uuid[~mask].count())

        print(df.head())
        print(df.info())

        # dict_keys(['timestamp', 'month', 'year', 'day', 'time_period', 'date_uuid'])

        # df = pd.read_json(response.json(), orient='index')
        # df = pd.json_normalize(response.json())
        # print(df.head())
        print(df.columns)

        return df

    def clean_orders_table(self, df):
        ''' Cleans the orders database stored in the RDS database

        Argument: df

        Returns: df
        '''

        # You should remove the columns, first_name, last_name and 1 to have the table in the correct form before uploading to the database.
        # You will see that the orders data contains column headers which are the same in other tables.
        df.info()
        df.drop(['first_name', 'last_name', '1'], axis=1, inplace=True)

        df.info()

        # remove nulls
        df = df[df.level_0 != 'NULL']

        # print(df.isna().sum())
        # print(df.isna().count())
        # print(df.head())

        def clean_explore(regex, column):
            mask = df[column].str.contains(
                regex, regex=True, na=False)
            print(column, " : ", df[column][~mask].count(), df[column][~mask])

        #  0   level_0           120123 non-null  int64
        #  1   index             120123 non-null  int64
        #  2   date_uuid         120123 non-null  object ✅
        #  3   user_uuid         120123 non-null  object ✅
        #  4   card_number       120123 non-null  int64 ✅ (they are all int64 anyways - should convert other credit card columns to this format)
        #  5   store_code        120123 non-null  object ✅
        #  6   product_code      120123 non-null  object ✅
        #  7   product_quantity  120123 non-null  int64 ✅

        # mask = df.user_uuid.str.contains(
        #     '\w{8}-\w{4}-\w{4}-\w{4}-\w{12}', regex=True, na=False)
        # print('user_uuid ', df.user_uuid[~mask].count(), df.user_uuid[~mask])

        # clean_explore('[a-zA-Z]+', 'card_number')
        # clean_explore('^[A-Z]+-\w{8}$', 'store_code')
        # clean_explore('\w{2}-\d+\w{1}', 'product_code')
        # print(df.product_quantity.min(), df.product_quantity.max())
        # print(df.level_0.min(), df.level_0.max())
        # print(df.index.min(), df.index.max())

        #  i've been dropping the indexes, and as it appear index and level_0 are the same, I'm going to drop these too
        df.drop(columns=['index', 'level_0'], inplace=True)

        # df.head()
        # df.info()

        return df

    def process_date(df, column):

        # print('\n**** BEFORE ANY PROCESSING **** \n')
        # df.info()
        # print(df.head())

        df.reset_index(inplace=True)  # THINK THE INDEX IS ISSUE

        date_regex = '^\d{4}\-(0[1-9]|1[012])\-(0[1-9]|[12][0-9]|3[01])$'
        YYYYMONTHDATE = '\d{4}\s[a-zA-Z]+\s\d{2}'  # 2012 October 21
        YYYYMMDD = '\d{4}\/\d{2}\/\d{2}'  # 2012/10/21
        MONTHYEARDATE = '[a-zA-Z]+\s\d{4}\s\d{2}'  # October 2012 21

        regular_dates = df[column].str.contains(date_regex, na=False)
        # print(df[column][~regular_dates])

        for index, unusual_date in df[column][~regular_dates].items():
            # 1968 October 16 \d{4}\s[a-zA-Z]+\s\d{2}

            # print(unusual_date, type(unusual_date))

            if isinstance(unusual_date, float):
                # print('this identifies ', unusual_date, ' as a float')
                continue  # this is get around a float exception for the regular expression

            print(index, 'Carrying on after type check')
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

            # i suspect this is this index playing difficulties here
            df.loc[[index], [column]] = formatted_date
            # print("THIS IS WHAT IS HAPPENING WHEN WE CHANGE THE VALUES: ", index, column)

        # print(df[column][~regular_dates])

        # it's complaining about floats so we could try to cast it as a string

        # print(df.head())
        # df.info()

        # don't think i need string conversion anymore
        # df[column][regular_dates] = df[column][regular_dates].astype('string')
        # print('-------------- After string conversion')
        # print(df.head())
        # df.info()

        '''
        print('************* DEBUG **************')
        for date in df[column][regular_dates]:
            if type(date) != 'str':
                print(date, type(date))

        print('All the null values', print(
            df[column][regular_dates].isnull().count()))
        print(df[column][regular_dates].isnull())

        ##########################
        
        '''

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

        # check for null values

        print(df.isna().values.sum())
        # NULL appears to appear in all rows when it does appear
        df = df[df.opening_date != 'NULL']

        print('NULLS... ', df[df.opening_date == 'NULL'])

        # clean up addresses by replacing \n for ', '

        df.address = df.address.str.replace('\n', ', ')
        df.address = df.address.astype('string')

        # longitude - replace None with null values

        # print(df.longitude.unique())  # these need to be NN.NN+
        mask = df.longitude.str.contains('\d{2}\.\d{1}', na=False)
        df = df[mask]  # is this where we dispense of the garbage values?
        # print('+++++++++++++++++++', df.longitude.unique())
        df.longitude = df.longitude.astype('float')

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
        # print(mask.unique())
        # print(~mask)
        print('The number of non-compliant store codes is ',
              len(df[column][~mask]))
        df.store_code = df.store_code.astype('string')

        # make sure staff numbers are all numbers - they aren't!!
        # df.staff_numbers = df.staff_numbers.astype('int16')
        # these need to be NN.NN+
        print('STAFF NUMBERS', df.staff_numbers.unique())
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
        df.latitude = df.latitude.astype('float')

        # print(df.country_code.unique()) - there are three country codes
        # print(df.continent.unique()) ## there is a mistake in the continents with the addition of 'ee'. Let's replace that

        df.country_code = df.country_code.astype('string')

        # print(df.continent.unique())
        df.continent = df.continent.str.replace('ee', '')
        # print(df.continent.unique())
        df.continent = df.continent.astype('string')

        ''''
        The dates tell us which are the lines with gobbledegook in them, so we want to drop those rows.

        Lat seems meaningless - does it have any meaningful values?
        '''

        df.info()

        df.drop(columns=['index'], inplace=True)
        return df

    def clean_card_data(self, df):
        print(df.iloc[50:70, 0:1])

        # for col in df.columns:
        #     print("Column: ", col)

        # new_index = pd.Index([x for x in range(0, len(df))])
        # df.set_index(new_index)
        # print(df.iloc[50:70, 0:1])

        # we want to reconstruct the index
        # NOT SURE HOW TO DO THIS

        # remove null values

        # print('The length of the dataframe  is ', len(df))
        # print(df.info())

        df = df[df.card_number != 'NULL']  # remove null values

        # print('The length of the dataframe AFTER NULL REMOVAL is ', len(df))
        # print(df.info())

        # check date_payment_confirmed format

        df = DataCleaning.process_date(df, 'date_payment_confirmed')
        # df = DataCleaning.clean_date(df, 'date_payment_confirmed')

        # check expiry date format - want to convert this to a date format too

        column = 'expiry_date'
        date_regex = '^\d{2}\/\d{2}$'
        regular_dates = df[column].str.contains(date_regex)
        # print(df[column])
        # print(df[column][~regular_dates])
        # irregular dates are a strange code. We want to remove all the rows with these in them
        df[column] = df[column][regular_dates]
        # let's check these have been deleted
        # print(df[column][~regular_dates])
        # print(df[column])

        # Try using .loc[row_indexer,col_indexer] = value instead
        df.drop(df[column][~regular_dates].index, inplace=True)

        # now convert the dates

        df[column] = pd.to_datetime(
            df[column], format='%d/%y', errors='ignore')

        # df = df.drop(df[column][~regular_dates], axis=1) THIS DOESN'T WORK
        # print(df[column])

        df.info()
        print(df.head())

        # check consistency in card_provider

        print(df.card_provider.unique())  # seems to be consistent.
        df.card_provider = df.card_provider.astype(
            'string')  # convert to string

        # check for consistency in card number

        # are there any non numbers in this?
        number_regex = '[^0-9]'
        mask = df['card_number'].str.contains(
            number_regex, regex=True, na=False)
        # checked to see if the length of the numbers are correct and they seem to be.
        # print(mask)
        # print(df.card_number[mask])
        # print(df.info(), df.head())
        df.card_number[mask] = df.card_number[mask].str.replace(
            '\?', '', regex=True)  # need to escape a ?
        # print(df.card_number[mask])

        # print(df.info(), df.head(100))

        # do we need to drop the index column?

        df.drop(columns=['index'], inplace=True)

        return df

    def convert_product_weight(self, df):
        ''' Converts the weights column to a decimal value representing their weight in kg. Use a 1:1 ratio of ml to g as a rough estimate for the rows containing ml.

        Parameter: dataframe

        Returns: dataframe
        '''

        '''
        print(df.weight)
        regex = '[a-zA-Z]+'
        all_weights = df.weight.str.findall(regex)

        # print(df.weight[df.weight.str.contains('x', na=False)])
        print(df.weight[df.weight.str.contains('nan', na=False)])
        print(df.weight[df.weight.str.contains(
            '[0-9]*[A-Z]+\d*[A-Z]*', na=False, regex=True)])
        print(df.weight.isna().sum())
        
        '''

        # print(all_weights.explode().unique()) ['kg' 'g' nan 'x' 'GO' 'NZ' 'JTL' 'Z' 'ZTDGUZVU' 'MX' 'RYSHX' 'ml' 'oz']
        # I am hypothesising that all upper case letters are junk
        # This leaves: kg, g, ml, oz. I am not sure 'x'
        # It is of the form  12 x 100g --> these are all in grams, so we can convert all of these to kg from grams
        #  only one case of ounces, oz
        # need to count nan -> there are 4

        # if we have 8g x 8, we need to do the calculation first and then return value divided by 1000
        # regex = '\d+\s\x\s\d+g'
        # calculation_regex = re.compile(r'(\d+)\s\\x\s(\d+)g')

        # for w in df.weight:
        #     # print(w)
        #     if not math.isnan(w):
        #         reg = calculation_regex.search(w)
        #         if reg:
        #             print('\t\t', reg.group(1), reg.group(2))

        # if grams, return the value divided by 1000
        # if df.weight.str.contains('g'):
        #     pass
        # if value is in ml, we divide by 1000

        # shall we do it by masks?

        # TODO: refactor this so it is one formula

        def process_product_weight(regex, lambda_formula):
            mask = df['weight'].str.contains(
                regex, regex=True, na=False)
            df.weight[mask] = df['weight'][mask].apply(lambda_formula)
            print(df.weight[mask])  # for debugging

        # kg
        process_product_weight(
            '(\d+)[k][g]', lambda_formula=lambda x: x.split('kg')[0])
        # print(df[df['weight'].str.contains(
        #     '(\d+)[k][g]', regex=True, na=False)]['weight'])

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
        # df.info()
        # print(df.head(30))

        return df

        # TODO: i would like to do this using regular expression grouping but I can't get this to work

        '''
        
        kg_regex = '(\d+)[k][g]'
        mask = df['weight'].str.contains(
            kg_regex, regex=True, na=False)
        # df[mask]['weight'] = df[mask]['weight'].apply(
        #    lambda x: x.split('kg')[0])
        df.weight[mask] = df['weight'][mask].apply(  # so just by changing the order, putting the mask at the end this can help with these problems https://realpython.com/pandas-settingwithcopywarning/
            lambda x: x.split('kg')[0])
        # df.loc(df['weight'].str.contains(kg_regex, regex=True, na=False), 'weight') = df.loc(df['weight'].str.contains(
        #     kg_regex, regex=True, na=False), 'weight').apply(
        #     lambda x: x.split('kg')[0])
        print(df[mask]['weight'])

        ml_regex = '(\d+)[m][l]'
        mask = df['weight'].str.contains(
            ml_regex, regex=True, na=False)
        df.weight[mask] = df['weight'][mask].apply(
            lambda x: float(x.split('ml')[0]) / 1000)
        print(df[mask]['weight'])

        
        x_g_regex = '\d+\s[x]\s\d+[g]'
        mask = df['weight'].str.contains(
            x_g_regex, regex=True, na=False)
        df.weight[mask] = df['weight'][mask].apply(
            lambda x: float(float(x.split(' x ')[0]) * float(x.split(' x ')[1].split('g')[0])) / 1000)
        print(df[mask]['weight'])
        '''

        # def process_product_weight(regex, lambda_formula):
        #     mask = df['weight'].str.contains(
        #         regex, regex=True, na=False)
        #     df.weight[mask] = df['weight'][mask].apply(lambda_formula
        #     )

        # for w in df.weight:
        #     # so the case of the 2 x 4 is the exception, so we can deal with that first
        #     print(type(w), w)
        #     regex = '\d+\s\\x\s\d+g'
        #     if w.contains(regex, regex=True):
        #         numbers = w.split(' x ')
        #         print(numbers)

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
        # print(len(df))
        df = df[mask]  # this drops the garbage data rows
        # print(len(df))
        df.product_price = df.product_price.apply(
            lambda x: float(x.split('£')[1]))
        # df.product_price = df.product_price.astype(float)
        # the currency representation is a bit odd in the database - 7 is not 7.00.

        ''''
        mask = df['weight'].str.contains(
                regex, regex=True, na=False)
            df.weight[mask] = df['weight'][mask].apply(lambda_formula)
        '''

        # date_added 1704
        df = DataCleaning.process_date(df, 'date_added')
        '''
        # df.info()
        # print(df.head())
        # print(df.loc[[306]])
        
        '''
        # weight 386 DONE

        # category 7

        # print('UNIQUE CATEORIES', df.category.unique(), df.category.nunique())
        df.category = df.category.astype('string')

        # EAN 1849 -- let's check this for any letters as they seem to be numbers only

        # print('EAN stuff now:',
        #     df.EAN[df.EAN.str.contains('^[1]', regex=True)])
        # not sure what to do with EANs - going to leave as is for the time being, as not clear if they need to be a number

        # uuid 1849

        # these seem to be 36 letters long and conform to a format of \w{8}-\w{4}-\w{4}-\w{12}
        # print(df.uuid.unique())
        # mask = df.uuid.str.contains(
        #    '\w{8}-\w{4}-\w{4}-\w{4}-\w{12}', regex=True, na=False)
        # print('UUID ', df.uuid[~mask].count(), df.uuid[mask])
        # uuid appear to be regular

        # removed 2
        # print(df.removed.unique())
        df.removed = df.removed.astype('string')

        # product_code 1849
        # print(df.product_code)

        # mask = df.product_code.str.contains(
        #     '\w{2}-\d{7}\w{1}', regex=True, na=False)
        # print('Product Code ',
        #       df.product_code[~mask].count(), '\n', df.product_code[~mask]) # these appear to be not concerning

        print(df.info())
        print(df.head())

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

        # print('Cleaning mode begun: ')

        # check for nulls
        # print('1-- Checking for null or na values')

        # first we check with standard check
        # print(df.isna().sum())  # comes up with zero
        # print(df.isnull().sum())

        # this comes up as all ok, so let's delve deeper:
        # for column in df.columns:
        #     print(df[column].value_counts())

        # this reveals 21 values which have the string 'NULL'

        # let's see if they are correlated

        for column in df.columns:
            index = df[column][df[column] == 'NULL'].index
            # print(df[column][df[column] == 'NULL'], '\t\t\t', index)
            # index = df[column][df[column] == 'NULL'].index

        # df.info()
        # we now have an object with the index values. Iterate through these to drop the null values
        # print('++++++++++')
        for row in index:
            # print('Deleting ', row)
            df.drop(row, inplace=True)

        # df.info()

        # it looks like they are.

        # check for dates
        # date_of_birth, join_date
        # print(df.date_of_birth[len(df.date_of_birth) > 10])
        # mask = (df.date_of_birth.str.len() > 10) & (
        #   df.date_of_birth[0].isdigit())
        # mask2 = (df.date_of_birth.str.len() > 10) & (
        #     df['date_of_birth'].str[0].str.isdigit())  # note you need to index the string and then use str again to use isdigit()

        # print(df.head())

        # --- correctly type the values

        df.first_name = df.first_name.astype('string')
        df.last_name = df.last_name.astype('string')
        df.company = df.company.astype('string')
        df.email_address = df.email_address.astype('string')
        df.address = df.address.str.replace('\n', ', ')
        df.address = df.address.astype('string')
        df.country = df.country.astype('string')
        df.country_code = df.country_code.astype('string')

        #################################

        '''
        valid_date_mask = df.date_of_birth.str.findall(
            '\d{4}-\d{2}-\d{2}')  # these should be all the valid dates
        '''

        # print(df.address)

        # print('!!!!!!!! Before date manipulation\n')
        # print(df.info())

        # df['just_date'] = df['dates'].dt.date   should keep just date part

        # so first of all we need to identify all the columns that don't adhere to the standard format of YYYY-MM-DD

        date_regex = '^\d{4}\-(0[1-9]|1[012])\-(0[1-9]|[12][0-9]|3[01])$'
        YYYYMONTHDATE = '\d{4}\s[a-zA-Z]+\s\d{2}'
        YYYYMMDD = '\d{4}\/\d{2}\/\d{2}'
        MONTHYEARDATE = '[a-zA-Z]+\s\d{4}\s\d{2}'

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

        # print(df.join_date[~regular_dates])

        df.join_date[regular_dates] = df.join_date[regular_dates].apply(
            parse)
        df.join_date[regular_dates] = pd.to_datetime(
            df.join_date[regular_dates], infer_datetime_format=True, errors='coerce')

        df.join_date = df.join_date.astype(
            'datetime64[ns]')  #  why do i need to do this???

        # date_of_birth
        regular_dates = df.date_of_birth.str.contains(date_regex)

        # print(df.date_of_birth[~regular_dates])

        # let's go through these one by one

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

        # debug
        # for value in df.date_of_birth[~regular_dates]:
        #     print(type(value), value)

        # Now we can convert the standard dates

        '''
        df.date_of_birth[regular_dates] = df.date_of_birth[regular_dates].apply(
            parse)
        df.date_of_birth = pd.to_datetime(
            df.date_of_birth[regular_dates], errors='coerce')
        
        '''

        df.date_of_birth[regular_dates] = df.date_of_birth[regular_dates].apply(
            parse)
        df.date_of_birth[regular_dates] = pd.to_datetime(
            df.date_of_birth[regular_dates], infer_datetime_format=True, errors='coerce')

        df.date_of_birth = df.date_of_birth.astype(
            'datetime64[ns]')  #  why do i need to do this???

        # drop the index, as we don't need it for the sql
        df.drop(columns=['index'], inplace=True)

        # df.date_of_birth.dt.normalize()
        # print(df.date_of_birth[~regular_dates])

        # print(df.info())
        # print(df.date_of_birth[~regular_dates])
        # print(df.head())

        # df.date_of_birth.dt.normalize()

        '''
        https://stackoverflow.com/questions/45858155/removing-the-timestamp-from-a-datetime-in-pandas-dataframe

            You can also use dt.normalize() to convert times to midnight (null times don't render) or dt.floor to floor the frequency to daily:

            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df['timestamp'] = df['timestamp'].dt.normalize()

            df['timestamp'] = df['timestamp'].dt.floor('D')
            Note that this keeps the dtype of the column datetime64[ns] because each element is still of type pd.Timestamp, whereas dt.date suggested in Andrew L's post converts it to object because each element becomes type datetime.date.
        
        '''

        # Phone cleaning

        # print(df.phone_number)

        '''
        
        
        # Our regular expression to match THIS IS FOR BRITISH NUMBERS
        regex_expression = '^(?:(?:\(?(?:0(?:0|11)\)?[\s-]?\(?|\+)44+\)?[\s-]?(?:\(?0\)?[\s-]?)?)|(?:\(?0))(?:(?:\d{5}\)?[\s-]?\d{4,5})|(?:\d{4}\)?[\s-]?(?:\d{5}|\d{3}[\s-]?\d{3}))|(?:\d{3}\)?[\s-]?\d{3}[\s-]?\d{3,4})|(?:\d{2}\)?[\s-]?\d{4}[\s-]?\d{4}))(?:[\s-]?(?:x|ext\.?|\#)\d{3,4})?$'
        # For every row  where the Phone column does not match our regular expression, replace the value with NaN
        df.loc[~df['phone_number'].str.match(
            regex_expression), 'phone_number'] = np.nan
        # df.Phone = df.Phone.astype('string')
        
        '''

        # print(df.head(20))

        # print('Null phone ', df.phone_number.isna().sum())

        # check for incorrect types
        # print('3 checking for data types')
        # print(df.dtypes)

        # so, they are all objects. We probably want to do something with that.

        # check for rows with the wrong info

        # PHONE NUMBER####

        '''
        print(df.date_of_birth[df.date_of_birth.isnull()])

        print(df_old.date_of_birth[df.date_of_birth.isnull()])
        
        '''

        # need to check these are the wrong values that we observed before

        # return df.date_of_birth.isnull()


'''

360     NaT
697     NaT
752     NaT
1046    NaT
1629    NaT
1996    NaT
2995    NaT
3066    NaT
3536    NaT
3613    NaT
3797    NaT
4205    NaT
4592    NaT
5306    NaT
5350    NaT
5423    NaT
5531    NaT
6108    NaT
6221    NaT
6420    NaT
7168    NaT
7259    NaT
8117    NaT
8273    NaT
8386    NaT
8524    NaT
9013    NaT
9934    NaT
10211   NaT
10245   NaT
10360   NaT
11203   NaT
11366   NaT
12177   NaT
13045   NaT
13111   NaT
13159   NaT
14101   NaT
14105   NaT
14499   NaT
14546   NaT
15302   NaT
'''


'''
Old date code

        # get boolean values for mask
        valid_date_mask = df.date_of_birth.str.match(
            '\d{4}-\d{2}-\d{2}')

        # print("Date mask type, ", type(valid_date_mask))
        # print(valid_date_mask)
        # print(df.date_of_birth[valid_date_mask])

        print('Dealing with non standard values first')

        mask1 = df.date_of_birth.str.len() > 10
        mask2 = df['date_of_birth'].str[0].str.isdigit()
        # print(df.date_of_birth[mask1])

        df.date_of_birth[mask1 & mask2] = pd.to_datetime(df.date_of_birth[mask1 & mask2],
                                                         format='%Y %B %d', errors='coerce')
        df.date_of_birth[mask1 & ~mask2] = pd.to_datetime(
            df.date_of_birth[mask1 & ~mask2], format='%B %Y %d', errors='coerce')

        # print('*********After non standard date manipulation\n')
        # print(df.info())

        # print('Now dealing with standard format')

        # df_old = df  # make copy of df before we eliminate the values

        df.date_of_birth[valid_date_mask] = df.date_of_birth[valid_date_mask].apply(
            parse)
        df.date_of_birth = pd.to_datetime(
            df.date_of_birth[valid_date_mask], errors='coerce')



        df.date_of_birth = df.date_of_birth.apply(parse)
        df.date_of_birth = pd.to_datetime(
            df.date_of_birth[~mask1 & ~mask2], infer_datetime_format=True, errors='coerce')

        df.date_of_birth[mask1 & mask2] = pd.to_datetime(df.date_of_birth[mask1 & mask2],
                                                         format='%Y %B %d', errors='coerce')
        df.date_of_birth[mask1 & ~mask2] = pd.to_datetime(
            df.date_of_birth[mask1 & ~mask2], format='%B %Y %d', errors='coerce')

        print('********', df.date_of_birth[mask1])
        

        # NON VALID DATES

        # so there seem to be 16 dates that are in a non-standard format. Let's try to get these to conform
        # https://docs.python.org/3/library/datetime.html#datetime.date

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

        # df.date_of_birth[~df.date_of_birth.str.contains(
        #     '(\d{4})[-](\d{2})[-](\d{2})')].sum()
        # print('DateOfBirth\n', df.date_of_birth)
        # print('JoinDate\n', df.join_date)

        # df.date_of_birth = pd.to_datetime(
        #     df.date_of_birth, infer_datetime_format=True)

        valid_date_mask = df.join_date.str.match(
            '\d{4}-\d{2}-\d{2}')

        print('Number of valid date ', valid_date_mask.count())

        print(df.info())

        mask1 = df.join_date.str.len() > 10

        print('Non standard dates here: ', mask1)

        mask2 = df['join_date'].str[0].str.isdigit()
        print(df.join_date[mask1])

        df.join_date[mask1 & mask2] = pd.to_datetime(df.join_date[mask1 & mask2],
                                                     format='%Y %B %d', errors='coerce')
        df.join_date[mask1 & ~mask2] = pd.to_datetime(
            df.join_date[mask1 & ~mask2], format='%B %Y %d', errors='coerce')

        for d in df.join_date:
            if type(d) == 'string':
                dt.strptime(d, '%Y-%m-%d')

        df.join_date[valid_date_mask] = df.join_date[valid_date_mask].apply(
            parse)
        df.join_date[valid_date_mask] = pd.to_datetime(
            df.join_date[valid_date_mask], errors='coerce')

        df.join_date[valid_date_mask].astype('datetime64[ns]')

        # for d in df.join_date:
        #     dt.strptime(d, '%Y-%m-%d')
        '''
