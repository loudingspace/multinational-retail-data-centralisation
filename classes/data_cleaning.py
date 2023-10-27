import pandas as pd
from dateutil.parser import parse
from datetime import datetime as dt
import numpy as np


class DataCleaning:
    '''
    This will have methods to clean data from each of the data sources
    '''

    def clean_user_data(self, df):
        '''performs the cleaning of the user data.

            Parameter: dataframe: panda dataframe

            Returns: panda dataframe

        Looks out for NULL values, 
        errors with dates, 
        incorrectly typed values and 
        rows filled with the wrong information.
        '''

        print('Cleaning mode begun: ')

        # check for nulls
        print('1-- Checking for null or na values')

        # first we check with standard check
        print(df.isna().sum())  # comes up with zero
        print(df.isnull().sum())

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
        print('++++++++++')
        for row in index:
            print('Deleting ', row)
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

        print(df.head())

        # --- correctly type the values

        df.first_name = df.first_name.astype('string')
        df.last_name = df.last_name.astype('string')
        df.company = df.company.astype('string')
        df.email_address = df.email_address.astype('string')
        df.address = df.address.astype('string')
        df.country = df.country.astype('string')
        df.country_code = df.country_code.astype('string')

        #################################

        '''
        valid_date_mask = df.date_of_birth.str.findall(
            '\d{4}-\d{2}-\d{2}')  # these should be all the valid dates
        '''

        print('!!!!!!!! Before date manipulation\n')
        print(df.info())

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

        '''
        
        '''
        df.date_of_birth[valid_date_mask] = df.date_of_birth[valid_date_mask].apply(
            parse)
        df.date_of_birth = pd.to_datetime(
            df.date_of_birth[valid_date_mask], errors='coerce')

        '''

        df.date_of_birth = df.date_of_birth.apply(parse)
        df.date_of_birth = pd.to_datetime(
            df.date_of_birth[~mask1 & ~mask2], infer_datetime_format=True, errors='coerce')

        df.date_of_birth[mask1 & mask2] = pd.to_datetime(df.date_of_birth[mask1 & mask2],
                                                         format='%Y %B %d', errors='coerce')
        df.date_of_birth[mask1 & ~mask2] = pd.to_datetime(
            df.date_of_birth[mask1 & ~mask2], format='%B %Y %d', errors='coerce')

        print('********', df.date_of_birth[mask1])
        '''

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

        print(df.info())
        print(df.head())

        print(df.phone_number)

        # Our regular expression to match
        regex_expression = '^(?:(?:\(?(?:0(?:0|11)\)?[\s-]?\(?|\+)44\)?[\s-]?(?:\(?0\)?[\s-]?)?)|(?:\(?0))(?:(?:\d{5}\)?[\s-]?\d{4,5})|(?:\d{4}\)?[\s-]?(?:\d{5}|\d{3}[\s-]?\d{3}))|(?:\d{3}\)?[\s-]?\d{3}[\s-]?\d{3,4})|(?:\d{2}\)?[\s-]?\d{4}[\s-]?\d{4}))(?:[\s-]?(?:x|ext\.?|\#)\d{3,4})?$'
        # For every row  where the Phone column does not match our regular expression, replace the value with NaN
        df.loc[~df['phone_number'].str.match(
            regex_expression), 'Phone'] = np.nan

        print('Null phone ', df.phone_number.isna().sum())

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
