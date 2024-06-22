import ast
import json
import pandas as pd
import numpy as np
from google.oauth2 import service_account
import pandas_gbq

class Dataset():
    def __init__(self) -> None:
        self.headers = ['ath', 'atl', 'circulatingSupply', 'cmcRank', 'dateAdded', 'high24h', 'id', 'isActive', 'isAudited', 'lastUpdated', 'low24h', 'marketPairCount', 'maxSupply', 'name', 'quote.USD.dominance', 'quote.USD.fullyDilluttedMarketCap', 'quote.USD.lastUpdated', 'quote.USD.marketCap', 'quote.USD.marketCapByTotalSupply', 'quote.USD.name', 'quote.USD.percentChange1h', 'quote.USD.percentChange1y', 'quote.USD.percentChange24h', 'quote.USD.percentChange30d', 'quote.USD.percentChange60d', 'quote.USD.percentChange7d', 'quote.USD.percentChange90d', 'quote.USD.price', 'quote.USD.selfReportedMarketCap', 'quote.USD.turnover', 'quote.USD.volume24h', 'quote.USD.volume30d', 'quote.USD.volume7d', 'quote.USD.ytdPriceChangePercentage', 'rank', 'selfReportedCirculatingSupply', 'slug', 'symbol', 'totalSupply', 'tvl']
        self.allowed_datatypes = ['float', 'int', 'str', 'bool', 'NoneType']
        self.df = None

    def parse_header_raw_data(self, df):
        df['header'] = df['header'].map(lambda x: ast.literal_eval(x))
        df['raw_data'] = df['raw_data'].map(lambda x: ast.literal_eval(x))
        df['header_count'] = df['header'].apply(lambda x: len(x))
        df['raw_data_count'] = df['raw_data'].apply(lambda x: len(x))
        self.df = df
       
    def get_unique_headers_with_frequency(self):
        header_frequency = {}
        for headers_list in self.df['header']:
            for header in headers_list:
                if header in header_frequency:
                    header_frequency[header] += 1
                else:
                    header_frequency[header] = 1

        remove_list=[]
        for key, value in header_frequency.items():
            if value != len(self.df):
                remove_list.append(key)

        self.header_frequency = header_frequency
        return remove_list, header_frequency


    def joined_arrays_df(self):
        list_of_dicts = []
        for row in range(len(self.df)):
            headers = [i for i in self.df['header'].iloc[row]]
            raw_data =  [i for i in self.df['raw_data'].iloc[row]]
            list_of_dicts.append(dict(zip(headers, raw_data)))


        dataset_columns = list(self.header_frequency.keys())
        output_dict = {key: [] for key in dataset_columns}

        for item in list_of_dicts:
            for key in dataset_columns:
                if key in item:
                    output_dict[key].append(item[key])
                else:
                    output_dict[key].append(None)

        return pd.DataFrame(output_dict)

    def cdp(self, num):
        if isinstance(num, str) or isinstance(num, bool) or num is None:
            return (0, 0)
        
        num_str = str(num)
        if '.' in num_str:
            integer_part, decimal_part = num_str.split('.')
            num_decimal_places = len(decimal_part)
            num_chars_before_decimal = len(integer_part)
            return num_chars_before_decimal, num_decimal_places
        else:
            return len(num_str), 0     

    def create_X(self, group):
        l = []
        for item in self.df['raw_data'][group]:
            l.append([item, type(item).__name__, self.cdp(item)[0], self.cdp(item)[1], len(str(item))])

        df = pd.DataFrame(
            columns=['value', 'datatype', 'integer_part', 'decimal_part', 'char_count'],
            data=l
            )
        unique_dtypes = list(df['datatype'].unique())

        encode_on = 'datatype'
        df = pd.get_dummies(df, columns=[encode_on])
        for col_name in unique_dtypes:
            col_name = encode_on + '_' + col_name
            df[col_name] = df[col_name].astype(int)
        return df
    
    def create_y(self, file_path):
        with open(file_path, 'r') as file:
            self.json_labels = json.load(file)

        l = []
        for list_item in self.json_labels['labels']:
            for key, value in list_item.items():
                if key != 'pk':
                    l.append([key, type(value).__name__, self.cdp(value)[0], self.cdp(value)[1], len(str(value))])

        df = pd.DataFrame(
            columns=['column', 'datatype', 'integer_part', 'decimal_part', 'char_count'],
            data=l
            )
    
        df = df.sort_values('column')
        unique_dtypes = list(df['datatype'].unique())
        print(f'Unique datatypes: {unique_dtypes}')

        encode_on = 'datatype'
        df_encoded = pd.get_dummies(df, columns=[encode_on])
        for col_name in unique_dtypes:
            col_name = encode_on + '_' + col_name
            df_encoded[col_name] = df_encoded[col_name].astype(int)

        # toggle group by median, mean, max etc
        df_mean = df_encoded.groupby('column').mean()
        df_std = df_encoded.groupby('column').std()
        df_mean_std = pd.DataFrame()

        for column in df_mean.columns:
            concatenated_values = df_mean[column].astype(str) + '-' + df_std[column].astype(str)
            df_mean_std[column] = concatenated_values

        df_mean_std = df_mean_std.reset_index('column')
        df_mean_std.rename(columns={'column': 'key'}, inplace=True)
        return df_mean_std
    
    def write_to_bq(self, project_id, table_name, df, cred_path, if_exists='append'):
        credentials = service_account.Credentials.from_service_account_file(cred_path)

        for column in df.columns:
            new_column = column.replace('.', '_')
            df.rename(columns={column: new_column}, inplace=True)
            
        pandas_gbq.to_gbq(
                    destination_table=table_name,
                    project_id=project_id,
                    if_exists=if_exists,
                    dataframe=df.astype(str),
                    credentials=credentials
                    )
        

class DataLabeller():
    def __init__(self) -> None:
        pass

    def create_label_prompt_gpt35(self, df, data_list, header_list, loc, places=3):
        prompt = f"""
        You are an expert in cryptocurrency and trading cryptocurrencies.
        You are given a list of column names and a list of values with similar lengths, but it appears that some data is missing and there is also some redundant data.
        In a perfect world, the sequence of the values would match the sequence of columns and that is how you would match them, but the data is imperfect. You can not move a value more than {places} places.
        Your task as a crypto expert is to guess the mapping between values and columns, where the sequence is still the most important factor.
        You will not always find matches or you may encounter 'None' etc. If you don't believe an existing header has a good value match, then enter 'no-match'.
        You must return your guess as a dictionary with the column-value pairs, column name is the key and value is the value.
        The output is to be created by you, the LLM, and not Python or any other programming language.
        You must not change the order of the items in the column list.
        Also include 'pk': {df.iloc[loc]['pk']} at the end.
        I will add this dictionary to a json file, make sure all values will not raise errors if loaded from a json file.
        """
        complete_prompt = f"{prompt} \n, column_list: {header_list} \n, data_list: {data_list}"
        return complete_prompt


class Model():
    def __init__(self) -> None:
        pass

    def model(features: np.array, labels: np.array, headers, max_shift=3):
        if len(features) > 36 and len(features) < 42:
            pass
        else:
            print('too long or too short')