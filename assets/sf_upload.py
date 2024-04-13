#Importing Libraries
import os
import re
import json
import numpy as np
import pandas as pd
import snowflake.connector
from unidecode import unidecode
from snowflake.connector.pandas_tools import write_pandas

class df_to_sf():  
    # Function to Move File to Snowflake
    @staticmethod
    def to_snowflake():
        # Input saved on a variable
        table_name = str(input('Enter table name including database and schema: '))
        
        # Load Credentials
        data = json.load(open(os.getcwd().replace('\\','/') + '/assets/' + str(''.join(os.listdir(os.getcwd().replace('\\','/') + '/assets')))))

        # Load CSV to Upload
        df = pd.read_csv(os.getcwd().replace("\\","/") + '/file-upload/' + str("".join(os.listdir(os.getcwd().replace("\\","/") + "/file-upload"))))

        # Initializing Connection to Snowflake
        conn = snowflake.connector.connect(user = data["snowflake"]["user"],
                                           authenticator = data["snowflake"]["authenticator"],
                                           account = data["snowflake"]["account"])

        # Method to Execute Snowflake Statements
        cur = conn.cursor()

        # Snowflake Specifications - Database Connection
        cur.execute(f'USE ROLE {data["spefications"]["role"]};')
        cur.execute(f'USE WAREHOUSE {data["spefications"]["warehouse"]};')
        cur.execute(f'USE SCHEMA {data["spefications"]["schema"]};')

        # Function to Rename Headers
        def fix_column_names(x):
            x = x.upper()
            x = unidecode(x)
            x = re.sub('[^A-Za-z0-9 -]+', '' , x)
            x = x.replace(' ', '_')
            x = x.replace('/', '_')
            x = x.replace('-', '_')
            x = x.replace('__', '_')
            return x
        
        # Modifying Headers
        df.columns = [fix_column_names(c) for c in df.columns]

        # Changing D_Types
        df_dtypes = pd.DataFrame(df.dtypes, columns=['dtype'])
        df_dtypes['dtype'] = df_dtypes['dtype'].replace('int64','int')
        df_dtypes['dtype'] = df_dtypes['dtype'].replace('float64','float')

        # Identifying Max Len on Each Column & Replacing Object to Varchar
        string_vars = df_dtypes['dtype'][df_dtypes['dtype'] == 'object'].index
        max_len = df[string_vars].apply(lambda x: int(round(x.str.len().max()))).to_dict()

        # Identifying max char values per each column
        for each in string_vars:
            df_dtypes.loc[each, 'dtype'] = f'varchar ({int(max_len.get(each))})'
        
        # Switching Date into Datetime Snowflake data type
        df_dtypes.loc['DATE','dtype'] = 'datetime'

        # Table Creation
        query = f"""CREATE OR REPLACE TABLE {table_name.upper()} ({", ".join([f'{key} {val}' for key,val in df_dtypes.to_dict()['dtype'].items()])});"""

        # Executing Table Creation
        cur.execute(query)

        # Inserting Data Onto Empty Table
        success, nchunks, nrows, _ = write_pandas(conn, df, table_name.upper(), quote_identifiers = False)
        
        # Delete Files Already Used
        os.remove(os.getcwd().replace("\\","/") + '/file-upload/' + str("".join(os.listdir(os.getcwd().replace("\\","/") + "/file-upload"))))
        return df.head()