import pandas as pd
import re

class StoreETL:
    def extract_transform_load(self, xl):
        df_store = xl.parse('store')
        # Limpieza y transformación de datos
        df_store = self.clean_store_data_int(df_store, 'store_id')
        df_store = self.clean_store_data_int(df_store, 'manager_staff_id')
        df_store = self.clean_store_data_int(df_store, 'address_id')
        self.store_df = df_store

    def clean_store_data_int(self, df, column_name):
        df[column_name] = df[column_name].apply(lambda x: re.sub(r'[^0-9]', '', str(x)))
        df[column_name] = df[column_name].astype(int)
        return df
    
    def clean_store_data_float(self, df, column_name):
        df[column_name] = df[column_name].apply(lambda x: re.sub(r'[^0-9]', '', str(x)))
        df[column_name] = df[column_name].astype(float)
        return df
    
    def save_to_csv(self, file_path):
        self.store_df.to_csv(file_path, index=False)