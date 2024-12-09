import pandas as pd
import re

class InventoryETL:
    def extract_transform_load(self, xl):
        df_inventory = xl.parse('inventory')
        # Limpieza y transformación de datos
        df_inventory = self.clean_inventory_data_int(df_inventory, 'inventory_id')
        df_inventory = self.clean_inventory_data_int(df_inventory, 'film_id')
        df_inventory = self.clean_inventory_data_int(df_inventory, 'store_id')
        self.inventory_df = df_inventory

    def clean_inventory_data_int(self, df, column_name):
        df[column_name] = df[column_name].apply(lambda x: re.sub(r'[^0-9]', '', str(x)))
        df[column_name] = df[column_name].astype(int)
        return df
    
    def clean_inventory_data_float(self, df, column_name):
        df[column_name] = df[column_name].apply(lambda x: re.sub(r'[^0-9]', '', str(x)))
        df[column_name] = df[column_name].astype(float)
        return df
    
    def save_to_csv(self, file_path):
        self.inventory_df.to_csv(file_path, index=False)