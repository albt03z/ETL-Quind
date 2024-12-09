import pandas as pd
import re

class RentalETL:
    def extract_transform_load(self, xl):
        df_rental = xl.parse('rental')
        # Limpieza y transformación de datos
        df_rental = self.clean_rental_data_int(df_rental, 'rental_id')
        df_rental = self.clean_rental_data_int(df_rental, 'inventory_id')
        df_rental = self.clean_rental_data_int(df_rental, 'customer_id')
        df_rental = self.clean_rental_data_int(df_rental, 'staff_id')
        df_rental = self.remove_null_return_dates(df_rental)
        self.rental_df = df_rental

    def clean_rental_data_int(self, df, column_name):
        df[column_name] = df[column_name].apply(lambda x: re.sub(r'[^0-9]', '', str(x)))
        df[column_name] = df[column_name].astype(int)
        return df
    
    def clean_rental_data_float(self, df, column_name):
        df[column_name] = df[column_name].apply(lambda x: re.sub(r'[^0-9]', '', str(x)))
        df[column_name] = df[column_name].astype(float)
        return df
    
    def remove_null_return_dates(self, df):
        df = df[~df['return_date'].astype(str).str.strip().isin(['NULL', 'N/A', 'NaN', ''])]
        return df
    
    def save_to_csv(self, file_path):
        self.rental_df.to_csv(file_path, index=False)