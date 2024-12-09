import pandas as pd
import re

class CustomerETL:
    def extract_transform_load(self, xl):
        df_customer = xl.parse('customer')
        # Limpieza y transformación de datos
        df_customer = self.clean_customer_data_int(df_customer, 'customer_id')
        df_customer = self.clean_customer_data_int(df_customer, 'store_id')
        df_customer = self.clean_customer_data_int(df_customer, 'address_id')
        df_customer = self.clean_customer_data_int(df_customer, 'active')
        df_customer = self.clean_email(df_customer, 'email')
        df_customer = self.to_upper(df_customer, 'first_name')
        df_customer = self.to_upper(df_customer, 'last_name')
        df_customer = self.to_upper(df_customer, 'email')
        df_customer = self.generate_customer_id_old(df_customer)
        self.customer_df = df_customer

    def clean_customer_data_int(self, df, column_name):
        df[column_name] = df[column_name].apply(lambda x: re.sub(r'[^0-9]', '', str(x)))
        df[column_name] = df[column_name].astype(int)
        return df
    
    def clean_customer_data_float(self, df, column_name):
        df[column_name] = df[column_name].apply(lambda x: re.sub(r'[^0-9]', '', str(x)))
        df[column_name] = df[column_name].astype(float)
        return df
    
    def clean_email(self, df, column_name):
        df[column_name] = df[column_name].str.extract(r'([\w\.-]+@[\w\.-]+\.\w+)')
        return df
    
    def to_upper(self, df, column_name):
        df[column_name] = df[column_name].str.upper()
        return df
    
    def generate_customer_id_old(self, df):
        def generate_id(row):
            current_value = str(row['customer_id_old']).strip().upper()
            if pd.isnull(row['customer_id_old']) or current_value in {"NULL", "N/A", "NAN"}:
                return f"{row['first_name'].strip()[0]}{row['last_name'].strip()[0]}-{str(row['customer_id']).zfill(5)}"
            return row['customer_id_old']
        
        df['customer_id_old'] = df.apply(generate_id, axis=1)
        return df

    def save_to_csv(self, file_path):
        self.customer_df.to_csv(file_path, index=False)