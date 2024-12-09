import pandas as pd
import re

class FilmETL:
    def extract_transform_load(self, xl):
        df_film = xl.parse('film')
        # Limpieza y transformación de datos
        df_film = self.clean_film_data_int(df_film, 'film_id')
        df_film = self.clean_film_data_int(df_film, 'release_year')
        df_film = self.clean_film_data_int(df_film, 'rental_duration')
        df_film = self.clean_film_data_float(df_film, 'rental_rate')
        df_film = self.clean_film_data_float(df_film, 'replacement_cost')
        df_film = self.clean_film_data_int(df_film, 'num_voted_users')
        self.film_df = df_film

    def clean_film_data_int(self, df, column_name):
        df[column_name] = df[column_name].apply(lambda x: re.sub(r'[^0-9]', '', str(x)))
        df[column_name] = df[column_name].astype(int)
        return df
    
    def clean_film_data_float(self, df, column_name):
        df[column_name] = df[column_name].apply(lambda x: re.sub(r'[^0-9]', '', str(x)))
        df[column_name] = df[column_name].astype(float)
        return df
    
    def save_to_csv(self, file_path):
        self.film_df.to_csv(file_path, index=False)