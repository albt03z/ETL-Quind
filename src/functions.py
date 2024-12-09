import pandas as pd
from src.customer import CustomerETL
from src.store import StoreETL
from src.rental import RentalETL
from src.inventory import InventoryETL
from src.film import FilmETL

class ETLManager:
    def __init__(self, input_file):
        self.input_file = input_file
        self.customer_etl = CustomerETL()
        self.store_etl = StoreETL()
        self.rental_etl = RentalETL()
        self.inventory_etl = InventoryETL()
        self.film_etl = FilmETL()

    def run_etl(self):
        # Cargar los datos del archivo de entrada
        xl = pd.ExcelFile(self.input_file)

        # Ejecutar los ETLs para cada tabla
        self.customer_etl.extract_transform_load(xl)
        self.store_etl.extract_transform_load(xl)
        self.rental_etl.extract_transform_load(xl)
        self.inventory_etl.extract_transform_load(xl)
        self.film_etl.extract_transform_load(xl)

        # Guardar los datos procesados en archivos CSV
        self.customer_etl.save_to_csv('data/customer_clean.csv')
        self.store_etl.save_to_csv('data/store_clean.csv')
        self.rental_etl.save_to_csv('data/rental_clean.csv')
        self.inventory_etl.save_to_csv('data/inventory_clean.csv')
        self.film_etl.save_to_csv('data/film_clean.csv')

if __name__ == "__main__":
    etl_manager = ETLManager('../resources/Films_2.xlsx')
    etl_manager.run_etl()