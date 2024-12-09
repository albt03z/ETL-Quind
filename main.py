from src.functions import ETLManager

if __name__ == "__main__":
    etl_manager = ETLManager('resources/Films_2.xlsx')
    etl_manager.run_etl()