# Prueba Técnica - Ingeniero de Datos

Este repositorio contiene la solución para la prueba técnica de un pipeline ETL, desarrollado para procesar datos desde un archivo Excel inicial, realizar limpieza, y cargarlos en AWS para realizar transformaciones adicionales.

## Estructura del Proyecto

etl_project/
├── data/
├── src/
│   ├── etl.py
│   ├── customer.py
│   ├── store.py
│   ├── rental.py
│   ├── inventory.py
│   └── film.py
├── README.md
├── requirements.txt
└── .gitignore


## Pasos del Proyecto

### 1. Extracción de Datos
- Se proporcionó un archivo Excel llamado `Films2.xlsx` con cinco hojas: `customer`, `store`, `rental`, `inventory` y `film`.
- Cada hoja representa una tabla en un modelo entidad-relación.

### 2. Limpieza y Transformación
Se identificaron problemas específicos en cada hoja de datos, los cuales fueron corregidos con las siguientes transformaciones:

#### **customer**
- Eliminación de caracteres no numéricos en los campos `customer_id`, `store_id`, `address_id` y `active`.
- Normalización de correos electrónicos con expresiones regulares.
- Conversión de nombres y correos electrónicos a mayúsculas.
- Generación de un identificador alternativo en caso de valores faltantes o inválidos en `customer_id_old`.

#### **store**
- Eliminación de caracteres no numéricos en `store_id`, `manager_staff_id` y `address_id`.

#### **rental**
- Limpieza de columnas clave: `rental_id`, `inventory_id`, `customer_id` y `staff_id` para eliminar caracteres no numéricos.

#### **inventory**
- Limpieza de `inventory_id`, `film_id` y `store_id` para eliminar caracteres no válidos, preservando la información relevante.

#### **film**
- Eliminación de caracteres no numéricos en columnas como `release_year`, `rental_duration`, `rental_rate` y `replacement_cost`.

### 3. Automatización del ETL
Se creó una estructura modular con clases específicas para cada tabla (`customer`, `store`, `rental`, `inventory`, `film`), ubicadas en la carpeta `src/`. Estas clases realizan las transformaciones y guardan los resultados en archivos CSV.

#### **Archivo `functions.py`**
- Centraliza las clases de ETL y coordina la ejecución del proceso.

#### **Archivo `main.py`**
- Ejecuta el proceso completo desde el archivo Excel hasta la generación de los archivos CSV limpios en la carpeta `data/`.

## Cómo Ejecutar
1. Clona el repositorio:
   ```bash
   git clone <REPO_URL>
   cd etl_project
   pip install -r requirements.txt
   python main.py