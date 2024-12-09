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

### 2. Limpieza de Datos
Se identificaron problemas en los datos:
- IDs corruptas en el campo `store_id` de la tabla `inventory`.
- Fechas en formatos incorrectos o inconsistentes.
- Años de lanzamiento (`release_year`) en la tabla `film` con caracteres no válidos (`x2006`, `2006xxx`).

#### Soluciones Aplicadas
- Eliminación de caracteres no válidos en las columnas afectadas usando expresiones regulares.
- Manejo de valores nulos y duplicados.
- Transformación de columnas con valores mal formateados.

### 3. Generación de Archivos CSV
- Los datos limpios fueron separados en archivos individuales y guardados en la carpeta `data/`.

### 4. Subida de Datos a AWS
- Los archivos CSV se subieron a un bucket de S3 para su procesamiento.

### 5. Transformación con AWS Glue
- Un script en AWS Glue realizará las transformaciones necesarias:
  - Limpieza adicional.
  - Joins entre tablas.
  - Generación de nuevas métricas.
  - Escritura de resultados en S3 en formato Parquet.

## Cómo Ejecutar
1. Clona el repositorio:
   ```bash
   git clone <REPO_URL>
   cd etl_project
   pip install -r requirements.txt
