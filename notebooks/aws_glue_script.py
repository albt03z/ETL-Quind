import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, when, regexp_replace
from awsglue.dynamicframe import DynamicFrame

# Argumentos del job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Leer tablas del catálogo de AWS Glue
dyf_customer = glueContext.create_dynamic_frame.from_catalog(
    database="dbfilminitial",
    table_name="customer_clean_csv",
    transformation_ctx="dyf_customer"
)

dyf_store = glueContext.create_dynamic_frame.from_catalog(
    database="dbfilminitial",
    table_name="store_clean_csv",
    transformation_ctx="dyf_store"
)

dyf_rental = glueContext.create_dynamic_frame.from_catalog(
    database="dbfilminitial",
    table_name="rental_clean_csv",
    transformation_ctx="dyf_rental"
)

dyf_inventory = glueContext.create_dynamic_frame.from_catalog(
    database="dbfilminitial",
    table_name="inventory_clean_csv",
    transformation_ctx="dyf_inventory"
)

dyf_film = glueContext.create_dynamic_frame.from_catalog(
    database="dbfilminitial",
    table_name="film_clean_csv",
    transformation_ctx="dyf_film"
)

# Convertir DynamicFrames a DataFrames para realizar transformaciones
df_customer = dyf_customer.toDF()
df_store = dyf_store.toDF()
df_inventory = dyf_inventory.toDF()
df_film = dyf_film.toDF()
df_rental = dyf_rental.toDF()

# Limpieza de datos (Ejemplo)
# 1. Limpieza de 'store_id' en inventory
df_inventory = df_inventory.withColumn("store_id", 
    when(col("store_id").rlike(r"^[0-9]+[\$\*\#\"]*$"), regexp_replace(col("store_id"), r"[\$\*\#\"]", ""))
    .otherwise(col("store_id"))
)

# 2. Limpieza de 'release_year' en film (Eliminación de caracteres no numéricos)
df_film = df_film.withColumn("release_year", 
    when(col("release_year").rlike(r"^\D*\d{4}\D*$"), regexp_replace(col("release_year"), r"\D", ""))
    .otherwise(col("release_year"))
)

# 3. Cálculos y análisis de las preguntas de negocio:
# a. Total de alquileres por tienda (por ejemplo)
df_rental_store = df_rental.groupBy("store_id").agg(
    {"rental_id": "count"}
).withColumnRenamed("count(rental_id)", "total_rentals")

# b. Total de alquileres por género de película
df_rental_genre = df_rental.join(df_film, df_rental["film_id"] == df_film["film_id"], "inner") \
    .groupBy("genre").agg({"rental_id": "count"}) \
    .withColumnRenamed("count(rental_id)", "total_rentals_by_genre")

# c. Duración promedio de los alquileres
df_rental_avg_duration = df_rental.withColumn("rental_duration_days", 
    (col("return_date").cast("long") - col("rental_date").cast("long")) / (24 * 60 * 60)
).groupBy().agg({"rental_duration_days": "avg"}).withColumnRenamed("avg(rental_duration_days)", "average_rental_duration")

# Convertir los DataFrames procesados a DynamicFrames para escritura en S3
dyf_rental_store = DynamicFrame.fromDF(df_rental_store, glueContext, "dyf_rental_store")
dyf_rental_genre = DynamicFrame.fromDF(df_rental_genre, glueContext, "dyf_rental_genre")
dyf_rental_avg_duration = DynamicFrame.fromDF(df_rental_avg_duration, glueContext, "dyf_rental_avg_duration")

# Escribir los resultados en S3 en formato Parquet
glueContext.write_dynamic_frame.from_options(
    frame=dyf_rental_store,
    connection_type="s3",
    connection_options={"path": "s3://etl-adaberto-gonzalez-quind/results/rental_store/"},
    format="parquet",
    transformation_ctx="write_parquet_store"
)

glueContext.write_dynamic_frame.from_options(
    frame=dyf_rental_genre,
    connection_type="s3",
    connection_options={"path": "s3://etl-adaberto-gonzalez-quind/results/rental_genre/"},
    format="parquet",
    transformation_ctx="write_parquet_genre"
)

glueContext.write_dynamic_frame.from_options(
    frame=dyf_rental_avg_duration,
    connection_type="s3",
    connection_options={"path": "s3://etl-adaberto-gonzalez-quind/results/rental_duration/"},
    format="parquet",
    transformation_ctx="write_parquet_avg_duration"
)

# Finalizar job
job.commit()
