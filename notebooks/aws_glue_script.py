import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, when, avg, count
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
    database="dbfilmsinitial",
    table_name="customer_clean_csv",
    transformation_ctx="dyf_customer"
)

dyf_store = glueContext.create_dynamic_frame.from_catalog(
    database="dbfilmsinitial",
    table_name="store_clean_csv",
    transformation_ctx="dyf_store"
)

dyf_rental = glueContext.create_dynamic_frame.from_catalog(
    database="dbfilmsinitial",
    table_name="rental_clean_csv",
    transformation_ctx="dyf_rental"
)

dyf_inventory = glueContext.create_dynamic_frame.from_catalog(
    database="dbfilmsinitial",
    table_name="inventory_clean_csv",
    transformation_ctx="dyf_inventory"
)

dyf_film = glueContext.create_dynamic_frame.from_catalog(
    database="dbfilmsinitial",
    table_name="film_clean_csv",
    transformation_ctx="dyf_film"
)

# Convertir DynamicFrames a DataFrames
df_customer = dyf_customer.toDF()
df_store = dyf_store.toDF()
df_rental = dyf_rental.toDF()
df_inventory = dyf_inventory.toDF()
df_film = dyf_film.toDF()

# ---- Transformaciones ----

# 1. Total de alquileres por tienda
df_rental_inventory = df_rental.join(df_inventory, "inventory_id", "inner")
df_rental_store = df_rental_inventory.join(df_store, "store_id", "inner")
df_rentals_by_store = df_rental_store.groupBy("store_id").agg(count("rental_id").alias("total_rentals"))

# 2. Total de alquileres por género (usando rating como aproximación)
if "rating" in df_film.columns:
    df_rental_film = df_rental_inventory.join(df_film, "film_id", "inner")
    df_rentals_by_genre = df_rental_film.groupBy("rating").agg(count("rental_id").alias("total_rentals_by_genre"))
else:
    df_rentals_by_genre = None

# 3. Total de alquileres por año de lanzamiento
if "release_year" in df_film.columns:
    df_rentals_by_year = df_rental_film.groupBy("release_year").agg(count("rental_id").alias("total_rentals_by_year"))
else:
    df_rentals_by_year = None

# 4. Duración promedio de los alquileres
df_rental = df_rental.withColumn(
    "rental_duration_days",
    (col("return_date").cast("long") - col("rental_date").cast("long")) / (24 * 60 * 60)
<<<<<<< HEAD
=======
).groupBy().agg({"rental_duration_days": "avg"}).withColumnRenamed("avg(rental_duration_days)", "average_rental_duration")

# Convertir los DataFrames procesados a DynamicFrames para escritura en S3
dyf_rental_store = DynamicFrame.fromDF(df_rental_store, glueContext, "dyf_rental_store")
dyf_rental_genre = DynamicFrame.fromDF(df_rental_genre, glueContext, "dyf_rental_genre")
dyf_rental_avg_duration = DynamicFrame.fromDF(df_rental_avg_duration, glueContext, "dyf_rental_avg_duration")

# Escribir los resultados en S3 en formato Parquet
glueContext.write_dynamic_frame.from_options(
    frame=dyf_rental_store,
    connection_type="s3",
    connection_options={"path": "s3://etl-adaberto-gonzalez-quind/results/"},
    format="parquet",
    transformation_ctx="write_parquet_store"
>>>>>>> e2f5851cf92abea518c4b5baf19b17dc43634678
)
df_avg_duration = df_rental.select(avg("rental_duration_days").alias("average_rental_duration"))

<<<<<<< HEAD
# 5. Películas más alquiladas
if "title" in df_film.columns:
    df_most_rented_films = df_rental_film.groupBy("film_id", "title").agg(count("rental_id").alias("total_rentals_by_film")) \
        .orderBy(col("total_rentals_by_film").desc()).limit(10)
else:
    df_most_rented_films = None

# ---- Escritura en S3 ----
def write_to_s3(dynamic_frame, path, context_name):
    if dynamic_frame is not None:
        glueContext.write_dynamic_frame.from_options(
            frame=dynamic_frame,
            connection_type="s3",
            connection_options={"path": path},
            format="parquet",
            transformation_ctx=context_name
        )

# Convertir y escribir resultados
dyf_rentals_by_store = DynamicFrame.fromDF(df_rentals_by_store, glueContext, "dyf_rentals_by_store")
write_to_s3(dyf_rentals_by_store, "s3://etl-adaberto-gonzalez-quind/results/rentals_by_store/", "write_rentals_by_store")

if df_rentals_by_genre is not None:
    dyf_rentals_by_genre = DynamicFrame.fromDF(df_rentals_by_genre, glueContext, "dyf_rentals_by_genre")
    write_to_s3(dyf_rentals_by_genre, "s3://etl-adaberto-gonzalez-quind/results/rentals_by_genre/", "write_rentals_by_genre")

if df_rentals_by_year is not None:
    dyf_rentals_by_year = DynamicFrame.fromDF(df_rentals_by_year, glueContext, "dyf_rentals_by_year")
    write_to_s3(dyf_rentals_by_year, "s3://etl-adaberto-gonzalez-quind/results/rentals_by_year/", "write_rentals_by_year")

dyf_avg_duration = DynamicFrame.fromDF(df_avg_duration, glueContext, "dyf_avg_duration")
write_to_s3(dyf_avg_duration, "s3://etl-adaberto-gonzalez-quind/results/avg_duration/", "write_avg_duration")

if df_most_rented_films is not None:
    dyf_most_rented_films = DynamicFrame.fromDF(df_most_rented_films, glueContext, "dyf_most_rented_films")
    write_to_s3(dyf_most_rented_films, "s3://etl-adaberto-gonzalez-quind/results/most_rented_films/", "write_most_rented_films")
=======
glueContext.write_dynamic_frame.from_options(
    frame=dyf_rental_genre,
    connection_type="s3",
    connection_options={"path": "s3://etl-adaberto-gonzalez-quind/results/"},
    format="parquet",
    transformation_ctx="write_parquet_genre"
)

glueContext.write_dynamic_frame.from_options(
    frame=dyf_rental_avg_duration,
    connection_type="s3",
    connection_options={"path": "s3://etl-adaberto-gonzalez-quind/results/"},
    format="parquet",
    transformation_ctx="write_parquet_avg_duration"
)
>>>>>>> e2f5851cf92abea518c4b5baf19b17dc43634678

# Finalizar job
job.commit()

