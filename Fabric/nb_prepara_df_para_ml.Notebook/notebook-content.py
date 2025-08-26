# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "43da1e05-d8be-4a6f-a2fd-c78d874da4c6",
# META       "default_lakehouse_name": "lh_gold_Andreu_Cabero",
# META       "default_lakehouse_workspace_id": "8875ebcb-20bc-4e11-af95-6f83a9444ab4",
# META       "known_lakehouses": [
# META         {
# META           "id": "43da1e05-d8be-4a6f-a2fd-c78d874da4c6"
# META         },
# META         {
# META           "id": "4e0ebb92-0f61-4678-a0c5-4c0e98caa998"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Para explotar modelo semantico directamente como fuente de datos para ML usaremos la libreria SemPy
%pip install semantic-link
%load_ext sempy

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Explorar todos los modelos semanticos del workspace
import sempy.fabric as fab
msemantic = fab.list_datasets()
msemantic

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# crear variables el modelo semantico sobre el que trabajaremos
my_sm = "sm_temperaturas_dia"

# revisar cuantas tablas existen en ese modelo
tablas = fab.list_tables(my_sm)
tablas

columnas = fab.list_columns(my_sm)
columnas

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# explorar tabla de hechos
df_hechos = fab.read_table(my_sm, "ft_temperaturas_estacion_dia")
df_hechos

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# enriquecemos la tabla de hechos con datos adicionales de estaciones meteorologicas

from pyspark.sql.functions import col, to_date, lit, broadcast
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df_hechos_fab = fab.read_table(my_sm, "ft_temperaturas_estacion_dia")
df_hechos = spark.createDataFrame(df_hechos_fab)

df_hechos = df_hechos.withColumn("fecha", to_date(col("fecha"), "yyyy-MM-dd"))

# Filtrar con between y descartando fechas nulas
df_filtrado = df_hechos.filter(
    col("fecha").isNotNull() &
    col("fecha").between(to_date(lit("1978-01-01")), to_date(lit("1993-12-31")))
)

df_estaciones_fab = fab.read_table(my_sm, "dim_estaciones_meteorologicas")
df_estaciones = spark.createDataFrame(df_estaciones_fab)

# Join con broadcast para optimizar
df_join = df_filtrado.join(
    broadcast(df_estaciones),
    df_filtrado["CodigoNacional"] == df_estaciones["CodigoNacional"],
    how="inner"
)





# Selección final de columnas (evita duplicados)
cols_out = [
    "fecha", "Tmax", "Tmin",
    df_filtrado["CodigoNacional"].alias("CodigoNacional"),
    # agrega aquí las columnas que te interesen de la dimensión:
    "nombreEstacion", "Latitud", "Longitud"
]

df_out = df_join.select(*cols_out)
display(df_out)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# guardamos el df como data de entrenamiento ML
df_out.write.mode("overwrite").saveAsTable("lh_gold_Andreu_Cabero.ml_train_temperaturas")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
