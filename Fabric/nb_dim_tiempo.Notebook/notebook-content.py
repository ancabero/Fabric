# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "4e0ebb92-0f61-4678-a0c5-4c0e98caa998",
# META       "default_lakehouse_name": "lh_silver_Andreu_Cabero",
# META       "default_lakehouse_workspace_id": "8875ebcb-20bc-4e11-af95-6f83a9444ab4",
# META       "known_lakehouses": [
# META         {
# META           "id": "4e0ebb92-0f61-4678-a0c5-4c0e98caa998"
# META         },
# META         {
# META           "id": "8a624db2-6d4f-42cf-bf6d-a4c0bc918125"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Construir una dimensión de tiempo a partir de los datos de temperatura

# importar librerías y funciones
from pyspark.sql.functions import col, to_date, date_format

# cargar los datos
df = spark.table("lh_bronze_Andreu_Cabero.tempmin")

# seleccionar solo la columna time quitando duplicados
df_time = df.select("time").distinct()
#display(df_time)

# agregar columnas "fecha" y "hora"
df_th = (
    df_time.withColumn("fecha", to_date(col("time")))
            .withColumn("hora", date_format(col("time"), "HH:mm:ss"))
)
df_th.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# agregar mas campos a partir del df_th
from pyspark.sql.functions import year, month, dayofmonth, dayofweek, dayofyear

dimtiempo = df_th.withColumn("año", year(col("fecha")))\
                 .withColumn("mes", month(col("fecha")))\
                 .withColumn("dia", dayofmonth(col("fecha")))\
                 .withColumn("nombre_dia", date_format(col("fecha"), "EEEE"))\
                 .withColumn("numero_dia_semana", dayofweek(col("fecha")))\
                 .withColumn("dia_corr_anual", dayofyear(col("fecha")))

dimtiempo.show(10)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# materializamos el df como tabla en lh_silver
dimtiempo.write.mode("overwrite").saveAsTable("lh_silver_Andreu_Cabero.dim_tiempo_fecha_hora")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
