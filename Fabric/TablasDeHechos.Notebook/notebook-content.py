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

# Exploramos datos para crear tabla de hechos

# temperaturas maximas
tmax = spark.sql(
    """
    SELECT date(time) as fecha, txPM, CodigoNacional FROM lh_bronze_Andreu_Cabero.tempmax
    """
)
tmax.show(5)


# temperaturas minimas
tmin = spark.sql(
    """
    SELECT date(time) as fecha, tnAM, CodigoNacional FROM lh_bronze_Andreu_Cabero.tempmin
    """
)
tmin.show(5)


# creamos tabla de hechos inferida por union de consultas
temperaturas = spark.sql(
    """
    SELECT date(A.time) as fecha, A.txPM as Tmax, B.tnAM as Tmin, cast(A.CodigoNacional AS int)
    FROM lh_bronze_Andreu_Cabero.tempmax A
    JOIN lh_bronze_Andreu_Cabero.tempmin B
        ON A.CodigoNacional = B.CodigoNacional
        AND date(A.time) = date(B.time);
    """
)
temperaturas.show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# guardo la tabla de hecho en lh_silver
temperaturas.write.mode("overwrite").option("overwriteschema", "true").saveAsTable("lh_silver_Andreu_Cabero.ft_temperaturas_estacion_dia")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
