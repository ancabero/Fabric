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
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# creamos agregaciones desde la tabla de hechos
mensual = spark.sql(
    """
    SELECT 
    year (fecha) as anno,
    month (fecha) AS mes,
    CodigoNacional,
    avg (Tmax) as p_tmax,
    avg (Tmin) as p_tmin
    FROM lh_silver_Andreu_Cabero.ft_temperaturas_estacion_dia
    GROUP BY 
    year (fecha), month (fecha), CodigoNacional
    """
)

mensual.show(5)

# guardamos la tabla de hechos agregada
mensual.write.mode("overwrite").saveAsTable("lh_silver_Andreu_Cabero.ft_temperaturas_estacion_mes")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
