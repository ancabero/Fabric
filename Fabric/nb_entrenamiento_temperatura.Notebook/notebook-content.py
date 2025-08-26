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
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# modelo predictivo de temperatura basado en datos historicos

# importar librerias y cargar datos
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year

# leer la tabla con los datos de entrenamiento
df = spark.read.table("ml_train_temperaturas")

df_convertido = (

    df.withColumn("anno", year(col("fecha")))

)

display(df_convertido)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# paso 2: ingenieria de variables (lags)
from pyspark.sql.window import Window
from pyspark.sql.functions import lag

window_spec = Window.partitionBy("nombreEstacion").orderBy("fecha")

for col_name in ["Tmax", "Tmin"]:
    df_convertido = (
        df_convertido.withColumn(f"{col_name}_lag1", lag(col_name, 1).over(window_spec))
                     .withColumn(f"{col_name}_lag2", lag(col_name, 2).over(window_spec))
    )

# limpiamos los nulos eventuales
df_ml = df_convertido.dropna()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************



#Paso 3: Vector Assembler y modelos

from pyspark.ml.feature import VectorAssembler

from pyspark.ml.regression import LinearRegression

features = [

    "Tmax_lag1", "Tmax_lag2",

    "Tmin_lag1", "Tmin_lag2"

]

assembler = VectorAssembler(inputCols=features , outputCol="features")

df_ml = assembler.transform(df_ml)

lr_tmax = LinearRegression(featuresCol="features", labelCol="Tmax")

lr_tmin = LinearRegression(featuresCol="features", labelCol="Tmin")

model_tmax = lr_tmax.fit(df_ml)

model_min = lr_tmin.fit(df_ml)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CREAR FECHAS FUTURAS A PARTIR DE HOY

from pyspark.sql.functions import sequence, date_add, current_date, explode, expr 

fechas_futuras = (

    spark.sql("SELECT current_date() AS today")

    .select(explode(sequence( date_add(col("today"), 1),date_add(col("today"), 730),expr("interval 1 day"))).alias("fecha"))

    .toPandas()["fecha"]

)

display(fechas_futuras)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
