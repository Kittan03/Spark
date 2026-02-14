from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder.appName("Daniel_Comercio360_Final").getOrCreate()

BUCKET = "bucketdanielhernan"
PATH_RAW = f"s3a://{BUCKET}/comercio360/alumnoDaniel/raw/"
PATH_OUT = f"s3a://{BUCKET}/comercio360/alumnoDaniel/resultados/"

# 1. Definir esquema manual de texto para evitar errores de formato en la carga
schema_orders = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("store_id", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("payment_method", StringType(), True)
])

# 2. Cargar como texto y filtrar el valor "60s"
orders_raw = spark.read.csv(PATH_RAW + "orders.csv", header=True, schema=schema_orders)
orders = orders_raw.filter(~F.concat_ws(" ", *orders_raw.columns).contains("60s"))

# 3. Castear a tipos correctos tras la limpieza
orders = orders.withColumn("order_id", F.col("order_id").cast("int")) \
               .withColumn("order_date", F.to_date("order_date"))

# Cargar resto de tablas
items = spark.read.csv(PATH_RAW + "order_items.csv", header=True, inferSchema=True)
products = spark.read.csv(PATH_RAW + "products.csv", header=True, inferSchema=True)

# --- Ejecución de Consultas (A, B, C) ---
# [El resto del código de las consultas se mantiene igual]

# Guardar resultados
res_a.write.mode("overwrite").parquet(PATH_OUT + "consulta_A")
res_b.write.mode("overwrite").parquet(PATH_OUT + "consulta_B")
res_c.write.mode("overwrite").parquet(PATH_OUT + "consulta_C")

spark.stop()