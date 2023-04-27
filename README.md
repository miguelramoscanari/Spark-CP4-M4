# Spark CP4 M4
Resolucion CP M4 con SPARK SQL Y PYSPARK usando Databrics Community que es gratis.

# Contenido
- "airports.csv", "raw-flight-data.snappy.parquet": fuente de datos
- "cp_m4_examen_miguel.ipynb": notebook con el codigo fuente

# Databrics
- Crear un cluster, usando Compute
- Abrir notebook ""cp_m4_examen.ipynb", usando Worrksapce / import / File
- En el Notebook, usar menu File / Upload data DBFS para añair "airports.csv", y "raw-flight-data.snappy.parquet"

# Explicacion del codigo
Usando Spark, creamos dos dataframes
```
df1 = spark.read.parquet("dbfs:/FileStore/shared_uploads/tacnampt@gmail.com/raw_flight_data_snappy.parquet")
df2 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/tacnampt@gmail.com/airports.csv")
```
Para ver el esquema del dataframe "flight"
```
df1.printSchema()
df1.show(2)
```
```
root
 |-- DayofMonth: integer (nullable = true)
 |-- DayOfWeek: integer (nullable = true)
 |-- Carrier: string (nullable = true)
 |-- OriginAirportID: integer (nullable = true)
 |-- DestAirportID: integer (nullable = true)
 |-- DepDelay: integer (nullable = true)
 |-- ArrDelay: integer (nullable = true)
```
```
+----------+---------+-------+---------------+-------------+--------+--------+
|DayofMonth|DayOfWeek|Carrier|OriginAirportID|DestAirportID|DepDelay|ArrDelay|
+----------+---------+-------+---------------+-------------+--------+--------+
|        30|        4|     UA|          13930|        10721|      -3|      -7|
|        30|        4|     UA|          11618|        12892|      -1|     -28|
+----------+---------+-------+---------------+-------------+--------+--------+
```
Para ver el esquema del dataframe "airport"
```
df2.printSchema()
df2.show(2)
```
```
root
 |-- airport_id: string (nullable = true)
 |-- city: string (nullable = true)
 |-- state: string (nullable = true)
 |-- name: string (nullable = true)
```
```
+----------+-----------+-----+--------------------+
|airport_id|       city|state|                name|
+----------+-----------+-----+--------------------+
|     10165|Adak Island|   AK|                Adak|
|     10299|  Anchorage|   AK|Ted Stevens Ancho...|
+----------+-----------+-----+--------------------+
```
Observamos que el dataframe "flight", los tipos de datos son correctos, al igual que el dataframe "airport".

Ahora creamos VISTA por cada DataFrame, las vista son como tablas que podemos hacer INNER JOIN
```
df1.createOrReplaceTempView("flights")
df2.createOrReplaceTempView("airports")
```
17) ¿Cuál es la tupla de aeropuertos, con mayor cantidad de vuelos entre sí?
Nota: Es posible tomar el nombre del aeropuerto desde el archivo "airports.csv", donde "airport_id" se puede relacionar con "OriginAirportID" y "DestAirportID" de la tabla "flights"

1- Honolulu International - Kahului Airport

2- San Francisco International - Los Angeles International

3- Los Angeles International - McCarran International

Resolviendo usando SPARK SQL:
```
spark.sql(""" 
SELECT a1.name as origen, a2.name as destino, COUNT(*) as cantidad
FROM flights f
INNER JOIN airports a1 ON f.OriginAirportID = a1.airport_id
INNER JOIN airports a2 ON f.DestAirportID = a2.airport_id
GROUP BY a1.name, a2.name
ORDER BY 3 DESC
LIMIT 5
           """).show(truncate=False)
```
```
+---------------------------+---------------------------+--------+
|origen                     |destino                    |cantidad|
+---------------------------+---------------------------+--------+
|San Francisco International|Los Angeles International  |9367    |
|Los Angeles International  |San Francisco International|9306    |
|Kahului Airport            |Honolulu International     |6891    |
|Los Angeles International  |McCarran International     |6861    |
|Honolulu International     |Kahului Airport            |6856    |
+---------------------------+---------------------------+--------+
```

Ahora resolviendo usando PYSPARK:
```
from pyspark.sql.functions import sum,avg,max,count
from pyspark.sql.functions import col

df1.alias("f") \
    .join(df2.alias("a1"), col("f.OriginAirportID") == col("a1.airport_id"), "inner") \
    .join(df2.alias("a2"), col("f.DestAirportID") == col("a2.airport_id"), "inner") \
    .select(col("a1.name").alias("origen"), col("a2.name").alias("destino")) \
    .groupBy(col("origen"), col("destino")) \
    .agg(count("*").alias("cantidad")) \
    .orderBy(col("cantidad"), ascending = False) \
    .show(5, truncate=False)
```
```
+---------------------------+---------------------------+--------+
|origen                     |destino                    |cantidad|
+---------------------------+---------------------------+--------+
|San Francisco International|Los Angeles International  |9367    |
|Los Angeles International  |San Francisco International|9306    |
|Kahului Airport            |Honolulu International     |6891    |
|Los Angeles International  |McCarran International     |6861    |
|Honolulu International     |Kahului Airport            |6856    |
+---------------------------+---------------------------+--------+
```
En ambos casos nos da el mismo resultado.
