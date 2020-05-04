#!/usr/bin/env python
# coding: utf-8


try:
    get_ipython().system('pip install pyspark=="2.4.5"  --quiet')
except:
    print("Running throw py file.")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession\
        .builder\
        .appName("Este eh uma job de processamento")\
        .getOrCreate()

#dataFrame de station.csv
dfs = spark.read.format("com.databricks.spark.csv")\
    .option("header","true").option("delimiter",",")\
    .option("inferSchema", "true")\
    .load("s3://tembici-fk/data//station.csv") 
    
#dataFrame de trip.csv
dft = spark.read.format("com.databricks.spark.csv")\
    .option("header","true").option("delimiter",",")\
    .option("inferSchema", "true")\
    .load("s3://tembici-fk/data//trip.csv") 
    
#dataFrame de weather.csv
dfw = spark.read.csv("s3://tembici-fk/data/weather.csv", header = True, sep = ",")


#conversao de colunas [starttime, stoptime] do tipo String para o tipo timestamp
dft = dft.withColumn('tripdate', F.date_format(F.to_timestamp('starttime', 'MM/dd/yyyy HH:mm'), 'MM/dd/yyyy'))\
         .withColumn('starttime', F.to_timestamp('starttime', 'MM/dd/yyyy HH:mm'))\
         .withColumn('stoptime', F.to_timestamp('stoptime', 'MM/dd/yyyy HH:mm'))

# conversao da coluna Date do tipo String para o tipo date, em seguida formatado novamente para o tipo String
# pois normaliza o formato de data de mês e dia com dois algarismos. (Ex. 1/2/2020 para 01/02/2020)
dfw = dfw.withColumn(  'Date', F.date_format(F.to_date('Date', 'MM/dd/yyyy'), 'MM/dd/yyyy'))


#Criação de Views para representação dos DataFrames
dfs.createOrReplaceTempView("station")
dft.createOrReplaceTempView("trip")
dfw.createOrReplaceTempView("weather")


# query da saida
dfOut = spark.sql("""
WITH local as (
    SELECT station_id, lat, long FROM station
), --
meteorology as (
    SELECT Date, Events FROM weather
)
SELECT t.trip_id, t.bikeid, -- id 
        /********** RELATED TO TRIP TIME *************/
        t.starttime, t.stoptime,  t.tripduration,
        CASE WHEN t.tripduration >= 600 THEN
            'true'
        ELSE
            'false'
        END long_trip,
        /********** RELATED TO TRIP ORIGIN & DESTIN *************/
        t.from_station_id, t.from_station_name, lf.lat from_lat, lf.long from_long,--origin
        t.to_station_id, t.to_station_name, lt.lat to_lat, lt.long to_long, -- destin
        /********** RELATED TO USER *************/
        t.usertype, t.gender, t.birthyear,
        CASE WHEN YEAR(current_date()) - t.birthyear <= 16 THEN 1
             WHEN YEAR(current_date()) - t.birthyear <= 25 THEN 2
             WHEN YEAR(current_date()) - t.birthyear <= 50 THEN 3
             WHEN YEAR(current_date()) - t.birthyear > 50 THEN 4
        ELSE
            null
        END age_range,  
        /********** RELATED TO WEATHER *************/
        met.Events weatherDay

FROM trip t, local lf, local lt, meteorology met
WHERE 1=1
      AND t.from_station_id = lf.station_id
      AND t.to_station_id = lt.station_id
      AND t.tripdate = met.Date
ORDER BY t.starttime
 """)


#Exportando Arquivo
dfOut.coalesce(1).write.parquet("s3://tembici-fk/output/trips")

