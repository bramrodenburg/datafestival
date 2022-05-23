# Databricks notebook source
import dlt

# COMMAND ----------

import pyspark.sql.functions as f

# COMMAND ----------

#spark.readStream.format("cloudFiles").option("cloudFiles.format", "text").option("cloudFiles.schemaLocation", "/tmp/test.schema").load("/datafestival/landing/weather_reports/*.csv")

# COMMAND ----------

@dlt.view(comment="Landed weather reports in CSV format.")
def weather_reports_landing():
  return spark.readStream.format("cloudFiles").option("cloudFiles.format", "text").load("/datafestival/landing/weather_reports/*.csv")

# COMMAND ----------

@dlt.table(comment="Bronze weather reports")
def weather_reports_bronze():
  df = dlt.read_stream("weather_reports_landing")
  
  return df.select(
    f.col("value").alias("input_row"),
    f.input_file_name().alias("m_processed_file"),
    f.current_timestamp().alias("m_processed_timestamp")
  )

# COMMAND ----------

@dlt.table(comment="Silver weather reports")
def weather_reports_silver():
  #df = spark.table("datafestival.weather_reports_bronze")
  df = dlt.read_stream("weather_reports_bronze")
  schema = "State STRING, Date DATE, MaxTemperature INT, MeanTemperature INT, MinTemperature INT, DewPoint INT, MeanDewPoint INT, MinDewPoint INT, MaxHumidity INT, MeanHumidity INT, MaxSeaLevelpressureHPA INT, MeanSeaLevelPressureHPA INT, MinSeaLevelPressureHPA INT, MaxVisibilityKM INT, MeanVisibilityKM INT, MinVisibilityKM INT, MaxWindSpeed INT, MeanWindSpeed INT, MaxGustSpeed INT, Precipitation INT, CloudCover INT, Events STRING, WindDirDegrees INT"
  
  return (df.select(f.from_csv(f.col("input_row"), schema).alias("parsed_row"))  # Parse bronze schema
            .select("parsed_row.*")  # Struct to columns
            .filter(f.col("State") != "file")  # Drop random headers
         )

# COMMAND ----------

#display(weather_reports_silver())
