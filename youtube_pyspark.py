from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

JSON_INPUT = '/content/IN_category_id.json'
PRE_PRO_JSON = '/content/invideos.parquet'
CSV_INPUT= '/content/INvideos.csv'
FINAL_OUTPUT= '/content/final'
PFINAL_OUTPUT= '/content/pfinal'

def pre_process_json(spark,JSON_INPUT):
  json=spark.read.format("json").option('multiline','true').load(JSON_INPUT)
  exploded_json=json.select(explode(col('items')).alias('new'))
  formatted_json=exploded_json.select(col('new').getItem('id').alias('id'),             \
         col('new').getItem('kind').alias('kind'),                                      \
         col('new').getItem('snippet').getItem('assignable').alias('assignable'),       \
         col('new').getItem('snippet').getItem('channelId').alias('channelId'),         \
         col('new').getItem('snippet').getItem('title').alias('tile')
         )
  #formatted_json.show()
  formatted_json.write.mode('overwrite').format('parquet').save(PRE_PRO_JSON )

def process_csv(spark, CSV_INPUT):
  csv=spark.read.format('csv').option('inferschema','true').option('header','true').load(CSV_INPUT)
  clean_csv=csv.na.drop('any')
  clean_csv=clean_csv.withColumn('views',col('views').cast('int'))
  #clean_csv.printSchema()
  return clean_csv

def process_data(spark,clean_json,clean_csv):
  join=clean_json.join(clean_csv,col('category_id')==col('id'))
  join.count()
  window= Window.partitionBy(col('tile')).orderBy(col('views').desc())
  window_join= join.withColumn('rank',rank().over(window))
  window_join.count()
  top_10_views = window_join.filter(col('rank') <= 10)
  #print('top count', top_10_views.count() )
  top_10_views= top_10_views.drop(col('rank'))

  return top_10_views

def main():
  
  #creating spark Driver
  spark= SparkSession.builder.appName('Youtube_DE').getOrCreate()

  #preprocess the JSON to extact the schema in usable format and save in it a location
  pre_process_json(spark,JSON_INPUT)
  
  #to remove nulls and typecast the key fields
  clean_csv=process_csv(spark, CSV_INPUT)

  #load the cleaned JSON
  clean_json=spark.read.load(PRE_PRO_JSON)

  #Perform the actual transformation  and aggregation to get the top 10 most viewed videos of youtube.
  top_10_views= process_data(spark,clean_json,clean_csv)

  top_10_views.write.format('csv').option('header','true').mode('overwrite').partitionBy('tile').save(PFINAL_OUTPUT)

  top_10_views.write.format('csv').option('header','true').mode('overwrite').save(FINAL_OUTPUT)


if __name__ == '__main__':
  main()