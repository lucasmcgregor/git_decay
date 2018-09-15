from datetime import date
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import DataFrameReader
import pyspark.sql.functions as Functions
from pyspark.sql.functions import col as col_, max as max_, min as min_, trunc as trunc_
from pyspark.sql.types import StructField, StructType, StringType, DateType, IntegerType


def clean_string_for_int(string):
	if string is None:
		return 0

	s = string.strip()
	if not s.isdigit():
		return -1

	return int(s)

def clean_string_for_date(string):
    if string is None:
        return None

    if string == "None":
        return None

    # sample string = 2018-09-14 00:00:00
    # 'Jun 1 2005  1:33PM' == '%b %d %Y %I:%M%p'
    date = datetime.strptime(string, '%Y-%m-%d %H:%M:%S')

    datetime_object = datetime.strptime('Jun 1 2005  1:33PM', '%b %d %Y %I:%M%p')

spark = SparkSession \
    .builder \
    .appName("Git Data Processor") \
    .getOrCreate()
#sqlContext = SQLContext(spark)

clean_string_for_int_udf = Functions.UserDefinedFunction(clean_string_for_int, IntegerType())

schema = StructType([\
    StructField("Path", StringType()),\
    StructField("Creator", StringType()),\
    StructField("Remover", StringType()),\
    StructField("Created", StringType()),\
    StructField("Removed", StringType()),\
    StructField("Lifespan", StringType()),\
    ])

line_decay_df = spark.read.csv(\
    "output/line_decay.csv",\
    header=True,\
    mode="DROPMALFORMED",\
    schema=schema\
    )

#line_decay_df = sqlContext.read.load('output/line_decay.csv', 
#                          format='com.databricks.spark.csv', 
#                          header='true', 
#                          inferSchema='true')

#line_decay_df = spark.read.csv('output/line_decay.csv', header=True, inferSchema=True)

line_decay_df = line_decay_df.withColumn("Lifespan", clean_string_for_int_udf(col_("Lifespan")))
line_decay_df.show()
print line_decay_df.schema

#sqlContext = SQLContext(spark)

## regsiter  the UDFs
#agebucket_udf = Functions.UserDefinedFunction(birthdate_to_age_bucket, Types.StringType())




#report_df.coalesce(1).write\
#	.mode("overwrite")\
#	.option("header", "true")\
#	.csv("report.cvs")



