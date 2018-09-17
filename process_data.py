from datetime import date
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import DataFrameReader
import pyspark.sql.functions as Functions
from pyspark.sql.functions import col as col_, max as max_, min as min_, trunc as trunc_, datediff as datediff_
from pyspark.sql.functions import current_date as current_date_
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

    date = None
    try:
        date = datetime.strptime(string, '%Y-%m-%d %H:%M:%S')
    except:
        print "error in parsing date: " + string

    return date


spark = SparkSession \
    .builder \
    .appName("Git Data Processor") \
    .getOrCreate()
sqlContext = SQLContext(spark)

clean_string_for_int_udf = Functions.UserDefinedFunction(clean_string_for_int, IntegerType())
clean_string_for_date_udf = Functions.UserDefinedFunction(clean_string_for_date, DateType())


schema = StructType([ \
    StructField("path", StringType()), \
    StructField("creator", StringType()), \
    StructField("remover", StringType()), \
    StructField("created", StringType()), \
    StructField("removed", StringType()), \
    StructField("lifespan", StringType()), \
    ])

line_decay_df = spark.read.csv( \
    "output/line_decay.csv", \
    header=True, \
    mode="DROPMALFORMED", \
    schema=schema \
    )


line_decay_df = line_decay_df.withColumn("lifespan", clean_string_for_int_udf(col_("lifespan")))
line_decay_df = line_decay_df.withColumn("created", clean_string_for_date_udf(col_("created")))
line_decay_df = line_decay_df.withColumn("removed", clean_string_for_date_udf(col_("removed")))
#line_decay_df.show()
#print line_decay_df.schema

#all_lines_by_creator = line_decay_df.groupBy(col_("creator"), \
#                                             trunc_(col_("created"), 'mon').alias("cohort"))\
#    .count()\
#    .withColumnRenamed("count", "lines_created")


total_active_lines = line_decay_df.filter(col_("removed").isNull()).count()

all_lines_by_creator = line_decay_df.groupBy(col_("creator"))\
    .count()\
    .withColumnRenamed("count", "lines_created")

author_start = line_decay_df.groupBy(col_("creator"))\
    .agg(min_(col_("created")))\
    .withColumnRenamed("min(created)", "author_first")

author_last = line_decay_df.groupBy(col_("creator"))\
    .agg(max_(col_("created")))\
    .withColumnRenamed("max(created)", "author_last")

removed_lines_by_creator = line_decay_df.filter(col_("removed").isNotNull())\
    .groupBy(col_("creator"))\
    .count()\
    .withColumnRenamed("count", "lines_removed")

active_lines_by_creator = line_decay_df.filter(col_("removed").isNull())\
    .groupBy(col_("creator"))\
    .count()\
    .withColumnRenamed("count", "lines_active")

self_removed_lines_by_creator = line_decay_df.filter(col_("removed").isNotNull())\
    .filter(col_("remover") == col_("creator"))\
    .groupBy(col_("creator"))\
    .count()\
    .withColumnRenamed("count", "lines_removed_by_author")


decay_by_creator = all_lines_by_creator.join(removed_lines_by_creator, "creator", "left_outer")\
    .join(author_start, "creator", "left_outer")\
    .join(author_last, "creator", "left_outer") \
    .join(self_removed_lines_by_creator, "creator", "left_outer") \
    .join(active_lines_by_creator, "creator", "left_outer") \
    .withColumn("account_age", datediff_(current_date_(), col_("author_first")))\
    .withColumn("account_lifespan", datediff_(col_("author_last"), col_("author_first")))\
    .withColumn("account_dormant_for", datediff_(current_date_(), col_("author_last")))\
    .withColumn("pct_removed", (col_("lines_removed") / col_("lines_created")))\
    .withColumn("pct_removed_by_self", (col_("lines_removed_by_author") / col_("lines_created")))\
    .withColumn("pct_of_active_code_base", (col_("lines_active") / total_active_lines))\
    .orderBy(col_("pct_removed"))

decay_by_creator.coalesce(1).write\
	.mode("overwrite")\
	.option("header", "true")\
	.csv("reports/decay_rate_by_user.cvs")
