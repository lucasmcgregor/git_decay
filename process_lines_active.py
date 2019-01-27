from datetime import datetime
#import datetime
from datetime import timedelta

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import pyspark.sql.functions as Functions
from pyspark.sql.functions import col as col_, max as max_, min as min_, trunc as trunc_, datediff as datediff_
from pyspark.sql.functions import current_date as current_date_, avg as avg_, date_trunc as truncd_, sum as sum_
from pyspark.sql.types import StructField, StructType, StringType, DateType, IntegerType
from pyspark.sql.window import Window
from pyspark.sql.functions import lag as lag_, when as when_, isnull as isnull_

spark = SparkSession \
    .builder \
    .appName("Git Data Lines Active processor") \
    .getOrCreate()

data_window=7

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



def create_cohort(date):


    rv = -1
    if date is not None:
        year = date.strftime("%y")
        week = "%02d" % int(date.strftime("%W"))
        rv = int("{0}{1}".format(year,week))

    return rv





clean_string_for_int_udf = Functions.UserDefinedFunction(clean_string_for_int, IntegerType())
clean_string_for_date_udf = Functions.UserDefinedFunction(clean_string_for_date, DateType())
create_cohort_udf = Functions.UserDefinedFunction(create_cohort, IntegerType())


schema = StructType([ \
    StructField("path", StringType()), \
    StructField("creator", StringType()), \
    StructField("remover", StringType()), \
    StructField("created", StringType()), \
    StructField("removed", StringType()), \
    StructField("lifespan", StringType()), \
    ])

line_decay_input_df = spark.read.csv( \
    "output/line_decay.csv", \
    header=True, \
    mode="DROPMALFORMED", \
    schema=schema \
    )

line_decay_with_dates_df = line_decay_input_df.withColumn("lifespan", clean_string_for_int_udf(col_("lifespan"))) \
    .withColumn("created", clean_string_for_date_udf(col_("created"))) \
    .withColumn("removed", clean_string_for_date_udf(col_("removed"))) \
    .withColumn("create_week", create_cohort_udf(col_("created"))) \
    .withColumn("remove_week", create_cohort_udf(col_("removed")))

create_window = Window.partitionBy().orderBy(col_("create_week"))
lines_created_on_week_df = line_decay_with_dates_df.filter(col_("create_week") != -1) \
    .groupBy(col_("create_week")) \
    .count() \
    .withColumnRenamed("count", "lines_created") \
    .withColumn("running_created", sum_(col_("lines_created")).over(create_window)) \
    .withColumnRenamed("create_week", "week")


#lines_created_on_week_df.show()


remove_window = Window.partitionBy().orderBy(col_("remove_week"))
lines_removed_on_week_df = line_decay_with_dates_df.filter(col_("remove_week") != -1)\
    .groupBy(col_("remove_week")) \
    .count() \
    .withColumnRenamed("count", "lines_removed") \
    .withColumn("running_removed", sum_(col_("lines_removed")).over(remove_window)) \
    .withColumnRenamed("remove_week", "week")

lines_removed_on_week_df.show()

active_lines_df = lines_created_on_week_df.join(lines_removed_on_week_df, "week", "left_outer") \
   .withColumn("active_lines", (col_("running_created") - col_("running_removed")))

active_lines_df.show()

active_lines_df.coalesce(1).write\
	.mode("overwrite")\
	.option("header", "true")\
	.csv("reports/active_lines.cvs")



