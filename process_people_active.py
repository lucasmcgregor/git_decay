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
from pyspark.sql.functions import lag as lag_, when as when_, isnull as isnull_, countDistinct as countDistinct_

spark = SparkSession \
    .builder \
    .appName("Git Data Authors Active processor") \
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

lines_with_create_week_df = line_decay_input_df.withColumn("lifespan", clean_string_for_int_udf(col_("lifespan"))) \
    .withColumn("created", clean_string_for_date_udf(col_("created"))) \
    .withColumn("removed", clean_string_for_date_udf(col_("removed"))) \
    .withColumn("create_week", create_cohort_udf(col_("created"))) \
    .withColumn("remove_week", create_cohort_udf(col_("removed"))) \

creators_by_week_df = lines_with_create_week_df.select(lines_with_create_week_df.creator, lines_with_create_week_df.create_week)\
    .withColumnRenamed("creator", "person") \
    .withColumnRenamed("create_week", "week")

removers_by_week_df = lines_with_create_week_df.select(lines_with_create_week_df.creator, lines_with_create_week_df.create_week)\
    .withColumnRenamed("remover", "person") \
    .withColumnRenamed("remove_week", "week")

people_by_week = creators_by_week_df.union(removers_by_week_df)

active_people_by_week_df = people_by_week.groupBy(col_("week"))\
    .agg(countDistinct_(col_("person"))) \
    .orderBy(col_("week"))

active_people_by_week_df.show()

active_people_by_week_df.coalesce(1).write\
	.mode("overwrite")\
	.option("header", "true")\
	.csv("reports/active_people.csv")



