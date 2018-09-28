from datetime import datetime
from datetime import timedelta

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import pyspark.sql.functions as Functions
from pyspark.sql.functions import col as col_, max as max_, min as min_, trunc as trunc_, datediff as datediff_
from pyspark.sql.functions import current_date as current_date_, avg as avg, date_trunc as truncd_, sum as sum_
from pyspark.sql.types import StructField, StructType, StringType, DateType, IntegerType

spark = SparkSession \
    .builder \
    .appName("Git Data Line Decay Processor") \
    .getOrCreate()

# might need the sql for better analysis
sqlContext = SQLContext(spark)

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

line_cohorts_df = line_decay_df.withColumn("create_cohort", truncd_("week", col_("created")))\
    .withColumn("remove_cohort", truncd_("week", col_("removed")))

created_in_cohorts_df = line_cohorts_df.groupBy("create_cohort")\
    .count()\
    .withColumnRenamed("count", "created_in_cohort")

removed_in_cohorts_df = line_cohorts_df.groupBy("create_cohort", "remove_cohort")\
    .count()\
    .withColumnRenamed("count", "removed_in_cohort")


removed_cohort_rows =  removed_in_cohorts_df.collect()
for row in removed_cohort_rows:
    removed_by_cohorts_df = removed_in_cohorts_df\
    .filter(col_("create_cohort") == row.create_cohort)\
    .filter(col_("remove_cohort") <= row.remove_cohort) \
    .agg(sum_("removed_in_cohort"))

    print "-----------"
    print "-----------"
    print "-----------"
    print "REMOVE COHORT: {0}, {1}".format(row.create_cohort, row.remove_cohort)
    removed_by_cohorts_df.show()
    print "-----------"
    print "-----------"
    print "-----------"


#join_cols = [removed_in_cohorts_df["create_cohort"], removed_in_cohorts_df["remove_cohort"]]
#report_df = line_cohorts_df.join(created_in_cohorts_df, "create_cohort", "left_outer")\
#    .join(removed_in_cohorts_df, ["create_cohort", "remove_cohort"], "left_outer")

"""
report_df = removed_in_cohorts_df.join(created_in_cohorts_df, "create_cohort", "left_outer")\
    .withColumn("decay_pct",\
                (\
                        (col_("created_in_cohort")-col_("removed_in_cohort"))/col_("created_in_cohort")\
                    *100))\
    .orderBy(col_("created_in_cohort"), col_("removed_in_cohort"))

report_df.show()
report_df.coalesce(1).write\
	.mode("overwrite")\
	.option("header", "true")\
	.csv("reports/decay_rate_by_line.cvs")


#row = date_range_df.collect()[0]
#first_create = row.first_create
#print "FC: {0}".format(first_create)
#print "FCPLUS: {0}".format(first_create + + timedelta(days=7))
"""
