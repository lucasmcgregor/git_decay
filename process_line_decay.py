from datetime import datetime
#import datetime
from datetime import timedelta

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import pyspark.sql.functions as Functions
from pyspark.sql.functions import col as col_, max as max_, min as min_, trunc as trunc_, datediff as datediff_, count as count_
from pyspark.sql.functions import lit as lit_
from pyspark.sql.functions import current_date as current_date_, avg as avg_, date_trunc as truncd_, sum as sum_, lag as lag_
from pyspark.sql.types import StructField, StructType, StringType, DateType, IntegerType, TimestampType
from pyspark.sql.window import Window

import math
import sys

spark = SparkSession \
    .builder \
    .appName("Git Data Line Decay Processor") \
    .getOrCreate()

# might need the sql for better analysis
#sparkContext = SparkContext(spark)
#sqlContext = SQLContext(spark)

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

def days_between(start, end):

    if start is None:
        return -1

    if end is None:
        return -2

    #print "   ::: C:{0}:{1}, R:{2}:{3} -- {4}".format(start, type(start), end, type(end), isinstance(end, datetime.date))

    #if not isinstance(start, datetime.date) or not isinstance(end, datetime.date):
    #    return -3

    return int((start - end).days)



def minutes_between(start, end):
    """
    if either time is none, or they are the same times; it will return a None
    else it will return the minutes between start - end

    :param start: start of the time window as a datetime
    :param end: end of the window as a datetime
    :return: minutes between
    """

    if start is None:
        return None

    if end is None:
        return None

    if start == end:
        return None

    td = start - end
    return int(td.seconds / 60)


def create_cohort(date, path, user):

    if date is not None:
        return date.strftime("%W_%y")
    else:
        return "1"

def create_cohort_id(date):

    rv = -1
    if date is not None:
        year = date.strftime("%y")
        week = "%02d" % int(date.strftime("%W"))
        rv = int("{0}{1}".format(year,week))

    return rv




clean_string_for_int_udf = Functions.UserDefinedFunction(clean_string_for_int, IntegerType())
clean_string_for_date_udf = Functions.UserDefinedFunction(clean_string_for_date, TimestampType())
days_between_udf = Functions.UserDefinedFunction(days_between, IntegerType())
create_cohort_id_udf = Functions.UserDefinedFunction(create_cohort_id, IntegerType())
minutes_between_udf = Functions.UserDefinedFunction(minutes_between, IntegerType())
#create_cohort_udf = Functions.UserDefinedFunction(create_cohort, StringType())


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


line_decay_df = line_decay_input_df.withColumn("lifespan", clean_string_for_int_udf(col_("lifespan"))) \
    .withColumn("created", clean_string_for_date_udf(col_("created"))) \
    .withColumn("removed", clean_string_for_date_udf(col_("removed"))) \
    .withColumn("create_cohort", create_cohort_id_udf(col_("created"))) \
    .withColumn("remove_cohort", create_cohort_id_udf(col_("removed"))) \


create_window = Window.partitionBy().orderBy("created")
line_decay_df = line_decay_df.withColumn("prev_created", lag_(line_decay_df.created).over(create_window)) \
    .withColumn("time_between", minutes_between_udf(col_("created"), col_("prev_created")))

line_decay_df.show()



created_in_cohorts_df = line_decay_df.groupBy("create_cohort")\
    .agg( \
        count_(lit_(1)).alias("total_in_cohort"),\
        avg_(col_("time_between")).alias("avg_time_between")\
        )

removed_in_cohorts_df = line_decay_df.groupBy("create_cohort", "remove_cohort")\
    .count()\
    .withColumnRenamed("count", "removed_in_this_cohort")

lifespand_days_in_cohort_df = line_decay_df.groupBy("create_cohort", "remove_cohort")\
    .agg(max_(col_("lifespan")))\
    .withColumnRenamed("max(lifespan)", "lifespan") \


lifespand_days_in_cohort_df.show()

removed_cohorts_with_lifespan_df = removed_in_cohorts_df.join(lifespand_days_in_cohort_df, \
                                                              (removed_in_cohorts_df.create_cohort == lifespand_days_in_cohort_df.create_cohort) & \
                                                              (removed_in_cohorts_df.remove_cohort == lifespand_days_in_cohort_df.remove_cohort)) \
        .drop(lifespand_days_in_cohort_df.create_cohort) \
        .drop(lifespand_days_in_cohort_df.remove_cohort)

cohort_df = created_in_cohorts_df.join(removed_cohorts_with_lifespan_df, "create_cohort", "left_outer") \
    .orderBy("create_cohort", "lifespan")

raw_data = cohort_df.collect()
processed_data = []
decay_data = []
previous_create_cohort = None
total_removed_by_this_cohort = 0
decay_rate = 0
## data should be ordered by:
## creation_cohort
## and lifespan (0...)
for row in raw_data:
    row_dict = row.asDict()

    ## if we enter a new creation cohort
    ## start counting the removals
    if row_dict["create_cohort"] != previous_create_cohort:
        if previous_create_cohort is not None:
            decay_row = {"week":previous_create_cohort, "decay_rate":decay_rate}
            decay_data.append(decay_row)

        previous_create_cohort = row_dict["create_cohort"]
        total_removed_by_this_cohort = 0


    total_removed_by_this_cohort = total_removed_by_this_cohort + int(row_dict["removed_in_this_cohort"])

    print "RUNNING TOTS: {0}:{1}  {2} -> {3}".format(row_dict["create_cohort"], previous_create_cohort, row_dict["removed_in_this_cohort"], total_removed_by_this_cohort)

    row_dict["total_removed_by_this_cohort"] = total_removed_by_this_cohort

    row_dict["pct_sliding_decay"] = (float(row_dict["removed_in_this_cohort"]) / float(row_dict["total_in_cohort"]) ) * 100
    row_dict["pct_total_decay"] = (float(total_removed_by_this_cohort) / float(row_dict["total_in_cohort"])) * 100

    try:
        initial = row_dict["total_in_cohort"]
        lost = total_removed_by_this_cohort
        time = row_dict["lifespan"]
        if time < 1:
            time = 1
        decay_rate = (math.log((float(initial) - float(lost)) / float(initial)) / float(time)) * 100000
        #decay_rate = "{0}".format(decay_rate)
        print "THE DECAY RATE: '{0}'".format(decay_rate)
    except Exception as e:
        print sys.exc_value
        print "DECAY RATE FAILED: {0},{1},{2}".format(initial, lost, time)
        #decay_rate = -1

    row_dict["decay_rate"] = decay_rate
    processed_data.append(row_dict)

processed_data_rdd = spark.sparkContext.parallelize(processed_data)
processed_data_df = spark.createDataFrame(processed_data_rdd)\
    .select(col_("create_cohort"), \
            col_("remove_cohort"), \
            col_("lifespan"), \
            col_("avg_time_between"), \
            col_("total_in_cohort"), \
            col_("removed_in_this_cohort"), \
            col_("total_removed_by_this_cohort"), \
            col_("pct_sliding_decay"), \
            col_("pct_total_decay"),\
            col_("decay_rate"))\
    .orderBy(col_("create_cohort"), col_("lifespan"))


processed_data_df.coalesce(1).write\
	.mode("overwrite")\
	.option("header", "true")\
    .option("inferSchema", "true")\
	.csv("reports/decay_rate_by_line.cvs")


decay_data_rdd = spark.sparkContext.parallelize(decay_data)
decay_data_df = spark.createDataFrame(decay_data_rdd)\
    .orderBy("week")

decay_data_df.coalesce(1).write\
	.mode("overwrite")\
	.option("header", "true")\
    .option("inferSchema", "true")\
	.csv("reports/decay_rate_by_week.cvs")



