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
    if start is None or end is None:
        return -1

    if datetime != type(start) or datetime != type(end):
        return -1

    return (start - end).days

def create_cohort(date, path, user):

    if date is not None:
        return date.strftime("%W_%y")
    else:
        return "1"




clean_string_for_int_udf = Functions.UserDefinedFunction(clean_string_for_int, IntegerType())
clean_string_for_date_udf = Functions.UserDefinedFunction(clean_string_for_date, DateType())
days_between_udf = Functions.UserDefinedFunction(days_between, IntegerType())
create_cohort_udf = Functions.UserDefinedFunction(create_cohort, StringType())


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


line_decay_df = line_decay_df.withColumn("lifespan", clean_string_for_int_udf(col_("lifespan"))) \
    .withColumn("created", clean_string_for_date_udf(col_("created"))) \
    .withColumn("removed", clean_string_for_date_udf(col_("removed"))) \
    .withColumn("create_cohort", create_cohort_udf(col_("created"), col_("path"), col_("creator"))) \
    .withColumn("remove_cohort", create_cohort_udf(col_("removed"), col_("path"), col_("creator")))

created_in_cohorts_df = line_decay_df.groupBy("create_cohort")\
    .count()\
    .withColumnRenamed("count", "created_in_cohort")

removed_in_cohorts_df = line_decay_df.groupBy("create_cohort", "remove_cohort")\
    .count()\
    .withColumnRenamed("count", "removed_in_cohort")


report_df = created_in_cohorts_df.join(removed_in_cohorts_df, "create_cohort", "left_outer") \
    .withColumn("days_in_cohort", days_between_udf(col_("remove_cohort"), col_("create_cohort"))) \
    .withColumn("sliding_decay_pct", \
                ((col_("created_in_cohort") - col_("removed_in_cohort")) / col_("created_in_cohort") \
                 * 100)) \
    .orderBy(col_("create_cohort"), col_("remove_cohort"))

raw_data = report_df.collect()
processed_data = []
running_decay_for_cohort = float(0)
total_removed_in_cohort = float(0)

for row in raw_data:
    row_dict = row.asDict()

    if row_dict["remove_cohort"] is None:
        total_removed_in_cohort = float(0)
    else:
        total_removed_in_cohort += float(row_dict["removed_in_cohort"])
        created_in_cohort = float(row_dict["created_in_cohort"])
        running_decay_for_cohort = ((created_in_cohort - total_removed_in_cohort) / created_in_cohort) * 100
        row_dict["running_decay_for_cohort"] = running_decay_for_cohort
        row_dict["total_removed_in_cohort"] = total_removed_in_cohort

        processed_data.append(row_dict)



processed_data_rdd = spark.sparkContext.parallelize(processed_data)
processed_data_df = spark.createDataFrame(processed_data_rdd)\
    .orderBy(col_("create_cohort"), col_("remove_cohort"))

processed_data_df.show()

processed_data_df.coalesce(1).write\
	.mode("overwrite")\
	.option("header", "true")\
	.csv("reports/decay_rate_by_line.cvs")

