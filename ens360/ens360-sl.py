# Semantic Layer Creation
## This Python script generates dimension and fact tables from various data sources.

### Importing necessary AWS Glue modules and libraries for configuring Apache Spark environment within AWS Glue.
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

### Importing essential Spark modules and libraries for data processing.
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import col, unix_timestamp, from_unixtime,date_format
from pyspark.sql.types import StructType, StructField, StringType

### Initializing Spark and Glue contexts, creating a Spark session, and defining an AWS Glue job.

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME','alarms.source','events.source','locations.source','transformed.location.source','sdfc.source','facilities.source','resident.dim.target','events.dim.target','community.dim.target','location.dim.target','alarms.fact.target','alarms.greater.2hrs.dim.target','c1.alarm.source','c1.events.source','market.alarm.target'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

### I. Defining functional blocks of code for reusage
def calculate_time_difference(df, start_col, end_col, new_col):
    """
    Description:
        The function calculates and derives 'acceptance_time', 'transit+caregiver_time', and 'resolution_time'
        fields in minutes based on the provided start and end columns. It uses the PySpark DataFrame 'df'
        as input and adds new columns for each time calculation.

    Parameter:
        df: PySpark DataFrame
            The input DataFrame on which the time calculations will be performed.
        start_col: string
            The name of the column representing the start timestamp.
        end_col: string
            The name of the column representing the end timestamp.
        new_col: string
            The name of the new column that will hold the calculated time difference in minutes.

    Return:
        df: PySpark DataFrame
            The modified DataFrame with the newly derived time columns.
    """

    return df.withColumn(
        new_col,round((unix_timestamp(col(end_col)) - unix_timestamp(col(start_col))) / 60, 3))


def calculate_alarms_shift(closedat):
    """
    Description:
        The function calculates and derives alarm shift timings based on the provided 'closedat' timestamp column.
        It categorizes each timestamp into one of three shifts: "8am-4pm", "4pm-12am", or "12am-8am".
        
    Parameter:
        closedat: PySpark DataFrame Column
            The timestamp column representing the time at which alarms occurred.

    Return:
        PySpark DataFrame Column
            A new column containing the derived alarm shift timings.
    """
    return when((hour(closedat) >= 8) & (hour(closedat) < 16), "8am-4pm") \
        .when((hour(closedat) >= 16) & (hour(closedat) <= 23), "4pm-12am") \
        .otherwise("12am-8am")


def calculate_is_night(closedat):
    """
    Description:
        The function calculates whether a given timestamp in the 'closedat' column falls within the night period.
        It assigns a value of 0 if the timestamp falls between 6 AM and 10 PM, indicating daytime, and 1 otherwise,
        indicating nighttime.

    Parameter:
        closedat: PySpark DataFrame Column
            The timestamp column representing the time at which alarms occurred.

    Return:
        PySpark DataFrame Column
            A new column containing binary values (0 or 1) indicating whether the timestamp represents night or day.
    """
    return when((hour(closedat) >= 6) & (hour(closedat) <= 22), 0) \
        .otherwise(1)


def calculate_acceptance_time_thresholds(acceptance_time):
    """
    Description:
        The function calculates threshold categories for 'acceptance_time' based on the provided time in minutes.
        It categorizes the time into "<2 min" if less than 2 minutes, "2 - 7 min" if between 2 and 7 minutes, 
        and "7+ min" if 7 minutes or more.

    Parameter:
        acceptance_time: PySpark DataFrame Column
            The column containing 'acceptance_time' values in minutes.

    Return:
        PySpark DataFrame Column
            A new column containing the derived threshold categories for 'acceptance_time'.
    """

    return when(acceptance_time < 2, "<2 min") \
        .when((acceptance_time >= 2) & (acceptance_time < 7), "2 - 7 min") \
        .when(acceptance_time >= 7, "7+ min")


def calculate_transit_caregiver_time_thresholds(transit_caregiver_time):
    """
    Description:
        The function calculates threshold categories for 'transit+caregiver_time' based on the provided time in minutes.
        It categorizes the time into "<3 min" if less than 3 minutes and "3+ min" if 3 minutes or more.

    Parameter:
        transit_caregiver_time: PySpark DataFrame Column
            The column containing 'transit+caregiver_time' values in minutes.

    Return:
        PySpark DataFrame Column
            A new column containing the derived threshold categories for 'transit+caregiver_time'.
    """

    return when(transit_caregiver_time < 3, "<3 min") \
        .when(transit_caregiver_time >= 3, "3+ min")


def calculate_resolution_time_thresholds(resolution_time):
    """
    Description:
        The function calculates threshold categories for 'resolution_time' based on the provided time in minutes.
        It categorizes the time into "<5 min" if less than 5 minutes, "5 - 15 min" if between 5 and 15 minutes, 
        and "15+ min" if 15 minutes or more.

    Parameter:
        resolution_time: PySpark DataFrame Column
            The column containing 'resolution_time' values in minutes.

    Return:
        PySpark DataFrame Column
            A new column containing the derived threshold categories for 'resolution_time'.
    """
    return when(resolution_time < 5, "<5 min") \
        .when((resolution_time >= 5) & (resolution_time < 15), "5 - 15 min") \
        .when(resolution_time >= 15, "15+ min")


def add_time_columns(df, timestamp_col):
    """
    Description:
        The function derives date, time, day, month, year, hour, minute, second, week, and quarter fields
        from the provided timestamp column.

    Parameter:
        df: PySpark DataFrame
            The input DataFrame containing the timestamp column.
        timestamp_col: string
            The name of the timestamp column from which date and time-related fields will be derived.

    Return:
        PySpark DataFrame
            A new DataFrame with additional columns for date, time, day, month, year, hour, minute, second, week,
            and quarter derived from the provided timestamp column.
    """

    derived_fields_df = df.withColumn(f"{timestamp_col}_date", F.date_format(df[timestamp_col], "yyyy-MM-dd")) \
                        .withColumn(f"{timestamp_col}_time", F.date_format(df[timestamp_col], "HH:mm:ss")) \
                        .withColumn(f"{timestamp_col}_day", F.dayofmonth(df[timestamp_col])) \
                        .withColumn(f"{timestamp_col}_month", F.month(df[timestamp_col])) \
                        .withColumn(f"{timestamp_col}_year", F.year(df[timestamp_col])) \
                        .withColumn(f"{timestamp_col}_hour", F.hour(df[timestamp_col])) \
                        .withColumn(f"{timestamp_col}_minute", F.minute(df[timestamp_col])) \
                        .withColumn(f"{timestamp_col}_second", F.second(df[timestamp_col])) \
                        .withColumn(f"{timestamp_col}_week", F.weekofyear(df[timestamp_col])) \
                        .withColumn(f"{timestamp_col}_quarter", F.quarter(df[timestamp_col]))
    return derived_fields_df


def initialize_columns_with_nan(df, columns):
    """
    Description:
        The function initializes specified columns in the provided DataFrame with "NaN" placeholder values.

    Parameter:
        df: PySpark DataFrame
            The input DataFrame in which specified columns will be initialized with "NaN" values.
        columns: list
            A list of column names that need to be initialized with "NaN" values.

    Return:
        PySpark DataFrame
            A new DataFrame with the specified columns initialized to "NaN" values.
    """
    for column in columns:
        df = df.withColumn(column, F.lit("NaN"))
    return df

def preprocess_timestamp_columns(input_df, timestamp_columns):
    """
    Description:
        The function preprocesses timestamp columns in the input DataFrame by excluding milliseconds,
        and it also renames the processed columns. It is designed to improve the readability and consistency
        of timestamp data.

    Parameter:
        input_df: PySpark DataFrame
            The input DataFrame containing various columns, including timestamp columns.
        timestamp_columns: list
            A list of column names representing timestamp columns that need to be preprocessed.

    Return:
        output_df: PySpark DataFrame
            A new DataFrame with the specified timestamp columns processed to exclude milliseconds
            and consistently renamed.
    """

    transformed_columns = []
    
    for column in input_df.columns:
        if column in timestamp_columns:
            new_column_name = column + "_without_ms"
            transformed_columns.append(
                to_timestamp(substring(col(column), 1, 19), "yyyy-MM-dd HH:mm:ss").alias(new_column_name)
            )
        else:
            transformed_columns.append(col(column))
    
    output_df = input_df.select(*transformed_columns)
    
    column_mapping = {col + "_without_ms": col for col in timestamp_columns}
    
    for old_col, new_col in column_mapping.items():
        output_df = output_df.withColumnRenamed(old_col, new_col)
    
    return output_df


def calculate_total_alarms_combined(resident):
    """
    Description:
        The function calculates the count of alarms for residents in the latest month and in a trailing 3-month period (T3M).
        It performs aggregations based on the specified criteria and joins the results to create the resident dimension table.
    
    Parameter:
        resident: PySpark DataFrame
            The input DataFrame contains alarm data with relevant fields selected for resident dimension table derivation.
    
    Return:
        resident_df: PySpark DataFrame
            A new DataFrame representing the resident dimension table with columns for resident ID, resident name,
            total alarms in the T3M period, total alarms in the latest month, total night alarms in the T3M period,
            and total night alarms in the latest month.
    """
    df_total = resident \
        .filter(F.datediff(F.current_date(), "openedat") <= 90) \
        .groupBy(resident.residentname.alias("T3M_residentname"),resident.residentid.alias("T3M_residentid")) \
        .agg(F.count("alarmid").alias("T3M"))

    most_recent_month_total = resident \
        .filter((F.year("openedat") == F.year(F.current_date())) & (F.month("openedat") == F.month(F.current_date()) - 1)) \
        .groupBy(resident.residentname.alias("Latest_Month_residentname"),resident.residentid.alias("Latest_Month_residentid")) \
        .agg(F.count("alarmid").alias("Latest_Month"))

    # Calculate alarms for night
    df_night = resident \
        .filter((resident["is_night"] == 1) & (F.datediff(F.current_date(), "openedat") <= 90)) \
        .groupBy(resident.residentname.alias("T3M_night_residentname"),resident.residentid.alias("T3M_night_residentid")) \
        .agg(F.count("alarmid").alias("T3M_night"))

    most_recent_month_night = resident \
        .filter((resident["is_night"] == 1) & ((F.year("openedat") == F.year(F.current_date())) & (F.month("openedat") == F.month(F.current_date()) - 1))) \
        .groupBy(resident.residentname.alias("Latest_Month_night_residentname"),resident.residentid.alias("Latest_Month_night_residentid")) \
        .agg(F.count("alarmid").alias("Latest_Month_night"))

    # Join and select columns for total
    df_total_joined = df_total.join(most_recent_month_total, F.col("T3M_residentid") == F.col("Latest_Month_residentid"), "inner") \
        .select("T3M_residentid","T3M_residentname", "T3M", "Latest_Month")
    df_total_joined = df_total_joined.dropDuplicates()
    
    # Join and select columns for night
    df_night_joined = df_night.join(most_recent_month_night, F.col("T3M_night_residentid") == F.col("Latest_Month_night_residentid"), "inner") \
        .select("T3M_night_residentid","T3M_night_residentname", "T3M_night", "Latest_Month_night")
    df_night_joined = df_night_joined.dropDuplicates()
        
    # Merge the DataFrames
    resident_df = df_total_joined.join(df_night_joined, F.col("T3M_residentid") == F.col("T3M_night_residentid"), "inner") \
        .select(df_total_joined["T3M_residentid"].alias('residentid'),df_total_joined["T3M_residentname"].alias('residentname'), "T3M", "Latest_Month", "T3M_night", "Latest_Month_night")
    resident_df = resident_df.dropDuplicates()
    
    return resident_df

### II. Reading data from the sources i.e. S3 bucket
#### A.1. Reading 'alarm' data as pyspark df 'alarms_df1'
glueContext = GlueContext(sc)
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://"+ args['alarms.source'] +"/"], 
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)
alarms_df1 = S3bucket_node1.toDF() 

#### A.2. Preprocessing 'alarms_df1' to exclude milliseconds from timestamp column
timestamp_columns_alarms = ["openedat", "closedat", "createdat", "updatedat"]
alarms_df_renamed = preprocess_timestamp_columns(alarms_df1, timestamp_columns_alarms)


#### B.1. Reading 'events' data as pyspark df 'events_df1'
glueContext = GlueContext(sc)

S3bucket_node2 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://"+ args['events.source'] +"/"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node2",
)
events_df1 = S3bucket_node2.toDF()

#### B.2. Preprocessing 'events_df1' to exclude milliseconds from timestamp column
timestamp_columns_events = ["createdat"]
events_df_renamed = preprocess_timestamp_columns(events_df1, timestamp_columns_events)

#### C. Reading 'locations' data as pyspark df 'loc_df'
glueContext = GlueContext(sc)

S3bucket_node3 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://"+ args['locations.source'] +"/"], 
        "recurse": True,
    },
    transformation_ctx="S3bucket_node3",
)
loc_df = S3bucket_node3.toDF()

#### D.1. Reading 'transformed_location_details' data as pyspark df 'trans_loc_df1'
glueContext = GlueContext(sc)

S3bucket_node4 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://"+ args['transformed.location.source'] +"/"], 
        "recurse": True,
    },
    transformation_ctx="S3bucket_node4",
)
trans_loc_df1 = S3bucket_node4.toDF()

#### D.2. Preprocessing 'trans_loc_df1' to exclude milliseconds from timestamp column
timestamp_columns_loc = ["createdat", "updatedat"]
trans_loc_df_renamed = preprocess_timestamp_columns(trans_loc_df1, timestamp_columns_loc)

#### E. Reading 'sfdc' data as pyspark df 'sfdc_df'
glueContext = GlueContext(sc)

S3bucket_node5 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://"+ args['sdfc.source'] +"/"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node5",
)
sfdc_df = S3bucket_node5.toDF()

#### F.1. Reading 'facilities' data as pyspark df 'facilities_df'
glueContext = GlueContext(sc)

S3bucket_node6 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://"+ args['facilities.source'] +"/"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node6",
)
facilities_df1 = S3bucket_node6.toDF()

#### F.2. Preprocessing 'facilities_df1' to exclude milliseconds from timestamp column
timestamp_columns_facility = ["updatedate", "createdat", "updatedat"]
facilities_df_renamed = preprocess_timestamp_columns(facilities_df1, timestamp_columns_facility)

### III. Semantic tables creation
#### A.1. Inner join 'loc_df' with 'trans_loc_df' on 'locationid'/'_ id' to derive 'location' dimension table  
join_condition = loc_df["locationid"] == trans_loc_df_renamed["_id"]
location_df = loc_df.join(trans_loc_df_renamed, join_condition, "inner")
location_df = location_df.dropDuplicates()
location = location_df.select(["locationkey", "location","facilitykey","locationid","hierarchicalindex","parentkey",loc_df["createdat"],loc_df["updatedat"],"locationType"])

#### A.2. Reading 'loc_df' and 'trans_loc_df_renamed' as temporay views  
loc_df.createOrReplaceTempView("locations")
trans_loc_df_renamed.createOrReplaceTempView("transformed_location_details")

#### A.3. Executing SQL query to derive location hierarchy
result = spark.sql("""
    SELECT 
        p.locationkey,
        p.location, 
        plt.locationtype,
        c1.location AS child_level1, 
        plc1.locationtype AS child_level1_type,
        c2.location AS child_level2, 
        plc2.locationtype AS child_level2_type,
        c3.location AS child_level3, 
        plc3.locationtype AS child_level3_type,
        c4.location AS child_level4, 
        plc4.locationtype AS child_level4_type,
        c5.location AS child_level5, 
        plc5.locationtype AS child_level5_type,
        c6.location AS child_level6, 
        plc6.locationtype AS child_level6_type
    FROM locations p
    LEFT JOIN transformed_location_details plt 
        ON p.locationid = plt._id
    LEFT JOIN locations c1 
        ON p.locationkey = c1.parentkey 
    LEFT JOIN transformed_location_details plc1 
        ON c1.locationid = plc1._id
    LEFT JOIN locations c2 
        ON c1.locationkey = c2.parentkey 
    LEFT JOIN transformed_location_details plc2 
        ON c2.locationid = plc2._id
    LEFT JOIN locations c3 
        ON c2.locationkey = c3.parentkey 
    LEFT JOIN transformed_location_details plc3 
        ON c3.locationid = plc3._id
    LEFT JOIN locations c4 
        ON c3.locationkey = c4.parentkey 
    LEFT JOIN transformed_location_details plc4 
        ON c4.locationid = plc4._id
    LEFT JOIN locations c5 
        ON c4.locationkey = c5.parentkey 
    LEFT JOIN transformed_location_details plc5 
        ON c5.locationid = plc5._id
    LEFT JOIN locations c6 
        ON c5.locationkey = c6.parentkey
    LEFT JOIN transformed_location_details plc6 
        ON c6.locationid = plc6._id
    WHERE p.parentkey IS NULL 
        AND p.hierarchicalindex = 0
""")
loc_hierarchy_df = result.dropDuplicates()
##### Dropping "locationtype" from 'loc_hierarchy_df' to avoid duplicate fields in join operation
loc_hierarchy_df = loc_hierarchy_df.drop("locationtype") 
#### A.4. Including all fields from 'loc_hierarchy_df' through inner join 'location' with 'loc_hierarchy_df' on 'locationkey'
location = location.join(loc_hierarchy_df, location.locationkey == loc_hierarchy_df.locationkey, 'inner')
location = location.dropDuplicates()

#### B. Selecting fields from 'events_df_renamed' to create 'events' dimension table. Converting date columns in string to timestamp.
events = events_df_renamed.select([events_df_renamed["id"].alias("eventid"),"alarmid","event","closing","createdat","facilitykey","closerid","accepterid"])
##### Fill null values with "NaN" placeholder before writing to Parquet
events = events.withColumn('closedat-quartile', lit("NaN"))

#### C. Inner join 'events_df_renamed' with 'sfdc_df' on 'facilitykey'/'ensureid' to derive 'community' dimension table  
join_condition_2 = events_df_renamed["facilitykey"] == sfdc_df["ensureid"]
community_df =  events_df_renamed.select(["communityid", events_df_renamed["community"].alias("communityname"), "facilitykey"]).join(sfdc_df.select([sfdc_df["parentaccountid"].alias("corporateid"),sfdc_df["parentaccount"].alias("corporatename"),"stratosidnumber", "ensureid"]),join_condition_2, "inner")
community = community_df.select("communityid","stratosidnumber", "communityname", "corporateid", "corporatename")
community = community.dropDuplicates()

##### Fill null values with "NaN" placeholder before writing to Parquet
community = community.withColumn('KPIType', lit("NaN")).withColumn('peer_25%', lit("NaN")).withColumn('peer_50%', lit("NaN")).withColumn('peer_75%', lit("NaN"))

#### D. Selecting fields from 'alarms_df_renamed' to create 'alarms' fact table. Converting date columns in string to timestamp.
alarms = alarms_df_renamed.select(["alarmid", alarms_df_renamed["originalsubjectid"].alias("residentid"), alarms_df_renamed["originalsubjectname"].alias("residentname"),"locationkey","dispositiontype","dispositionnote","dispositionauthorkey","openedat","createdat","updatedat","closedat",alarms_df_renamed["status"].alias("alarms_status"), alarms_df_renamed["type"].alias("alarms_type"),"topic","domain","class"])

alarms = alarms.join(events_df_renamed.select("alarmid","communityid"), "alarmid")
alarms = alarms.dropDuplicates()

alarms = alarms.select("alarmid","communityid","residentid","residentname","locationkey","dispositiontype","dispositionnote","dispositionauthorkey","openedat","createdat","updatedat","closedat","alarms_status","alarms_type","topic","domain","class")

#### E. Derving calculated fields for the 'alarms' fact table. 
##### 1. 'earliest_event_date'
earliest_event_date_df = alarms.select("alarmid","openedat","closedat").join(events_df_renamed.select("alarmid","event","id","createdat"), "alarmid")
earliest_event_date_df = earliest_event_date_df.dropDuplicates()

###### Calculate the earliest response time for each alarmid
earliest_event_date = earliest_event_date_df.groupBy("alarmid").agg(min("createdat").alias("earliest_event_date"))

###### Join the result with the original DataFrame
earliest_event_date_df = earliest_event_date_df.join(earliest_event_date, on="alarmid")
earliest_event_date_df = earliest_event_date_df.dropDuplicates()

##### 2. 'earliest_event_id'
###### Calculate the earliest event id for each alarmid
earliest_event_id = earliest_event_date_df.orderBy("createdat").groupBy("earliest_event_date").agg(first("id").alias("earliest_event_id"))

###### Join the result with the original DataFrame
earliest_event_id_df = earliest_event_date_df.join(earliest_event_id, on="earliest_event_date")
earliest_event_id_df = earliest_event_id_df.dropDuplicates()

##### 3. 'earliest_event_name'
###### Calculate the earliest event name for each alarmid
earliest_event_name = earliest_event_id_df.orderBy("createdat").groupBy("earliest_event_date").agg(first("event").alias("earliest_event_name"))

###### Join the result with the original DataFrame
earliest_event_name_df = earliest_event_id_df.join(earliest_event_name, on="earliest_event_date")
earliest_event_name_df = earliest_event_name_df.dropDuplicates()

##### 4. Deriving 'acceptance_time','transit+caregiver_time' and 'resolution_time' using calculate_time_difference()
###### The time_calculations list consists of tuples, with each tuple containing the name of the duration column and 
###### the necessary fields for its derivation.
time_calculations = [
    ("acceptance_time", "openedat", "earliest_event_date"),
    ("transit+caregiver_time", "earliest_event_date", "closedat"),
    ("resolution_time", "openedat", "closedat")]

####### Applying calculate_time_difference() in a loop
resolution_time_df = earliest_event_name_df  
for new_col, start_col, end_col in time_calculations:
    resolution_time_df = calculate_time_difference(resolution_time_df, start_col, end_col, new_col)

##### 5. 'alarms_shift'

###### Apply the alarms shift calculations using withColumn
alarms_shift_df = resolution_time_df.withColumn(
    "alarms_shift",
    calculate_alarms_shift(col("closedat"))
)

##### 6. 'resolution_time'

###### Apply the night calculations using withColumn
alarms_day_night_df = alarms_shift_df.withColumn(
    "is_night",
    calculate_is_night(col("closedat"))
)


###### Apply the threshold calculations using withColumn
acceptance_time_thresholds_df = alarms_day_night_df.withColumn(
    "acceptance_time_thresholds",
    calculate_acceptance_time_thresholds(col("acceptance_time"))
)

transit_caregiver_time_thresholds_df = acceptance_time_thresholds_df.withColumn(
    "transit+caregiver_time_thresholds",
    calculate_transit_caregiver_time_thresholds(col("transit+caregiver_time"))
)

resolution_time_thresholds_df = transit_caregiver_time_thresholds_df.withColumn(
    "resolution_time_thresholds",
    calculate_resolution_time_thresholds(col("resolution_time"))
)

##### 8.  Deriving date, time, day, month, year, hour, minute, second, week and quarter from 'closedat' as fields

derived_fields_df = add_time_columns(resolution_time_thresholds_df,"closedat")

#### F. Selecting relevant fields prior to joining 'derived_fields_df' to 'alarms' on 'alarmid'
field_list = ["alarmid", "earliest_event_id", "earliest_event_date", "earliest_event_name", "acceptance_time", "transit+caregiver_time",
             "resolution_time", "alarms_shift", "acceptance_time_thresholds", "transit+caregiver_time_thresholds", "resolution_time_thresholds", "closedat_date",
             "closedat_time", "closedat_day", "closedat_month", "closedat_year", "closedat_hour", "closedat_minute", "closedat_second", "closedat_week", 
             "closedat_quarter", "is_night"]
alarms = derived_fields_df.select(field_list).join(alarms, "alarmid")
alarms = alarms.dropDuplicates()

##### Including fields and filling them with "NaN" placeholder before writing to Parquet
nan_fields = ['response_time','transit_time','caregiver_time','peer_25%_AR','peer_50%_AR', 'peer_75%_AR','peer_25%_AC','peer_50%_AC', 'peer_75%_AC']

alarms = initialize_columns_with_nan(alarms,nan_fields)

#### G.Selecting fields from 'alarms' to create resident dimension table.
resident = alarms.select("residentid","residentname","alarmid","is_night","openedat","topic")

##### Fill null values with a placeholder before writing to Parquet
resident = resident.withColumn('t30d_baseline', lit("NaN")).withColumn('night_t30d_baseline', lit("NaN"))
##### Deriving fields to calculate alarm counts for Trailing 3 months and latest month
resident_df = calculate_total_alarms_combined(resident)

#### H. Applying filtering rules to alarms table:
##### 1.Filter out alarms with acceptance time less than 5 seconds (0.0833 minutes) but not the System-generated alarms
alarms = alarms.filter( (col("acceptance_time") > 0.08333) | (col("domain").like("%System%")))

##### 2.Filter out 'alarms' df where 'openedat' and 'closedat' is null
# alarms = alarms.filter( (col("openedat").isNotNull()) | (col("closedat").isNotNull()))
alarms = alarms.na.drop(subset=["openedat"])
alarms = alarms.na.drop(subset=["closedat"])

##### 3.Filter out 'alarms' df that don't have "QA" or "Dev" in the facility and community columns
alarms= alarms.join(facilities_df_renamed.select("communityid","facility"),"communityid").join(community.select("communityid","communityname"),"communityid")
alarms = alarms.dropDuplicates()
alarms = alarms.filter(~(col("facility").like("%QA%") | col("facility").like("%Dev%")) & ~(col("communityname").like("%QA%") | col("communityname").like("%Dev%"))) 

##### 4.Creating a separate table for alarms that have acceptance_time 2+ hours.
alarms_greater_than_2hrs = alarms.filter(col("acceptance_time") >= 120)
alarms = alarms.filter(col("acceptance_time") < 120)

alarms = alarms.dropDuplicates()
alarms_greater_than_2hrs = alarms_greater_than_2hrs.dropDuplicates()

#### I. Inserting 'total' value under 'alarms_shift' column
alarms_with_total = alarms.withColumn("alarms_shift", lit('total')).withColumn("alarmid", concat(col("alarmid"), lit("_"), col("alarms_shift")))
alarm_combined_df = alarms.union(alarms_with_total)
alarms_greater_than_2hrs_with_total = alarms_greater_than_2hrs.withColumn("alarms_shift", lit('total')).withColumn("alarmid", concat(col("alarmid"), lit("_"), col("alarms_shift")))
alarms_greater_than_2hrs_with_total_combined_df = alarms_greater_than_2hrs.union(alarms_greater_than_2hrs_with_total)


### IV. Writing the the dimension and fact tables to S3 buckets as parquet.
#### A. Converting pyspark df into Dynamic Frame
resident_dynamic_frame = DynamicFrame.fromDF(resident_df, glueContext, "resident_dynamic_frame")
events_dynamic_frame = DynamicFrame.fromDF(events, glueContext, "events_dynamic_frame")
community_dynamic_frame = DynamicFrame.fromDF(community, glueContext, "community_dynamic_frame")
location_dynamic_frame = DynamicFrame.fromDF(location, glueContext, "location_dynamic_frame")
alarm_combined_dynamic_frame = DynamicFrame.fromDF(alarm_combined_df, glueContext, "alarm_combined_dynamic_frame")
alarms_greater_than_2hrs_with_total_combined_dynamic_frame = DynamicFrame.fromDF(alarms_greater_than_2hrs_with_total_combined_df, glueContext, "alarms_greater_than_2hrs_with_total_combined_dynamic_frame")

#### B. Writing Dynamic Frame to S3 buckets
resident_dynamic_frame1 = glueContext.write_dynamic_frame.from_options(
    frame=resident_dynamic_frame,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://"+ args['resident.dim.target'] +"/",
    },
    format_options={"compression": "uncompressed"},
    transformation_ctx="resident_dynamic_frame1",
)

events_dynamic_frame1 = glueContext.write_dynamic_frame.from_options(
    frame=events_dynamic_frame,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://"+ args['events.dim.target'] +"/",  
    },
    format_options={"compression": "uncompressed"},
    transformation_ctx="events_dynamic_frame1",
)

community_dynamic_frame1 = glueContext.write_dynamic_frame.from_options(
    frame=community_dynamic_frame,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://"+ args['community.dim.target'] +"/", 
    },
    format_options={"compression": "uncompressed"},
    transformation_ctx="community_dynamic_frame",
)

location_dynamic_frame1 = glueContext.write_dynamic_frame.from_options(
    frame=location_dynamic_frame,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://"+ args['location.dim.target'] +"/", 
    },
    format_options={"compression": "uncompressed"},
    transformation_ctx="location_dynamic_frame1",
)

alarm_combined_dynamic_frame1 = glueContext.write_dynamic_frame.from_options(
    frame=alarm_combined_dynamic_frame,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://"+ args['alarms.fact.target'] +"/",
    },
    format_options={"compression": "uncompressed"},
    transformation_ctx="alarm_combined_dynamic_frame2",
)

alarms_greater_than_2hrs_with_total_combined_dynamic_frame1 = glueContext.write_dynamic_frame.from_options(
    frame=alarms_greater_than_2hrs_with_total_combined_dynamic_frame,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://"+ args['alarms.greater.2hrs.dim.target'] +"/",
    },
    format_options={"compression": "uncompressed"},
    transformation_ctx="alarms_greater_than_2hrs_with_total_combined_dynamic_frame2",
)

job.commit()