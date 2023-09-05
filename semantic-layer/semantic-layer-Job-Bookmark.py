import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import col, unix_timestamp, from_unixtime
from pyspark.sql.functions import col, date_format

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# 1. Reading data from the sources i.e. S3 bucket
glueContext = GlueContext(sc)
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://sentrics.sample.data/alarms/input/"], 
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)
alarms_df1 = S3bucket_node1.toDF()

#Timestamp Column Transformation and Renaming in PySpark DataFrame
timestamp_columns = ["openedat", "closedat", "createdat", "updatedat"]
transformed_columns = []
for column in alarms_df1.columns:
    if column in timestamp_columns:
        new_column_name = column + "_without_ms"
        transformed_columns.append(
            to_timestamp(substring(col(column), 1, 19), "yyyy-MM-dd HH:mm:ss").alias(new_column_name)
        )
    else:
        transformed_columns.append(col(column))

# Select all original and modified columns
alarms_df = alarms_df1.select(*transformed_columns)

# Dictionary to store column rename mappings
column_mapping = {
      "openedat_without_ms": "openedat",
    "closedat_without_ms": "closedat",
    "createdat_without_ms": "createdat",
    "updatedat_without_ms": "updatedat"
}

# Rename columns using loop
alarms_df_renamed = alarms_df
for old_col, new_col in column_mapping.items():
    alarms_df_renamed = alarms_df_renamed.withColumnRenamed(old_col, new_col)

# 2. Reading data from the sources i.e. S3 bucket
glueContext = GlueContext(sc)
S3bucket_node2 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://sentrics.sample.data/events/input/"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node2",
)
events_df1 = S3bucket_node2.toDF()

#Timestamp Column Transformation and Renaming in PySpark DataFrame
timestamp_columns = ["createdat"]
transformed_columns = []
for column in events_df1.columns:
    if column in timestamp_columns:
        new_column_name = column + "_without_ms"
        transformed_columns.append(
            to_timestamp(substring(col(column), 1, 19), "yyyy-MM-dd HH:mm:ss").alias(new_column_name)
        )
    else:
        transformed_columns.append(col(column))

# Select all original and modified columns
events_df = events_df1.select(*transformed_columns)

# Dictionary to store column rename mappings
column_mapping = {
    "createdat_without_ms": "createdat"
}

# Rename columns using loop
events_df_renamed = events_df
for old_col, new_col in column_mapping.items():
    events_df_renamed = events_df_renamed.withColumnRenamed(old_col, new_col)

# 3. Reading data from the sources i.e. S3 bucket
glueContext = GlueContext(sc)
S3bucket_node3 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://test.processing.for.glue.prod/locations/"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node3",
)
loc_df = S3bucket_node3.toDF()

# 4. Reading data from the sources i.e. S3 bucket
glueContext = GlueContext(sc)
S3bucket_node4 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://test.processing.for.glue.prod/transformed_location_details/"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node4",
)
trans_loc_df1 = S3bucket_node4.toDF()

#Timestamp Column Transformation and Renaming in PySpark DataFrame
timestamp_columns = ["createdat", "updatedat"]
transformed_columns = []
for column in trans_loc_df1.columns:
    if column in timestamp_columns:
        new_column_name = column + "_without_ms"
        transformed_columns.append(
            to_timestamp(substring(col(column), 1, 19), "yyyy-MM-dd HH:mm:ss").alias(new_column_name)
        )
    else:
        transformed_columns.append(col(column))

# Select all original and modified columns
trans_loc_df = trans_loc_df1.select(*transformed_columns)

# Dictionary to store column rename mappings
column_mapping = {
    "createdat_without_ms": "createdat",
    "updatedat_without_ms": "updatedat"
}

# Rename columns using loop
trans_loc_df_renamed = trans_loc_df
for old_col, new_col in column_mapping.items():
    trans_loc_df_renamed = trans_loc_df_renamed.withColumnRenamed(old_col, new_col)

# 5. Reading data from the sources i.e. S3 bucket
glueContext = GlueContext(sc)
S3bucket_node5 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://test.processing.for.glue.prod/sfdc/input/"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node5",
)
sfdc_df = S3bucket_node5.toDF()

# 6. Reading data from the sources i.e. S3 bucket
glueContext = GlueContext(sc)
S3bucket_node6 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://test.processing.for.glue.prod/facilities/"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node6",
)
facilities_df1 = S3bucket_node6.toDF()

#Timestamp Column Transformation and Renaming in PySpark DataFrame
timestamp_columns = ["updatedate","createdat", "updatedat"]
transformed_columns = []
for column in facilities_df1.columns:
    if column in timestamp_columns:
        new_column_name = column + "_without_ms"
        transformed_columns.append(
            to_timestamp(substring(col(column), 1, 19), "yyyy-MM-dd HH:mm:ss").alias(new_column_name)
        )
    else:
        transformed_columns.append(col(column))

# Select all original and modified columns
facilities_df = facilities_df1.select(*transformed_columns)

# Dictionary to store column rename mappings
column_mapping = {
    "updatedate_without_ms": "updatedate",
    "createdat_without_ms": "createdat",
    "updatedat_without_ms": "updatedat"
}

# Rename columns using loop
facilities_df_renamed = facilities_df
for old_col, new_col in column_mapping.items():
    facilities_df_renamed = facilities_df_renamed.withColumnRenamed(old_col, new_col)

# Semantic tables creation
# A. Inner join 'loc_df' with 'trans_loc_df' on 'locationid'/'_ id' to derive 'location' dimension table  
# Perform an inner join
join_condition = loc_df["locationid"] == trans_loc_df_renamed["_id"]
location_df = loc_df.join(trans_loc_df_renamed, join_condition, "inner")
location = location_df.select(["locationkey", "location","facilitykey","locationid","hierarchicalindex","parentkey",loc_df["createdat"],loc_df["updatedat"],"locationType"])

# B. Selecting fields from 'alarms_df' to create resident dimension table.
resident = alarms_df_renamed.select([ alarms_df_renamed["originalsubjectid"].alias("residentid"), alarms_df_renamed["originalsubjectname"].alias("residentname")])
resident = resident.dropDuplicates()

# Fill null values with a placeholder before writing to Parquet
resident = resident.withColumn('t30d_baseline', lit("NaN")).withColumn('night_t30d_baseline', lit("NaN"))

# C. Selecting fields from 'events_df' to create 'events' dimension table. Converting date columns in string to timestamp.
events = events_df_renamed.select([events_df_renamed["id"].alias("eventid"),"alarmid","closing","createdat","facilitykey","closerid","accepterid"])
# Fill null values with a placeholder before writing to Parquet
events = events.withColumn('closedat-quartile', lit("NaN"))

# D. Inner join 'events_df' with 'sfdc_df' on 'facilitykey'/'ensureid' to derive 'community' dimension table  
join_condition_2 = events_df_renamed["facilitykey"] == sfdc_df["ensureid"]
community_df =  events_df_renamed.select(["communityid", events_df_renamed["community"].alias("communityname"), "facilitykey"])\
    .join(sfdc_df.select([sfdc_df["parentaccountid"].alias("corporateid"),sfdc_df["parentaccount"].alias("corporatename"), "ensureid"]),join_condition_2, "inner")
community = community_df.select("communityid", "communityname", "corporateid", "corporatename")
community = community.dropDuplicates()

# # Fill null values with a placeholder before writing to Parquet
community = community.withColumn('KPIType', lit("NaN")).withColumn('peer_25%', lit("NaN")).withColumn('peer_50%', lit("NaN")).withColumn('peer_75%', lit("NaN"))

# E. Selecting fields from 'alarms_df' to create 'alarms' fact table. Converting date columns in string to timestamp.
alarms = alarms_df_renamed.select(["alarmid", alarms_df_renamed["originalsubjectid"].alias("residentid"),"locationkey","dispositiontype","dispositionnote",\
                                   "dispositionauthorkey","openedat","createdat","updatedat","closedat",alarms_df_renamed["status"].alias("alarms_status"),\
                                   alarms_df_renamed["type"].alias("alarms_type"),"topic","domain","class"])

alarms = alarms.join(events_df_renamed.select("alarmid","communityid"), "alarmid")
alarms = alarms.dropDuplicates()

alarms1 = alarms.select("alarmid","communityid","residentid","locationkey","dispositiontype","dispositionnote","dispositionauthorkey","openedat","createdat","updatedat","closedat","alarms_status","alarms_type","topic","domain","class")

# F. Derving calculated fields for the 'alarms' fact table. 
# 1. 'earliest_event_date'
earliest_event_date_df = alarms1.select("alarmid","openedat","closedat").join(events_df_renamed.select("alarmid","event","id","createdat"), "alarmid")

# Calculate the earliest response time for each alarmid
earliest_event_date = earliest_event_date_df.groupBy("alarmid").agg(min("createdat").alias("earliest_event_date"))

# Join the result with the original DataFrame
earliest_event_date_df = earliest_event_date_df.join(earliest_event_date, on="alarmid")

# Calculate the earliest event id for each alarmid
earliest_event_id = earliest_event_date_df.orderBy("createdat").groupBy("earliest_event_date").agg(first("id").alias("earliest_event_id"))

# Join the result with the original DataFrame
earliest_event_id_df = earliest_event_date_df.join(earliest_event_id, on="earliest_event_date")

# 2. 'earliest_event_name'
# Calculate the earliest event name for each alarmid
earliest_event_name = earliest_event_id_df.orderBy("createdat").groupBy("earliest_event_date").agg(first("event").alias("earliest_event_name"))

# Join the result with the original DataFrame
earliest_event_name_df = earliest_event_id_df.join(earliest_event_name, on="earliest_event_date")

# Calculate the acceptance time in minutes and round to 3 decimal places
acceptance_time_df = earliest_event_name_df.withColumn(
    "acceptance_time",
    round((unix_timestamp(col("earliest_event_date")) - unix_timestamp(col("openedat"))) / 60, 3)
)

# 3. 'transit+caregiver_time'
# Calculate the transit+caregiver_time time in minutes and round to 3 decimal places
transit_caregiver_time_df = acceptance_time_df.withColumn(
    "transit+caregiver_time",
    round((unix_timestamp(col("closedat")) - unix_timestamp(col("earliest_event_date"))) / 60, 3)
)

# Calculate the resolution_time time in minutes and round to 3 decimal places
resolution_time_df = transit_caregiver_time_df.withColumn(
    "resolution_time",
    round((unix_timestamp(col("closedat")) - unix_timestamp(col("openedat"))) / 60, 3)
)

# 5. 'alarms_shift'
# Add a "alarms_shift" column based on the "closedat"
alarms_shift_df = resolution_time_df.withColumn(
    "alarms_shift",
    when((hour(col("closedat")) >= 8) & (hour(col("closedat")) < 16), "8am-4pm")
    .when((hour(col("closedat")) >= 16) & (hour(col("closedat")) <= 23), "4pm-12am")
    .otherwise("12am-8am")
)

# Deriving 'is_night' field from 'closedat'
alarms_day_night_df = alarms_shift_df.withColumn(
    "is_night",
    when((hour(col("closedat")) >= 8) & (hour(col("closedat")) <= 22), 0)
    .otherwise(1)
)

# 7. 'acceptance_time_thresholds'
# Add a "acceptance_time_thresholds" column based on the "closedat"
acceptance_time_thresholds_df = alarms_day_night_df.withColumn(
    "acceptance_time_thresholds",
    when(alarms_shift_df["acceptance_time"] < 2, "<2 min")
    .when((alarms_shift_df["acceptance_time"] >= 2) & (alarms_shift_df["acceptance_time"] < 7), "2 - 7 min")
    .when(alarms_shift_df["acceptance_time"] >= 7, "7+ min")
    .otherwise(None)
)

# 8. 'transit+caregiver_time_thresholds'
# Add a "transit+caregiver_time_thresholds" column based on the "closedat"
transit_caregiver_time_thresholds_df = acceptance_time_thresholds_df.withColumn(
    "transit+caregiver_time_thresholds",
    when(acceptance_time_thresholds_df["transit+caregiver_time"] < 3, "<3 min")
    .when(acceptance_time_thresholds_df["transit+caregiver_time"] >= 3, "3+ min")
    .otherwise(None)
)

# 9. 'resolution_time_thresholds'
# Add a "resolution_time_thresholds" column based on the "closedat"
resolution_time_thresholds_df = transit_caregiver_time_thresholds_df.withColumn(
    "resolution_time_thresholds",
    when(transit_caregiver_time_thresholds_df["resolution_time"] < 5, "<5 min")
    .when((transit_caregiver_time_thresholds_df["resolution_time"] >= 5) & (transit_caregiver_time_thresholds_df["resolution_time"] < 15), "5 - 15 min")
    .when(transit_caregiver_time_thresholds_df["resolution_time"] >= 15, "15+ min")
    .otherwise(None)
)

# 10. Deriving date, time, day, month, year, hour, minute, second, week and quarter from 'closedat' as fields
derived_fields_df = resolution_time_thresholds_df.withColumn("closedat_date", date_format(resolution_time_thresholds_df["closedat"], "yyyy-MM-dd"))
derived_fields_df = derived_fields_df.withColumn("closedat_time", date_format(derived_fields_df["closedat"], "HH:mm:ss"))
derived_fields_df = derived_fields_df.withColumn("closedat_day", dayofmonth(derived_fields_df["closedat"]))
derived_fields_df = derived_fields_df.withColumn("closedat_month", month(derived_fields_df["closedat"]))
derived_fields_df = derived_fields_df.withColumn("closedat_year", year(derived_fields_df["closedat"]))
derived_fields_df = derived_fields_df.withColumn("closedat_hour", hour(derived_fields_df["closedat"]))
derived_fields_df = derived_fields_df.withColumn("closedat_minute", minute(derived_fields_df["closedat"]))
derived_fields_df = derived_fields_df.withColumn("closedat_second", second(derived_fields_df["closedat"]))
derived_fields_df = derived_fields_df.withColumn("closedat_week", weekofyear(derived_fields_df["closedat"]))
derived_fields_df = derived_fields_df.withColumn("closedat_quarter", quarter(derived_fields_df["closedat"]))

# G. Selecting and Ordering the fields
alarms2 = derived_fields_df.select(
    "alarmid", "earliest_event_id","earliest_event_date","earliest_event_name", "earliest_event_date","acceptance_time","transit+caregiver_time","resolution_time",\
    "alarms_shift","acceptance_time_thresholds","transit+caregiver_time_thresholds","resolution_time_thresholds","closedat_date","closedat_time","closedat_day",\
    "closedat_month","closedat_year", "closedat_hour","closedat_minute","closedat_second","closedat_week","closedat_quarter","is_night")\
    .join(alarms1, "alarmid")

# Fill null values with a placeholder before writing to Parquet
alarms3 = alarms2.withColumn('response_time', lit("NaN")) \
                .withColumn('transit_time', lit("NaN")) \
                .withColumn('caregiver_time', lit("NaN")) \
                .withColumn('peer_25%_AR', lit("NaN")) \
                .withColumn('peer_50%_AR', lit("NaN")) \
                .withColumn('peer_75%_AR', lit("NaN")) \
                .withColumn('peer_25%_AC', lit("NaN")) \
                .withColumn('peer_50%_AC', lit("NaN")) \
                .withColumn('peer_75%_AC', lit("NaN"))

# ordering the columns
alarms = alarms3.select("alarmid",
                        "communityid",
                        "residentid",
                        "locationkey",
                        "dispositiontype",
                        "dispositionnote",
                        "dispositionauthorkey",
                        "openedat",
                        "createdat",
                        "updatedat",
                        "closedat",
                        "alarms_status",
                        "alarms_type",
                        "topic",
                        "domain",
                        "class",
                         "earliest_event_id",
                         "earliest_event_date",
                        "earliest_event_name",
                        "alarms_shift",
                         "closedat_date",
                         "closedat_time",
                         "closedat_day",
                         "closedat_month",
                         "closedat_year",
                         "closedat_hour",
                         "closedat_minute",
                         "closedat_second",
                         "closedat_week",
                         "closedat_quarter",
                           "is_night",
                         "acceptance_time",
                       "transit_time",
                       "caregiver_time",
                         "transit+caregiver_time",
                            "response_time",
                         "resolution_time",
                         "acceptance_time_thresholds",
                         "transit+caregiver_time_thresholds",
                         "resolution_time_thresholds",
                       "peer_25%_AR",
                       "peer_50%_AR",
                       "peer_75%_AR",
                       "peer_25%_AC",
                       "peer_50%_AC",
                       "peer_75%_AC"
                        )

alarms.dropDuplicates()

# H. Applying filtering rules to alarms table:
# 1.Filter out alarms with acceptance time less than 5 seconds (0.0833 minutes)
# 2.Filter out records that don't have "QA" or "Dev" in the facility and community columns
# 3.Creating a separate table for alarms that haven’t been responded to for 2+ hours.
# Filter out alarms with acceptance time less than 5 seconds (0.0833 minutes)
alarms = alarms.filter(col("acceptance_time") > 0.08333)

# Filtering alarms that have both open and close dates; i.e. exclude 'openedat' and 'closedat' that null values
alarms = alarms.filter( (col("openedat").isNotNull()) | (col("closedat").isNotNull()))

alarms= alarms.join(facilities_df.select("communityid","facility"),"communityid").join(community.select("communityid","communityname"),"communityid")
alarms.dropDuplicates()

# Filter out records that don't have "QA" or "Dev" in the facility and community columns 
alarms = alarms.filter(~(col("facility").like("%QA%") | col("facility").like("%Dev%")) & ~(col("communityname").like("%QA%") | col("communityname").like("%Dev%"))) 

# Creating a separate table for alarms that haven’t been responded to for 2+ hours.
alarms_outliers = alarms.filter(col("acceptance_time") >= 120)
# alarms = alarms.filter(col("acceptance_time") < 120)

alarms = alarms.select("alarmid",
                        "communityid",
                        "residentid",
                        "locationkey",
                        "dispositiontype",
                        "dispositionnote",
                        "dispositionauthorkey",
                        "openedat",
                        "createdat",
                        "updatedat",
                        "closedat",
                        "alarms_status",
                        "alarms_type",
                        "topic",
                        "domain",
                        "class",
                         "earliest_event_id",
                         "earliest_event_date",
                        "earliest_event_name",
                        "alarms_shift",
                         "closedat_date",
                         "closedat_time",
                         "closedat_day",
                         "closedat_month",
                         "closedat_year",
                        "closedat_hour",
                        "closedat_minute",
                        "closedat_second",
                        "closedat_week",
                        "closedat_quarter",
                        "is_night",
                        "acceptance_time",
                        "transit_time",
                        "caregiver_time",
                        "transit+caregiver_time",
                        "response_time",
                        "resolution_time",
                        "acceptance_time_thresholds",
                        "transit+caregiver_time_thresholds",
                        "resolution_time_thresholds",
                       "peer_25%_AR",
                       "peer_50%_AR",
                       "peer_75%_AR",
                       "peer_25%_AC",
                       "peer_50%_AC",
                       "peer_75%_AC"
                        )

alarms_outliers = alarms_outliers.select("alarmid",
                        "communityid",
                        "residentid",
                        "locationkey",
                        "dispositiontype",
                        "dispositionnote",
                        "dispositionauthorkey",
                        "openedat",
                        "createdat",
                        "updatedat",
                        "closedat",
                        "alarms_status",
                        "alarms_type",
                        "topic",
                        "domain",
                        "class",
                         "earliest_event_id",
                         "earliest_event_date",
                        "earliest_event_name",
                        "alarms_shift",
                         "closedat_date",
                         "closedat_time",
                         "closedat_day",
                         "closedat_month",
                         "closedat_year",
                         "closedat_hour",
                         "closedat_minute",
                         "closedat_second",
                         "closedat_week",
                         "closedat_quarter",
                           "is_night",
                         "acceptance_time",
                       "transit_time",
                       "caregiver_time",
                         "transit+caregiver_time",
                            "response_time",
                         "resolution_time",
                         "acceptance_time_thresholds",
                         "transit+caregiver_time_thresholds",
                         "resolution_time_thresholds",
                       "peer_25%_AR",
                       "peer_50%_AR",
                       "peer_75%_AR",
                       "peer_25%_AC",
                       "peer_50%_AC",
                       "peer_75%_AC"
                        )
# alarms_outliers.printSchema()

# Converting the pyspark_df's to glue DynamicFrame 
resident_dynamic_frame = DynamicFrame.fromDF(resident, glueContext, "resident_dynamic_frame")
events_dynamic_frame = DynamicFrame.fromDF(events, glueContext, "events_dynamic_frame")
community_dynamic_frame = DynamicFrame.fromDF(community, glueContext, "community_dynamic_frame")
location_dynamic_frame = DynamicFrame.fromDF(location, glueContext, "location_dynamic_frame")
alarms_1_dynamic_frame = DynamicFrame.fromDF(alarms, glueContext, "alarms_1_dynamic_frame")
alarms_2_dynamic_frame = DynamicFrame.fromDF(alarms_outliers, glueContext, "alarms_2_dynamic_frame")

### III. Writing the the dimension and fact tables to S3 buckets as parquet.
resident_dynamic_frame1 = glueContext.write_dynamic_frame.from_options(
    frame=resident_dynamic_frame,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://sentrics.semantic.layer.output/resident_dim/",
    },
    format_options={"compression": "uncompressed"},
    transformation_ctx="resident_dynamic_frame1",
)
events_dynamic_frame1 = glueContext.write_dynamic_frame.from_options(
    frame=events_dynamic_frame,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://sentrics.semantic.layer.output/events_dim/",
    },
    format_options={"compression": "uncompressed"},
    transformation_ctx="events_dynamic_frame1",
)
community_dynamic_frame1 = glueContext.write_dynamic_frame.from_options(
    frame=community_dynamic_frame,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://sentrics.semantic.layer.output/community_dim/",
    },
    format_options={"compression": "uncompressed"},
    transformation_ctx="community_dynamic_frame",
)
location_dynamic_frame1 = glueContext.write_dynamic_frame.from_options(
    frame=location_dynamic_frame,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://sentrics.semantic.layer.output/location_dim/",
    },
    format_options={"compression": "uncompressed"},
    transformation_ctx="location_dynamic_frame1",
)
alarms_1_dynamic_frame1 = glueContext.write_dynamic_frame.from_options(
    frame=alarms_1_dynamic_frame,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://sentrics.semantic.layer.output/alarms_fact/",
    },
    format_options={"compression": "uncompressed"},
    transformation_ctx="alarms_1_dynamic_frame1",
)

alarms_2_dynamic_frame1 = glueContext.write_dynamic_frame.from_options(
    frame=alarms_2_dynamic_frame,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://sentrics.semantic.layer.output/alarms_outliers/",
    },
    format_options={"compression": "uncompressed"},
    transformation_ctx="alarms_2_dynamic_frame2",
)
job.commit()