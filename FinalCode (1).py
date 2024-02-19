from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import col, sha2, udf, month, lit
from pyspark.sql.types import DecimalType, StringType
import sys
from pyspark.sql.utils import AnalysisException
from datetime import datetime

#delta_jars = "/usr/share/aws/delta/lib/delta-core.jar,/usr/share/aws/delta/lib/delta-storage.jar,/usr/share/aws/delta/lib/delta-storage-s3-dynamodb.jar"

# Configure Spark session with additional JAR files
spark = SparkSession.builder \
    .appName("SparkApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
spark.sparkContext.addPyFile("s3://chinmaya-landingbucket-batch01/config-files/delta-core_2.12-1.2.0.jar")

def read_config_file(app_config_path):
    return spark.read.option("inferSchema", "true").json(app_config_path)

def process_section(config_df, section):
    source_info = config_df.select(section).first()[section].asDict()
    return source_info['source_bucket'], source_info['destination_bucket'], source_info['transformations']

def apply_transformations(df, transformations):
    column_to_join = transformations['to_string']['location']
    column_to_mask = transformations['to_string']['mask']
    column_to_dec_lat = transformations['to_decimal']['latitude']
    column_to_dec_lon = transformations['to_decimal']['longitude']

    join_list_udf = udf(lambda lst: ",".join(lst) if lst else "", StringType())
    masked_df = df.withColumn(column_to_join, join_list_udf(df[column_to_join]))

    if isinstance(column_to_mask, list):
        for i in column_to_mask:
            masked_df = masked_df.withColumn("masked_"+i, sha2(col(i), 256).cast("string"))
    else:
        masked_df = masked_df.withColumn("masked_"+column_to_mask, sha2(col(column_to_mask), 256).cast("string"))
    masked_df = masked_df.withColumn(column_to_dec_lat, col(column_to_dec_lat).cast(DecimalType(10, 7)))
    masked_df = masked_df.withColumn(column_to_dec_lon, col(column_to_dec_lon).cast(DecimalType(10, 7)))

    return masked_df

def write_processed_data(df, destination, partition_columns=None):
    if partition_columns:
        df.write.mode("overwrite").partitionBy(*partition_columns).parquet(destination)  # Unpack partition columns
    else:
        df.write.mode("overwrite").parquet(destination)
    print(f"Writing processed data to '{destination}'...")
    print(df.head())

def check_existing_parquet_files(s3_destination_path):
    try:
        existing_df = spark.read.option("basePath", s3_destination_path).parquet(s3_destination_path)
        return existing_df.count() > 0 
    except AnalysisException as e:
        return False
def update_delta_table(df, deltatablepath):
    print("Updating delta table...")
    try:
        # Read the existing delta table
        from delta.tables import DeltaTable 
        delta_table = DeltaTable.forPath(spark, deltatablepath)
        delta_df = delta_table.toDF()
        print("existing delta table")
        delta_df.show(5)

        joined_df = df.join(delta_df, "advertising_id", "left_outer").select(
            df["*"],
            delta_df.advertising_id.alias("delta_advertising_id"),
            delta_df.user_id.alias("delta_user_id"),
            delta_df.masked_advertising_id.alias("delta_masked_advertising_id"),
            delta_df.masked_user_id.alias("delta_masked_user_id"),
            delta_df.start_date.alias("delta_start_date"),
            delta_df.end_date.alias("delta_end_date")
        )
        print("Joined dataframe:")
        joined_df.show(5)

        # Filter the joined DataFrame to get rows where user_id is different in delta
        filter_df = joined_df.filter((joined_df["delta_advertising_id"].isNull()) |
                                      ((joined_df["user_id"] != joined_df["delta_user_id"]) & (joined_df["delta_end_date"]=="00-00-0000")))

        print("Filter dataframe:")
        filter_df.show()
        
        # Add mergekey column for later use
        merge_df = filter_df.withColumn("mergekey", filter_df["advertising_id"])

        print("Merge dataframe:")
        merge_df.show()
        

        # Create dummy_df for rows where advertising_id matches but user_id is different
        dummy_df = merge_df.filter((merge_df["advertising_id"] == merge_df["delta_advertising_id"]) & (merge_df["user_id"] != merge_df["delta_user_id"])) \
            .withColumn("mergekey", lit(None))

        print("Dummy dataframe:")
        dummy_df.show()

        # Union merge_df and dummy_df to get SCD dataframe
        scd_df = merge_df.union(dummy_df)

        print("SCD dataframe:")
        scd_df.show()
        delta_table.alias("delta").merge(
            source=scd_df.alias("source"),
            condition="delta.advertising_id = source.mergekey"
        ).whenMatchedUpdate(
            set={
                "end_date": str("current_date"),
                "active_status": lit(False)  # Set flag_active to false
            }
        ).whenNotMatchedInsert(
            values={
                "advertising_id": "source.advertising_id",
                "user_id": "source.user_id",
                "masked_advertising_id": "source.masked_advertising_id",
                "masked_user_id": "source.masked_user_id",
                "start_date": "current_date",
                "end_date": lit("00-00-0000"),
                "active_status": lit(True)  # Set flag_active to true for new records
            }
        ).execute()
        # Show the updated Delta table
        print("Updated Delta table:")
        delta_table.toDF().show(10)
        delta_df=delta_table.toDF()
        delta_df.write.format("delta").mode("overwrite").save(deltatablepath)
        print("After saving")


        print("Delta table updated successfully.")
    except Exception as e:
        print(f"Error updating delta table: {str(e)}")

def process_and_write_section(config_df, section):
    source_sou, source_dest, source_tran = process_section(config_df, section)

    if source_sou and source_dest and source_tran:
        # Check if Parquet files already exist in the destination path
        df = spark.read.parquet(source_sou)
        masked_df = apply_transformations(df, source_tran)
        delta_path = "s3://chinmaya-stagingbucket-batch01/delta/"
        if(section=='actives'):
            
            selected_columns_df = masked_df.select("advertising_id", "user_id", "masked_advertising_id", "masked_user_id")
            
            # Adding additional columns
            selected_columns_df = selected_columns_df.withColumn("start_date", lit(datetime.today().strftime('%Y-%m-%d')))  # Adding start_date with today's date
            selected_columns_df = selected_columns_df.withColumn("end_date", lit("00-00-0000")) # Adding end_date with null values
            
            
            if not check_existing_parquet_files(delta_path):
                selected_columns_df = selected_columns_df.withColumn("active_status", lit("true"))  # Adding active_status with default value "true"
                # Write the DataFrame as Parquet to the S3 destination
                selected_columns_df.write.mode("overwrite").format("delta").save(delta_path)
                
                # Confirming the operation
                print(f"Selected columns along with additional columns saved as Parquet in S3 location: {delta_path}")
            
            else:
                # Delta path exists, update the delta table
                update_delta_table(selected_columns_df, delta_path)
                print(f"Delta table updated successfully at: {delta_path}")
        # Specify the partition columns as a list
        partition_columns = ['month', 'date']
        write_processed_data(masked_df, source_dest, partition_columns)
        print("Processing complete.\n")

# Read configuration file
app_config_path = "s3://chinmaya-landingbucket-batch01/config-files/app-config.json"
app_config_df = read_config_file(app_config_path)

s3_key = sys.argv[1]
if 'actives' in s3_key:
    process_and_write_section(app_config_df, 'actives')
elif 'viewership' in s3_key:
    process_and_write_section(app_config_df, 'viewership')
    
spark.stop()