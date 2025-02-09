from pyspark.sql.types import StructType, StructField, StringType, TimestampType, FloatType, ArrayType, IntegerType

catalog = "tabular"
schema = "dataexpert"

table_schema = StructType([
    StructField("TableName", StringType(), True),
    StructField("TableType", StringType(), True),
    StructField("SizeInGB", FloatType(), True),
    StructField("Location", StringType(), True),
    StructField("CreatedAt", TimestampType(), True), 
    StructField("LastModified", TimestampType(), True), 
    StructField("PartitionColumns", ArrayType(StringType()), True), 
    StructField("ClusteringColumns", ArrayType(StringType()), True),
    StructField("NumFiles", IntegerType(), True),
])

# Get the list of tables
tables = (
    spark.sql(f"SHOW TABLES IN {catalog}.{schema}")
    .select("tableName")
    .where("isTemporary == false")
)

# Collect table names
table_names = [row["tableName"] for row in tables.collect()]

table_sizes = []

# Loop through each table and fetch details
for table in table_names:
    table_path = f"{catalog}.{schema}.{table}"
    try:
        detail = spark.sql(f"DESCRIBE DETAIL {table_path}")
        
        # Collect the details
        row = detail.collect()[0]  # Get the first row of the result
        size_in_bytes = row["sizeInBytes"]
        location = row["location"]
        created_at = row["createdAt"]
        last_modified = row["lastModified"]
        partition_columns = row["partitionColumns"]
        clustering_columns = row["clusteringColumns"]
        num_files = row["numFiles"]
        
        # Convert size to GB
        size_in_gb = round((size_in_bytes / (1024 * 1024 * 1024)), 2)

        table_sizes.append(
            (
                table,
                "Table",
                size_in_gb,
                location,
                created_at,
                last_modified,
                partition_columns,
                clustering_columns,
                num_files,
            )
        )
    except Exception as e:
        error_message = str(e).split("\n")[0]
        print(f"Error processing table {table}: {error_message}")
        if "[EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE]" in error_message:
            table_sizes.append(
                (table, "View", None, None, None, None, None, None, None)  # Append None for missing details
            )
        else:
            table_sizes.append(
                (table, "Error", None, None, None, None, None, None, None)  # Append None for missing details
            )

# Create DataFrame from table sizes list
result_df = spark.createDataFrame(
    table_sizes,
    schema=table_schema,
)

result_df.createOrReplaceTempView("table_sizes_view")

# Using Spark SQL to format the result
formatted_result = spark.sql(f"""
SELECT 
    --CONCAT('{catalog}',".",'{schema}',".",TableName) AS FullTableName,
    TableName,
    TableType,
    CAST(SizeInGB AS STRING) AS SizeInGB,
    RIGHT(Location, 50) AS Location,
    CreatedAt,
    LastModified,
    PartitionColumns,
    ClusteringColumns,
    NumFiles,
    DATEDIFF(CURRENT_DATE(), LastModified) AS DaysSinceLastUpdate
    --,DATEDIFF(CURRENT_DATE(), CreatedAt) AS DaysSinceTableCreated
FROM table_sizes_view
ORDER BY SizeInGB DESC
""")

formatted_result.show(truncate=False)
