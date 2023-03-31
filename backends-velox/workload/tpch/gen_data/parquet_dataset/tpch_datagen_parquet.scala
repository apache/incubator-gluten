import com.databricks.spark.sql.perf.tpch._


val scaleFactor = "100" // scaleFactor defines the size of the dataset to generate (in GB).
val numPartitions = 200  // how many dsdgen partitions to run - number of input tasks.

val format = "parquet" // valid spark format like parquet "parquet".
val rootDir = "/PATH/TO/TPCH_PARQUET_PATH" // root directory of location to create data in.
val dbgenDir = "/PATH/TO/TPCH_DBGEN" // location of dbgen

val tables = new TPCHTables(spark.sqlContext,
    dbgenDir = dbgenDir,
    scaleFactor = scaleFactor,
    useDoubleForDecimal = false, // true to replace DecimalType with DoubleType
    useStringForDate = false) // true to replace DateType with StringType


tables.genData(
    location = rootDir,
    format = format,
    overwrite = true, // overwrite the data that is already there
    partitionTables = false, // do not create the partitioned fact tables
    clusterByPartitionColumns = false, // shuffle to get partitions coalesced into single files.
    filterOutNullPartitionValues = false, // true to filter out the partition with NULL key value
    tableFilter = "", // "" means generate all tables
    numPartitions = numPartitions) // how many dsdgen partitions to run - number of input tasks.

