import org.apache.spark.sql.execution.debug._
import scala.io.Source
import java.io.File
import java.util.Arrays
import org.apache.spark.sql.types.{DoubleType, TimestampType, LongType, IntegerType}

val lineitem_parquet_path = "file:///mnt/DP_disk1/sparkuser/tpch/tpch_parquet_nonpart_1000/lineitem"
val customer_parquet_path = "file:///mnt/DP_disk1/sparkuser/tpch/tpch_parquet_nonpart_1000/customer"
val nation_parquet_path = "file:///mnt/DP_disk1/sparkuser/tpch/tpch_parquet_nonpart_1000/nation"
val orders_parquet_path = "file:///mnt/DP_disk1/sparkuser/tpch/tpch_parquet_nonpart_1000/orders"
val part_parquet_path = "file:///mnt/DP_disk1/sparkuser/tpch/tpch_parquet_nonpart_1000/part"
val partsupp_parquet_path = "file:///mnt/DP_disk1/sparkuser/tpch/tpch_parquet_nonpart_1000/partsupp"
val region_path_path = "file:///mnt/DP_disk1/sparkuser/tpch/tpch_parquet_nonpart_1000/region"
val supplier_parquet_path = "file:///mnt/DP_disk1/sparkuser/tpch/tpch_parquet_nonpart_1000/supplier"



val lineitem = spark.read.format("parquet").load(lineitem_parquet_path)
val customer = spark.read.format("parquet").load(customer_parquet_path)
val nation = spark.read.format("parquet").load(nation_parquet_path)
val orders = spark.read.format("parquet").load(orders_parquet_path)
val part = spark.read.format("parquet").load(part_parquet_path)
val partsupp = spark.read.format("parquet").load(partsupp_parquet_path)
val region = spark.read.format("parquet").load(region_path_path)
val supplier = spark.read.format("parquet").load(supplier_parquet_path)

val nation_dwrf_path = "file:///mnt/DP_disk1/sparkuser/tpch/tpch_dwrf_nonpart_1000_32K/nation"
val lineitem_dwrf_path = "file:///mnt/DP_disk1/sparkuser/tpch/tpch_dwrf_nonpart_1000_32K/lineitem"
val customer_dwrf_path = "file:///mnt/DP_disk1/sparkuser/tpch/tpch_dwrf_nonpart_1000_32K/customer"
val nation_dwrf_path = "file:///mnt/DP_disk1/sparkuser/tpch/tpch_dwrf_nonpart_1000_32K/nation"
val orders_dwrf_path = "file:///mnt/DP_disk1/sparkuser/tpch/tpch_dwrf_nonpart_1000_32K/orders"
val part_dwrf_path = "file:///mnt/DP_disk1/sparkuser/tpch/tpch_dwrf_nonpart_1000_32K/nation/part"
val partsupp_dwrf_path = "file:///mnt/DP_disk1/sparkuser/tpch/tpch_dwrf_nonpart_1000_32K/partsupp"
val supplier_dwrf_path = "file:///mnt/DP_disk1/sparkuser/tpch/tpch_dwrf_nonpart_1000_32K/supplier"

nation.write.mode("append").format("dwrf").save(nation_dwrf_path)
lineitem.write.mode("append").format("dwrf").save(lineitem_dwrf_path)
customer.write.mode("append").format("dwrf").save(customer_dwrf_path)
orders.write.mode("append").format("dwrf").save(orders_dwrf_path)
part.write.mode("append").format("dwrf").save(part_dwrf_path)
partsupp.write.mode("append").format("dwrf").save(partsupp_dwrf_path)
region.write.mode("append").format("dwrf").save(region_dwrf_path)
supplier.write.mode("append").format("dwrf").save(supplier_dwrf_path)


