/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import org.apache.spark.sql.execution.debug._
import scala.io.Source
import java.io.File
import java.util.Arrays
import org.apache.spark.sql.types.{DoubleType, TimestampType, LongType, IntegerType}

val parquet_file_path = "/PATH/TO/TPCH_PARQUET_PATH"
val dwrf_file_path = "/PATH/TO/TPCH_DWRF_PATH"

val lineitem_parquet_path = "file://" + parquet_file_path + "/lineitem"
val customer_parquet_path = "file://" + parquet_file_path + "/customer"
val nation_parquet_path = "file://" + parquet_file_path + "/nation"
val orders_parquet_path = "file://" + parquet_file_path + "/orders"
val part_parquet_path = "file://" + parquet_file_path + "/part"
val partsupp_parquet_path = "file://" + parquet_file_path + "/partsupp"
val region_path_path = "file://" + parquet_file_path + "/region"
val supplier_parquet_path = "file://" + parquet_file_path + "/supplier"

val lineitem = spark.read.format("parquet").load(lineitem_parquet_path)
val customer = spark.read.format("parquet").load(customer_parquet_path)
val nation = spark.read.format("parquet").load(nation_parquet_path)
val orders = spark.read.format("parquet").load(orders_parquet_path)
val part = spark.read.format("parquet").load(part_parquet_path)
val partsupp = spark.read.format("parquet").load(partsupp_parquet_path)
val region = spark.read.format("parquet").load(region_path_path)
val supplier = spark.read.format("parquet").load(supplier_parquet_path)

val lineitem_dwrf_path = "file://" + dwrf_file_path + "/lineitem"
val customer_dwrf_path = "file://" + dwrf_file_path + "/customer"
val nation_dwrf_path = "file://" + dwrf_file_path + "/nation"
val orders_dwrf_path = "file://" + dwrf_file_path + "/orders"
val part_dwrf_path = "file://" + dwrf_file_path + "/part"
val partsupp_dwrf_path = "file://" + dwrf_file_path + "/partsupp"
val region_dwrf_path = "file://" + dwrf_file_path + "/region"
val supplier_dwrf_path = "file://" + dwrf_file_path + "/supplier"

lineitem.write.mode("append").format("dwrf").save(lineitem_dwrf_path)
customer.write.mode("append").format("dwrf").save(customer_dwrf_path)
nation.write.mode("append").format("dwrf").save(nation_dwrf_path)
orders.write.mode("append").format("dwrf").save(orders_dwrf_path)
part.write.mode("append").format("dwrf").save(part_dwrf_path)
partsupp.write.mode("append").format("dwrf").save(partsupp_dwrf_path)
region.write.mode("append").format("dwrf").save(region_dwrf_path)
supplier.write.mode("append").format("dwrf").save(supplier_dwrf_path)


