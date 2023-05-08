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

package io.glutenproject.execution

import org.apache.spark.SparkConf
import java.io.File

import io.glutenproject.utils.GlutenArrowUtil
import io.glutenproject.vectorized.ArrowWritableColumnVector

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.execution.GlutenColumnarToRowExec
import org.apache.spark.sql.execution.datasources.v2.arrow.SparkVectorUtil
import org.apache.spark.sql.execution.vectorized.{OnHeapColumnVector, WritableColumnVector}
import org.apache.spark.sql.types.{ArrayType, BooleanType, CalendarIntervalType, Decimal, DecimalType, IntegerType, MapType, StructField, StructType, TimestampType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnarMap}
import org.apache.spark.unsafe.types.CalendarInterval

class VeloxDataTypeValidationSuite extends WholeStageTransformerSuite {
  protected val rootPath: String = getClass.getResource("/").getPath
  override protected val backend: String = "velox"
  override protected val resourcePath: String = "/data-type-validation-data"
  override protected val fileFormat: String = "parquet"

  override def beforeAll(): Unit = {
    super.beforeAll()
    createDataTypeTable()
  }

  protected def createDataTypeTable(): Unit = {
    TPCHTables = Seq(
      "type1",
      "type2"
    ).map { table =>
      val tableDir = getClass.getResource(resourcePath).getFile
      val tablePath = new File(tableDir, table).getAbsolutePath
      val tableDF = spark.read.format(fileFormat).load(tablePath)
      tableDF.createOrReplaceTempView(table)
      (table, tableDF)
    }.toMap
  }

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.memory.offHeap.size", "2g")
      .set("spark.unsafe.exceptionOnMemoryLeak", "true")
      .set("spark.sql.autoBroadcastJoinThreshold", "10M")
      .set("spark.sql.sources.useV1SourceList", "avro")
  }

  test("Bool type") {
    runQueryAndCompare("select bool from type1 limit 1") { _ => }

    // Validation: BatchScan Filter Project Aggregate Expand Sort Limit
    runQueryAndCompare("select int, bool from type1 where bool == true  " +
      " group by grouping sets(int, bool) sort by int, bool limit 1") { _ => }

    // Validation: BroadHashJoin, Filter, Project
    super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "10M")
    runQueryAndCompare("select type1.bool from type1," +
      " type2 where type1.bool = type2.bool") { _ => }

    // Validation: ShuffledHashJoin, Filter, Project
    super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    runQueryAndCompare("select type1.bool from type1," +
      " type2 where type1.bool = type2.bool") { _ => }
  }

  test("Binary type") {
    // Validation: BatchScan Project Aggregate Expand Sort Limit
    runQueryAndCompare("select int, binary from type1 " +
      " group by grouping sets(int, binary) sort by int, binary limit 1") { _ => }

    // Validation: BroadHashJoin, Filter, Project
    super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "10M")
    runQueryAndCompare("select type1.binary from type1," +
      " type2 where type1.binary = type2.binary") { _ => }

    // Validation: ShuffledHashJoin, Filter, Project
    super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    runQueryAndCompare("select type1.binary from type1," +
      " type2 where type1.binary = type2.binary") { _ => }
  }

  test("String type") {
    // Validation: BatchScan Project Aggregate Expand Sort Limit
    runQueryAndCompare("select int, string from type1" +
      " group by grouping sets(int, string) sort by int, string limit 1") { _ => }

    // Validation: BroadHashJoin, Filter, Project
    super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "10M")
    runQueryAndCompare("select type1.string from type1," +
      " type2 where type1.string = type2.string") { _ => }

    // Validation: ShuffledHashJoin, Filter, Project
    super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    runQueryAndCompare("select type1.string from type1," +
      " type2 where type1.string = type2.string") { _ => }
  }


  test("Double type") {
    // Validation: BatchScan Project Aggregate Expand Sort Limit
    runQueryAndCompare("select int, double from type1 " +
      " group by grouping sets(int, double) sort by int, double limit 1") { _ => }

    // Validation: BroadHashJoin, Filter, Project
    super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "10M")
    runQueryAndCompare("select type1.double from type1," +
      " type2 where type1.double = type2.double") { _ => }

    // Validation: ShuffledHashJoin, Filter, Project
    super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    runQueryAndCompare("select type1.double from type1," +
      " type2 where type1.double = type2.double") { _ => }
  }

  test("Float type") {
    // Validation: BatchScan Project Aggregate Expand Sort Limit
    runQueryAndCompare("select int, float from type1 " +
      " group by grouping sets(int, float) sort by int, float limit 1") { _ => }

    // Validation: BroadHashJoin, Filter, Project
    super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "10M")
    runQueryAndCompare("select type1.float from type1," +
      " type2 where type1.float = type2.float") { _ => }

    // Validation: ShuffledHashJoin, Filter, Project
    super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    runQueryAndCompare("select type1.float from type1," +
      " type2 where type1.float = type2.float") { _ => }
  }

  test("Long type") {
    // Validation: BatchScan Project Aggregate Expand Sort Limit
    runQueryAndCompare("select int, long from type1 " +
      " group by grouping sets(int, long) sort by int, long limit 1") { _ => }

    // Validation: BroadHashJoin, Filter, Project
    super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "10M")
    runQueryAndCompare("select type1.long from type1," +
      " type2 where type1.long = type2.long") { _ => }

    // Validation: ShuffledHashJoin, Filter, Project
    super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    runQueryAndCompare("select type1.long from type1," +
      " type2 where type1.long = type2.long") { _ => }
  }

  test("Int type") {
    // Validation: BatchScan Project Aggregate Expand Sort Limit
    runQueryAndCompare("select int, short from type1 " +
      " group by grouping sets(int, short) sort by short, int limit 1") { _ => }

    // Validation: BroadHashJoin, Filter, Project
    super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "10M")
    runQueryAndCompare("select type1.int from type1," +
      " type2 where type1.int = type2.int") { _ => }

    // Validation: ShuffledHashJoin, Filter, Project
    super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    runQueryAndCompare("select type1.int from type1," +
      " type2 where type1.int = type2.int") { _ => }
  }

  test("Short type") {
    // Validation: BatchScan Project Aggregate Expand Sort Limit
    runQueryAndCompare("select int, short from type1 " +
      " group by grouping sets(int, short) sort by short, int limit 1") { _ => }

    // Validation: BroadHashJoin, Filter, Project
    super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "10M")
    runQueryAndCompare("select type1.short from type1," +
      " type2 where type1.short = type2.short") { _ => }

    // Validation: ShuffledHashJoin, Filter, Project
    super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    runQueryAndCompare("select type1.short from type1," +
      " type2 where type1.short = type2.short") { _ => }
  }

  test("Date type") {
    // Validation: BatchScan, Project, Aggregate, Sort.
    runQueryAndCompare("select int, date from type1 " +
      " group by grouping sets(int, date) sort by date, int limit 1") { df => {
      val executedPlan = getExecutedPlan(df)
      assert(executedPlan.exists(plan => plan.isInstanceOf[BatchScanExecTransformer]))
      assert(executedPlan.exists(plan => plan.isInstanceOf[ProjectExecTransformer]))
      assert(executedPlan.exists(plan => plan.isInstanceOf[GlutenHashAggregateExecTransformer]))
      assert(executedPlan.exists(plan => plan.isInstanceOf[SortExecTransformer]))
    }}

    // Validation: Expand, Filter.
    runQueryAndCompare("select date, string, sum(int) from type1 where date > date '1990-01-09' " +
      "group by rollup(date, string) order by date, string") { df => {
      val executedPlan = getExecutedPlan(df)
      assert(executedPlan.exists(plan => plan.isInstanceOf[ExpandExecTransformer]))
      assert(executedPlan.exists(plan => plan.isInstanceOf[GlutenFilterExecTransformer]))
    }}

    // Validation: Union.
    runQueryAndCompare(
      """
        |select count(d) from (
        | select date as d from type1
        | union all
        | select date as d from type1
        |);
        |""".stripMargin) { df => {
      assert(getExecutedPlan(df).exists(plan => plan.isInstanceOf[UnionExecTransformer]))
    }}

    // Validation: Limit.
    runQueryAndCompare(
      """
        |select date, int from (
        | select date, int from type1 limit 100
        |) where int != 0 limit 10;
        |""".stripMargin) {
      checkOperatorMatch[LimitTransformer]
    }

    // Validation: Window.
    runQueryAndCompare(
      "select row_number() over (partition by date order by date) from type1 order by int, date") {
      checkOperatorMatch[WindowExecTransformer]
    }

    // Validation: BroadHashJoin.
    withSQLConf("spark.sql.autoBroadcastJoinThreshold" -> "10M") {
      runQueryAndCompare("select type1.date from type1," +
        " type2 where type1.date = type2.date") {
        checkOperatorMatch[GlutenBroadcastHashJoinExecTransformer]
      }
    }

    // Validation: ShuffledHashJoin.
    withSQLConf("spark.sql.autoBroadcastJoinThreshold" -> "-1") {
      runQueryAndCompare("select type1.date from type1," +
        " type2 where type1.date = type2.date") {
        checkOperatorMatch[GlutenShuffledHashJoinExecTransformer]
      }
    }

    // Validation: SortMergeJoin.
    withSQLConf("spark.sql.autoBroadcastJoinThreshold" -> "-1") {
      withSQLConf("spark.gluten.sql.columnar.forceshuffledhashjoin" -> "false") {
        runQueryAndCompare("select type1.date from type1," +
          " type2 where type1.date = type2.date") {
          checkOperatorMatch[SortMergeJoinExecTransformer]
        }
      }
    }
  }

  test("Byte type") {
    // Validation: BatchScan Project Aggregate Expand Sort Limit
    runQueryAndCompare("select int, byte from type1 " +
      " group by grouping sets(int, byte) sort by byte, int limit 1") { _ => }

    // Validation: BroadHashJoin, Filter, Project
    super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "10M")
    runQueryAndCompare("select type1.byte from type1," +
      " type2 where type1.byte = type2.byte") { _ => }

    // Validation: ShuffledHashJoin, Filter, Project
    super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    runQueryAndCompare("select type1.byte from type1," +
      " type2 where type1.byte = type2.byte") { _ => }
  }

  test("Array type") {
    // Validation: BatchScan Project Aggregate Expand Sort Limit
    runQueryAndCompare("select int, array from type1 " +
      " group by grouping sets(int, array) sort by array, int limit 1") { _ => }

    // Validation: BroadHashJoin, Filter, Project
    super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "10M")
    runQueryAndCompare("select type1.array from type1," +
      " type2 where type1.array = type2.array") { _ => }

    // Validation: ShuffledHashJoin, Filter, Project
    super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    runQueryAndCompare("select type1.array from type1," +
      " type2 where type1.array = type2.array") { _ => }
  }

  test("Map type") {
    // Validation: BatchScan Project Limit
    runQueryAndCompare("select map from type1 limit 1") { _ => }
    // Validation: BatchScan Project Aggregate Sort Limit
    // TODO validate Expand operator support map type ?
    runQueryAndCompare("select map['key'] from type1 group by map['key']" +
      " sort by map['key'] limit 1") { _ => }

    // Validation: BroadHashJoin, Filter, Project
    super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "10M")
    runQueryAndCompare("select type1.map['key'] from type1," +
      " type2 where type1.map['key'] = type2.map['key']") { _ => }

    // Validation: ShuffledHashJoin, Filter, Project
    super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    runQueryAndCompare("select type1.map['key'] from type1," +
      " type2 where type1.map['key'] = type2.map['key']") { _ => }
  }

  test("Decimal type") {
    // Validation: BatchScan Project Aggregate Expand Sort Limit
    runQueryAndCompare("select int, decimal from type1 " +
      " group by grouping sets(int, decimal) sort by decimal, int limit 1") { _ => }

    // Validation: BroadHashJoin, Filter, Project
    super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "10M")
    runQueryAndCompare("select type1.decimal from type1," +
      " type2 where type1.decimal = type2.decimal") { _ => }

    // Validation: ShuffledHashJoin, Filter, Project
    super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    runQueryAndCompare("select type1.decimal from type1," +
      " type2 where type1.decimal = type2.decimal") { _ => }
  }

  test("Timestamp type") {
    // Validation: BatchScan Project Aggregate Expand Sort Limit
    runQueryAndCompare("select int, timestamp from type1 " +
      " group by grouping sets(int, timestamp) sort by timestamp, int limit 1") { _ => }

    // Validation: BroadHashJoin, Filter, Project
    super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "10M")
    runQueryAndCompare("select type1.timestamp from type1," +
      " type2 where type1.timestamp = type2.timestamp") { _ => }

    // Validation: ShuffledHashJoin, Filter, Project
    super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    runQueryAndCompare("select type1.timestamp from type1," +
      " type2 where type1.timestamp = type2.timestamp") { _ => }
  }

  test("Struct type") {
    // Validation: BatchScan Project Limit
    runQueryAndCompare("select struct from type1") { _ => }
    // Validation: BatchScan Project Aggregate Sort Limit
    // TODO validate Expand operator support Struct type ?
    runQueryAndCompare("select int, struct.struct_1 from type1 " +
      "sort by struct.struct_1 limit 1") { _ => }

    // Validation: BroadHashJoin, Filter, Project
    super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "10M")
    runQueryAndCompare("select type1.struct.struct_1 from type1," +
      " type2 where type1.struct.struct_1 = type2.struct.struct_1") { _ => }

    // Validation: ShuffledHashJoin, Filter, Project
    super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    runQueryAndCompare("select type1.struct.struct_1 from type1," +
      " type2 where type1.struct.struct_1 = type2.struct.struct_1") { _ => }
  }

  test("RowToArrow and VeloxToRow") {
    withSQLConf("spark.gluten.sql.columnar.batchscan" -> "false") {
      runQueryAndCompare("select count(short, bool, byte, int, long, float, double, " +
        "string, binary, date) from type1") { _ => }
    }
  }

  test("RowToArrow native") {
    withSQLConf("spark.gluten.sql.columnar.batchscan" -> "false") {
      runQueryAndCompare("select count(short, bool, byte, int, long, float, double, " +
        "string, binary) from type1") { _ => }
    }
  }

  // Some type cannot read by velox, so we cannot test it
  test("RowToArrow decimal type") {
    val schema = StructType(Seq(
      StructField("Cc", DecimalType(10, 2))
    ))
    val row = new GenericInternalRow(1)
    row(0) = new Decimal().set(BigDecimal("100.92"))
    val converters = new RowToColumnConverter(schema)
    val vectors: Seq[WritableColumnVector] =
      ArrowWritableColumnVector.allocateColumns(1, schema)
    converters.convert(row, vectors.toArray)

    assert(vectors.head.getDecimal(0, 10, 2).toString()
      .equals("100.92"))
  }

  test("RowToArrow timestamp type") {
    val schema = StructType(Seq(
      StructField("Cc", TimestampType)
    ))
    val row = new GenericInternalRow(1)
    row(0) = 5L
    val converters = new RowToColumnConverter(schema)
    val vectors: Seq[WritableColumnVector] =
      ArrowWritableColumnVector.allocateColumns(1, schema)
    converters.convert(row, vectors.toArray)

    assert(vectors.head.getLong(0) == 5)
  }


  test("RowToArrow calender type") {
    val schema = StructType(Seq(
      StructField("Cc", CalendarIntervalType)
    ))
    val row = new GenericInternalRow(1)
    row(0) = new CalendarInterval(12, 1, 0)
    assertThrows[UnsupportedOperationException](
      ArrowWritableColumnVector.allocateColumns(1, schema))
  }

  test("RowToArrow array type") {
    val schema = StructType(Seq(
      StructField("Cc", ArrayType(IntegerType))
    ))
    val row = new GenericInternalRow(1)
    row(0) = new GenericArrayData(Seq(5, 6))
    val converters = new RowToColumnConverter(schema)
    val vectors: Seq[WritableColumnVector] =
      ArrowWritableColumnVector.allocateColumns(1, schema)
    converters.convert(row, vectors.toArray)
    val result = vectors.head.getArray(0)
    assert(result.getInt(0) == 5 && result.getInt(1) == 6)

    vectors.foreach(
      _.asInstanceOf[ArrowWritableColumnVector].getValueVector.setValueCount(1))
    SparkVectorUtil.toArrowRecordBatch(new ColumnarBatch(vectors.toArray, 1))
  }

  test("RowToArrow map type") {
    val schema = StructType(Seq(
      StructField("Cc", MapType(IntegerType, BooleanType))
    ))
    val row = new GenericInternalRow(1)
    val keys = new OnHeapColumnVector(1, IntegerType)
    val values = new OnHeapColumnVector(1, BooleanType)
    keys.putInt(0, 1)
    values.putBoolean(0, true)

    val columnarMap = new ColumnarMap(keys, values, 0, 1)
    row(0) = columnarMap
    val converters = new RowToColumnConverter(schema)
    val vectors: Seq[WritableColumnVector] =
      ArrowWritableColumnVector.allocateColumns(1, schema)
    converters.convert(row, vectors.toArray)
    val result = vectors.head.getMap(0)
    assert(result.keyArray.getInt(0) == 1 && result.valueArray().getBoolean(0))
  }

  test("RowToArrow struct type") {
    val schema = StructType(Seq(
      StructField("Cc", StructType(Seq(
        StructField("int", IntegerType)
      )))
    ))
    val row = new GenericInternalRow(1)
    row(0) = 5
    val structRow = new GenericInternalRow(1)
    structRow(0) = row
    val converters = new RowToColumnConverter(schema)
    val vectors: Seq[WritableColumnVector] =
      ArrowWritableColumnVector.allocateColumns(1, schema)
    converters.convert(structRow, vectors.toArray)
    val result = vectors.head.getStruct(0)
    assert(result.getInt(0) == 5)
  }

  test("ArrowToRow native") {
    withSQLConf("spark.gluten.sql.columnar.batchscan" -> "false") {
      val sqlStr = "select short, bool, byte, int, long, float, double, string, binary, date " +
        "from type1 limit 1"
      val df = spark.sql(sqlStr)
      var expected: Seq[Row] = null
      withSQLConf(vanillaSparkConfs(): _*) {
        val df = spark.sql(sqlStr)
        expected = df.collect()
      }
      checkAnswer(df.repartition(10), expected)
      assert(df.queryExecution.executedPlan.find(_.isInstanceOf[GlutenColumnarToRowExec]).isDefined)
    }
  }


  test("ArrowToVelox") {
    withSQLConf("spark.gluten.sql.columnar.batchscan" -> "false",
              "spark.sql.shuffle.partitions" -> "5") {
      val sqlStr = "select short, bool, byte, int, long, float, double, string, binary " +
        "from type1"
      val df = spark.sql(sqlStr)
      var expected: Seq[Row] = null
      withSQLConf(vanillaSparkConfs(): _*) {
        val df = spark.sql(sqlStr)
        expected = df.collect()
      }
      checkAnswer(df.repartition(20), expected)
      assert(df.queryExecution.executedPlan.find(_.isInstanceOf[GlutenColumnarToRowExec]).isDefined)
    }
  }

  test("Velox Parquet Write") {
    withTempDir { dir =>
      val write_path = dir.toURI.getPath
      val data_path = getClass.getResource("/").getPath + "/data-type-validation-data/type1"
      val df = spark.read.format("parquet").load(data_path)
      
      // Timestamp, array and map type is not supported.
      df.drop("timestamp").drop("array").drop("map").
        write.mode("append").format("velox").save(write_path)
    }
  }
}
