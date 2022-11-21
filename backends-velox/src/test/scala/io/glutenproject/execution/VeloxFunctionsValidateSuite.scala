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

import java.nio.file.Files

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.optimizer.{ConstantFolding, NullPropagation}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{col, expr}


import scala.collection.JavaConverters._

class VeloxFunctionsValidateSuite extends WholeStageTransformerSuite {

  override protected val resourcePath: String = "/tpch-data-parquet-velox"
  override protected val fileFormat: String = "parquet"
  override protected val backend: String = "velox"

  private var parquetPath: String = _

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.memory.offHeap.size", "2g")
      .set("spark.unsafe.exceptionOnMemoryLeak", "false")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      .set("spark.sql.sources.useV1SourceList", "avro")
      .set("spark.sql.optimizer.excludedRules", ConstantFolding.ruleName + "," +
        NullPropagation.ruleName)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    createTPCHNotNullTables()

    val lfile = Files.createTempFile("", ".parquet").toFile
    lfile.deleteOnExit()
    parquetPath = lfile.getAbsolutePath

    val schema = StructType(Array(
      StructField("double_field1", DoubleType, true),
      StructField("string_field1", StringType, true)
    ))
    val rowData = Seq(
      Row(1.025, "{\"a\":\"b\"}"),
      Row(1.035, null),
      Row(1.045, null)
    )

    var dfParquet = spark.createDataFrame(rowData.asJava, schema)
    dfParquet.coalesce(1)
      .write
      .format("parquet")
      .mode("overwrite")
      .parquet(parquetPath)

    spark.catalog.createTable("datatab", parquetPath, fileFormat)
  }

  test("Test chr function") {
    val df = runQueryAndCompare("SELECT chr(l_orderkey + 64) from lineitem limit 1") {
      checkOperatorMatch[ProjectExecTransformer]
    }
    df.show()
    df.explain(false)
  }

  test("Test abs function") {
    val df = runQueryAndCompare("SELECT abs(l_orderkey) from lineitem limit 1") {
      checkOperatorMatch[ProjectExecTransformer]
    }
    df.show()
    df.explain(false)
  }

  test("Test ceil function") {
    val df = runQueryAndCompare("SELECT ceil(cast(l_orderkey as long)) from lineitem limit 1") {
      checkOperatorMatch[ProjectExecTransformer]
    }
    df.show()
    df.explain(false)
    df.printSchema()
  }

  test("Test floor function") {
    val df = runQueryAndCompare("SELECT floor(cast(l_orderkey as long)) from lineitem limit 1") {
      checkOperatorMatch[ProjectExecTransformer]
    }
    df.show()
    df.explain(false)
    df.printSchema()
  }

  test("Test Exp function") {
    val df = runQueryAndCompare("SELECT exp(l_orderkey) from lineitem limit 1") {
      checkOperatorMatch[ProjectExecTransformer]
    }
    df.show()
    df.explain(false)
    df.printSchema()
  }

  test("Test Power function") {
    val df = runQueryAndCompare("SELECT power(l_orderkey, 2) from lineitem limit 1") {
      checkOperatorMatch[ProjectExecTransformer]
    }
    df.show()
    df.explain(false)
    df.printSchema()
  }

  test("Test Pmod function") {
    val df = runQueryAndCompare("SELECT pmod(cast(l_orderkey as int), 3) from lineitem limit 1") {
      checkOperatorMatch[ProjectExecTransformer]
    }
    df.show()
    df.explain(false)
    df.printSchema()
  }

  ignore("Test round function") {
    val df = runQueryAndCompare("SELECT round(cast(l_orderkey as int), 2)" +
      "from lineitem limit 1") {
      checkOperatorMatch[ProjectExecTransformer]
    }
    df.show()
    df.explain(false)
    df.printSchema()
  }

  test("Test greatest function") {
    val df = runQueryAndCompare("SELECT greatest(l_orderkey, l_orderkey)" +
      "from lineitem limit 1") {
      checkOperatorMatch[ProjectExecTransformer]
    }
    df.show()
    df.explain(false)
    df.printSchema()
  }

  test("Test least function") {
    val df = runQueryAndCompare("SELECT least(l_orderkey, l_orderkey)" +
      "from lineitem limit 1") {
      checkOperatorMatch[ProjectExecTransformer]
    }
    df.show()
    df.explain(false)
    df.printSchema()
  }

  test("Test hash function") {
    val df = runQueryAndCompare("SELECT hash(l_orderkey) from lineitem limit 1") {
      checkOperatorMatch[ProjectExecTransformer]
    }
    df.show()
    df.explain(false)
    df.printSchema()
  }

  test("Test get_json_object datatab function") {
    val df = runQueryAndCompare("SELECT get_json_object(string_field1, '$.a') " +
      "from datatab limit 1;") {
      checkOperatorMatch[ProjectExecTransformer]
    }
    df.show()
    df.explain(false)
    df.printSchema()
  }

  test("Test get_json_object lineitem function") {
    val df = runQueryAndCompare("SELECT l_orderkey, get_json_object('{\"a\":\"b\"}', '$.a') " +
      "from lineitem limit 1;") {
      checkOperatorMatch[ProjectExecTransformer]
    }
    df.show()
    df.explain(false)
    df.printSchema()
  }

  test("Test acos function") {
    val df = runQueryAndCompare("SELECT acos(l_orderkey) from lineitem limit 1") {
      checkOperatorMatch[ProjectExecTransformer]
    }
    df.show()
    df.explain(false)
    df.printSchema()
  }

  test("Test asin function") {
    val df = runQueryAndCompare("SELECT asin(l_orderkey) from lineitem limit 1") {
      checkOperatorMatch[ProjectExecTransformer]
    }
    df.show()
    df.explain(false)
    df.printSchema()
  }

  test("Test atan function") {
    val df = runQueryAndCompare("SELECT atan(l_orderkey) from lineitem limit 1") {
      checkOperatorMatch[ProjectExecTransformer]
    }
    df.show()
    df.explain(false)
    df.printSchema()
  }

  ignore("Test atan2 function datatab") {
    val df = runQueryAndCompare("SELECT atan2(double_field1, 0) from datatab limit 1") {
      checkOperatorMatch[ProjectExecTransformer]
    }
    df.show()
    df.explain(false)
    df.printSchema()
  }

  test("Test ceiling function") {
    val df = runQueryAndCompare("SELECT ceiling(cast(l_orderkey as long)) from lineitem limit 1") {
      checkOperatorMatch[ProjectExecTransformer]
    }
    df.show()
    df.explain(false)
    df.printSchema()
  }

  test("Test cos function") {
    val df = runQueryAndCompare("SELECT cos(l_orderkey) from lineitem limit 1") {
      checkOperatorMatch[ProjectExecTransformer]
    }
    df.show()
    df.explain(false)
    df.printSchema()
  }

  test("Test cosh function") {
    val df = runQueryAndCompare("SELECT cosh(l_orderkey) from lineitem limit 1") {
      checkOperatorMatch[ProjectExecTransformer]
    }
    df.show()
    df.explain(false)
    df.printSchema()
  }

  test("Test degrees function") {
    val df = runQueryAndCompare("SELECT degrees(l_orderkey) from lineitem limit 1") {
      checkOperatorMatch[ProjectExecTransformer]
    }
    df.show()
    df.explain(false)
    df.printSchema()
  }

  test("Test log10 function") {
    val df = runQueryAndCompare("SELECT log10(l_orderkey) from lineitem limit 1") {
      checkOperatorMatch[ProjectExecTransformer]
    }
    df.show()
    df.explain(false)
    df.printSchema()
  }
}
