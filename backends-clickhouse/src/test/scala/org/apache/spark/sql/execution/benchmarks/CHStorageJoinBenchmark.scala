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
package org.apache.spark.sql.execution.benchmarks

import io.glutenproject.column.ColumnarBatchUtil
import io.glutenproject.execution.SerializableColumnarBatch
import io.glutenproject.vectorized.{BlockOutputStream, CHBlockWriterJniWrapper}

import org.apache.spark.SparkEnv
import org.apache.spark.benchmark.Benchmark
import org.apache.spark.io.CompressionCodec
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.BoundReference
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.execution.{RemoveTopColumnarToRow, SparkPlan}
import org.apache.spark.sql.execution.benchmark.SqlBasedBenchmark
import org.apache.spark.sql.execution.joins.ClickHouseBuildSideRelation

import java.io.{ByteArrayOutputStream, DataOutputStream}

object CHStorageJoinBenchmark extends SqlBasedBenchmark with CHSqlBasedBenchmark {

  protected lazy val appName = "CHStorageJoinBenchmark"
  protected lazy val thrdNum = "2"
  protected lazy val memorySize = "4G"
  protected lazy val offheapSize = "4G"

  override def getSparkSession: SparkSession = {

    val conf = getSparkcConf
      .set("spark.driver.maxResultSize", "0")
    SparkSession.builder.config(conf).getOrCreate()
  }
  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {

    val (parquetDir, readFileCnt, scanSchema, executedCnt, executedVanilla) =
      if (mainArgs.isEmpty) {
        ("/data/tpch-data/parquet/lineitem", 3, "l_orderkey,l_receiptdate", 5, true)
      } else {
        (mainArgs(0), mainArgs(1).toInt, mainArgs(2), mainArgs(3).toInt, mainArgs(4).toBoolean)
      }

    val chParquet = spark.sql(s"""
                                 |select $scanSchema from parquet.`$parquetDir`
                                 |
                                 |""".stripMargin)

    val benchmark = new Benchmark("CHStorageJoinBenchmark", readFileCnt, output = output)
    benchmark.addCase("mapPartitions", executedCnt) {
      _ => createBroadcastRelation1(RemoveTopColumnarToRow(chParquet.queryExecution.executedPlan))
    }
    benchmark.addCase("map", executedCnt) {
      _ => createBroadcastRelation2(RemoveTopColumnarToRow(chParquet.queryExecution.executedPlan))
    }
    benchmark.addCase("3", executedCnt) {
      _ => createBroadcastRelation3(RemoveTopColumnarToRow(chParquet.queryExecution.executedPlan))
    }
    benchmark.addCase("5", executedCnt) {
      _ => createBroadcastRelation5(RemoveTopColumnarToRow(chParquet.queryExecution.executedPlan))
    }
    benchmark.addCase("6", executedCnt) {
      _ => createBroadcastRelation6(RemoveTopColumnarToRow(chParquet.queryExecution.executedPlan))
    }
    benchmark.run()
  }

  def createBroadcastRelation1(child: SparkPlan): ClickHouseBuildSideRelation = {
    val countsAndBytes = child
      .executeColumnar()
      .mapPartitions {
        iter =>
          var _numRows: Long = 0
          // Use for reading bytes array from block
          val blockNativeWriter = new CHBlockWriterJniWrapper()
          while (iter.hasNext) {
            val batch = iter.next
            blockNativeWriter.write(batch)
            _numRows += batch.numRows
          }
          Iterator((_numRows, blockNativeWriter.collectAsByteArray()))
        // Iterator((_numRows, new Array[Byte](0)))
      }
      .collect
    val batches = countsAndBytes.map(_._2)
    val rawSize = batches.map(_.length).sum

    ClickHouseBuildSideRelation(
      NoopBroadcastMode,
      child.output,
      batches,
      Seq(BoundReference(0, child.output.head.dataType, child.output.head.nullable)))
  }

  def createBroadcastRelation2(child: SparkPlan): ClickHouseBuildSideRelation = {
    val countsAndBytes = child
      .executeColumnar()
      .map {
        cb =>
          val blockNativeWriter = new CHBlockWriterJniWrapper()
          blockNativeWriter.write(cb)
          (cb.numRows, blockNativeWriter.collectAsByteArray())
      }
      .collect
    val batches = countsAndBytes.map(_._2)
    val rawSize = batches.map(_.length).sum
    ClickHouseBuildSideRelation(
      NoopBroadcastMode,
      child.output,
      batches,
      Seq(BoundReference(0, child.output.head.dataType, child.output.head.nullable)))
  }

  def createBroadcastRelation3(child: SparkPlan): ClickHouseBuildSideRelation = {
    val countsAndBytes = child
      .executeColumnar()
      .map(cb => new SerializableColumnarBatch(cb))
      .collect

    val batches = countsAndBytes.map(_.getBuffer)
    val rawSize = batches.map(_.length).sum
    ClickHouseBuildSideRelation(
      NoopBroadcastMode,
      child.output,
      batches,
      Seq(BoundReference(0, child.output.head.dataType, child.output.head.nullable)))
  }

  def createBroadcastRelation4(child: SparkPlan): ClickHouseBuildSideRelation = {
    val countsAndBytes = child
      .executeColumnar()
      .mapPartitionsInternal {
        iter =>
          var count = 0
          val codec = CompressionCodec.createCodec(SparkEnv.get.conf)
          val bos = new ByteArrayOutputStream()
          val out = new DataOutputStream(codec.compressedOutputStream(bos))
          while (iter.hasNext) {
            val batch = iter.next()
            count += batch.numRows
            val blockNativeWriter = new CHBlockWriterJniWrapper()
            blockNativeWriter.write(batch)
            val bytes = blockNativeWriter.collectAsByteArray()
            out.writeInt(bytes.length)
            out.write(bytes)
          }
          out.writeInt(-1)
          out.flush()
          out.close()
          Iterator((count, bos.toByteArray))
      }
      .collect()

    val batches = countsAndBytes.map(_._2)
    val rawSize = batches.map(_.length).sum
    ClickHouseBuildSideRelation(
      NoopBroadcastMode,
      child.output,
      batches,
      Seq(BoundReference(0, child.output.head.dataType, child.output.head.nullable)))
  }

  def createBroadcastRelation5(child: SparkPlan): ClickHouseBuildSideRelation = {
    val countsAndBytes = child
      .executeColumnar()
      .mapPartitionsInternal {
        iter =>
          var count = 0
          val bos = new ByteArrayOutputStream(4 << 10)
          val buffer = new Array[Byte](4 << 10) // 4K
          val dout = new BlockOutputStream(bos, buffer, null, true, "lz4", buffer.length)
          while (iter.hasNext) {
            val batch = iter.next()
            count += batch.numRows
            dout.write(batch)
          }
          dout.flush()
          dout.close()
          Iterator((count, bos.toByteArray))
      }
      .collect()

    val batches = countsAndBytes.map(_._2)
    val rawSize = batches.map(_.length).sum
    ClickHouseBuildSideRelation(
      NoopBroadcastMode,
      child.output,
      batches,
      Seq(BoundReference(0, child.output.head.dataType, child.output.head.nullable)))
  }

  def createBroadcastRelation6(child: SparkPlan): ClickHouseBuildSideRelation = {
    val countsAndBytes = child
      .executeColumnar()
      .map {
        batch =>
          val bos = new ByteArrayOutputStream(4 << 10)
          val buffer = new Array[Byte](4 << 10) // 4K
          ColumnarBatchUtil.writeBatch(batch, bos, buffer, buffer.length)
          (batch.numRows, bos.toByteArray)
      }
      .collect()

    val batches = countsAndBytes.map(_._2)
    val rawSize = batches.map(_.length).sum
    ClickHouseBuildSideRelation(
      NoopBroadcastMode,
      child.output,
      batches,
      Seq(BoundReference(0, child.output.head.dataType, child.output.head.nullable)))
  }
}

/** NoopBroadcastMode does nothing. */
case object NoopBroadcastMode extends BroadcastMode {
  override def transform(rows: Array[InternalRow]): Any =
    throw new UnsupportedOperationException("NoOPBroadcastMode.transform")
  override def transform(rows: Iterator[InternalRow], sizeHint: Option[Long]): Any =
    throw new UnsupportedOperationException("NoOPBroadcastMode.transform")
  override def canonicalized: BroadcastMode =
    throw new UnsupportedOperationException("NoOPBroadcastMode.canonicalized")
}
