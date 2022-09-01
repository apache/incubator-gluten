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

package io.glutenproject.backendsapi.clickhouse

import scala.collection.mutable.ArrayBuffer

import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi.ISparkPlanExecApi
import io.glutenproject.execution._
import io.glutenproject.vectorized.{BlockNativeWriter, CHColumnarBatchSerializer}

import org.apache.spark.{ShuffleDependency, SparkException}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.{GenShuffleWriterParameters, GlutenShuffleWriterWrapper}
import org.apache.spark.shuffle.utils.CHShuffleUtil
import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, BoundReference, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastMode, Partitioning}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarRule, ProjectExec, SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.execution.joins.{BuildSideRelation, ClickHouseBuildSideRelation, HashedRelationBroadcastMode}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.utils.CHExecUtil
import org.apache.spark.sql.extension.{CHDataSourceV2Strategy, ClickHouseAnalysis}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

class CHSparkPlanExecApi extends ISparkPlanExecApi {

  /**
    * Generate NativeColumnarToRowExec.
    *
    * @param child
    * @return
    */
  override def genNativeColumnarToRowExec(child: SparkPlan): NativeColumnarToRowExec = {
    new BlockNativeColumnarToRowExec(child);
  }


  /**
    * Generate RowToColumnarExec.
    *
    * @param child
    * @return
    */
  override def genRowToColumnarExec(child: SparkPlan): RowToArrowColumnarExec =
    throw new UnsupportedOperationException(
      "Cannot support RowToArrowColumnarExec operation with ClickHouse backend.")

  /**
    * Generate FilterExecTransformer.
    *
    * @param condition : the filter condition
    * @param child     : the chid of FilterExec
    * @return the transformer of FilterExec
    */
  override def genFilterExecTransformer(condition: Expression, child: SparkPlan)
  : FilterExecBaseTransformer = FilterExecTransformer(condition, child)


  /**
    * Generate HashAggregateExecTransformer.
    */
  override def genHashAggregateExecTransformer(
                                      requiredChildDistributionExpressions: Option[Seq[Expression]],
                                      groupingExpressions: Seq[NamedExpression],
                                      aggregateExpressions: Seq[AggregateExpression],
                                      aggregateAttributes: Seq[Attribute],
                                      initialInputBufferOffset: Int,
                                      resultExpressions: Seq[NamedExpression],
                                      child: SparkPlan): HashAggregateExecBaseTransformer =
    CHHashAggregateExecTransformer(
      requiredChildDistributionExpressions,
      groupingExpressions,
      aggregateExpressions,
      aggregateAttributes,
      initialInputBufferOffset,
      resultExpressions,
      child)

  /**
    * Generate ShuffleDependency for ColumnarShuffleExchangeExec.
    *
    * @return
    */
  // scalastyle:off argcount
  override def genShuffleDependency(rdd: RDD[ColumnarBatch],
                                    outputAttributes: Seq[Attribute],
                                    newPartitioning: Partitioning,
                                    serializer: Serializer,
                                    writeMetrics: Map[String, SQLMetric],
                                    dataSize: SQLMetric,
                                    bytesSpilled: SQLMetric,
                                    numInputRows: SQLMetric,
                                    computePidTime: SQLMetric,
                                    splitTime: SQLMetric,
                                    spillTime: SQLMetric,
                                    compressTime: SQLMetric,
                                    prepareTime: SQLMetric
                                   ): ShuffleDependency[Int, ColumnarBatch, ColumnarBatch] = {
    CHExecUtil.genShuffleDependency(rdd, outputAttributes, newPartitioning, serializer,
      writeMetrics, dataSize, bytesSpilled, numInputRows, computePidTime, splitTime,
      spillTime, compressTime, prepareTime)
  }
  // scalastyle:on argcount

  /**
    * Generate ColumnarShuffleWriter for ColumnarShuffleManager.
    *
    * @return
    */
  override def genColumnarShuffleWriter[K, V](parameters: GenShuffleWriterParameters[K, V]
                                             ): GlutenShuffleWriterWrapper[K, V] = {
    CHShuffleUtil.genColumnarShuffleWriter(parameters)
  }

  /**
    * Generate ColumnarBatchSerializer for ColumnarShuffleExchangeExec.
    *
    * @return
    */
  override def createColumnarBatchSerializer(schema: StructType,
                                             readBatchNumRows: SQLMetric,
                                             numOutputRows: SQLMetric): Serializer = {
    new CHColumnarBatchSerializer(readBatchNumRows, numOutputRows)
  }


  /**
    * Create broadcast relation for BroadcastExchangeExec
    */
  override def createBroadcastRelation(mode: BroadcastMode,
                                       child: SparkPlan,
                                       numOutputRows: SQLMetric,
                                       dataSize: SQLMetric): BuildSideRelation = {
    val hashedRelationBroadcastMode = mode.asInstanceOf[HashedRelationBroadcastMode]
    val (newChild, newOutput, newBuildKeys) = if (hashedRelationBroadcastMode.key
      .forall(k => k.isInstanceOf[AttributeReference] || k.isInstanceOf[BoundReference])) {
      (child, child.output, Seq.empty[Expression])
    } else {
      // pre projection in case of expression join keys
      val buildKeys = hashedRelationBroadcastMode.key
      val appendedProjections = new ArrayBuffer[NamedExpression]()
      val preProjectionBuildKeys = buildKeys.zipWithIndex.map { case (e, idx) =>
        e match {
          case b: BoundReference => child.output(b.ordinal)
          case o: Expression =>
            val newExpr = Alias(o, "col_" + idx)()
            appendedProjections += newExpr
            newExpr
        }
      }
      val newChild = child match {
        case wf: WholeStageTransformerExec =>
          wf.withNewChildren(Seq(ProjectExecTransformer(
            child.output ++ appendedProjections.toSeq,
            wf.child)))
        case w: WholeStageCodegenExec =>
          w.withNewChildren(Seq(ProjectExec(child.output ++ appendedProjections.toSeq, w.child)))
      }
      (newChild, (child.output ++ appendedProjections.toSeq).map(_.toAttribute),
        preProjectionBuildKeys)
    }
    val countsAndBytes = newChild
      .executeColumnar()
      .mapPartitions { iter =>
        var _numRows: Long = 0

        // Use for reading bytes array from block
        val blockNativeWriter = new BlockNativeWriter()
        while (iter.hasNext) {
          val batch = iter.next
          blockNativeWriter.write(batch)
          _numRows += batch.numRows
        }
        Iterator((_numRows, blockNativeWriter.collectAsByteArray()))
      }
      .collect

    val batches = countsAndBytes.map(_._2)
    val rawSize = batches.map(_.length).sum
    if (rawSize >= BroadcastExchangeExec.MAX_BROADCAST_TABLE_BYTES) {
      throw new SparkException(
        s"Cannot broadcast the table that is larger than 8GB: ${rawSize >> 30} GB")
    }
    numOutputRows += countsAndBytes.map(_._1).sum
    dataSize += rawSize
    ClickHouseBuildSideRelation(mode, newOutput, batches, newBuildKeys)
  }

  /**
    * Generate extended DataSourceV2 Strategy.
    * Currently only for ClickHouse backend.
    *
    * @return
    */
  override def genExtendedDataSourceV2Strategy(spark: SparkSession): Strategy = {
    CHDataSourceV2Strategy(spark)
  }

  /**
    * Generate extended Analyzer.
    * Currently only for ClickHouse backend.
    *
    * @return
    */
  override def genExtendedAnalyzer(spark: SparkSession, conf: SQLConf): Rule[LogicalPlan] = {
    new ClickHouseAnalysis(spark, conf)
  }

  /**
    * Generate extended Rule.
    * Currently only for Velox backend.
    *
    * @return
    */
  override def genExtendedRule(spark: SparkSession): ColumnarRule = {
    throw new UnsupportedOperationException(
      "Cannot support extending Rule for ClickHouse backend.")
  }

  /**
    * Generate extended Strategy.
    * Currently only for Velox backend.
    *
    * @return
    */
  override def genExtendedStrategy(): Strategy = {
    throw new UnsupportedOperationException(
      "Cannot support extending Strategy for ClickHouse backend.")
  }

  /**
    * Get the backend api name.
    *
    * @return
    */
  override def getBackendName: String = GlutenConfig.GLUTEN_CLICKHOUSE_BACKEND
}
