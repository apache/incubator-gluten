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

package org.apache.spark.sql.execution.python

import java.io._
import java.net._
import java.util.concurrent.atomic.AtomicBoolean

import com.intel.oap.execution.{TransformContext, TransformSupport}
import com.intel.oap.expression._
import com.intel.oap.vectorized._

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import org.apache.arrow.vector.{ValueVector, VectorLoader, VectorSchemaRoot}
import org.apache.spark.{ContextAwareIterator, SparkEnv}
import org.apache.spark.TaskContext
import org.apache.spark.api.python.ChainedPythonFunctions
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.python.EvalPythonExec
import org.apache.spark.sql.execution.datasources.v2.arrow.SparkMemoryUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnVector, ColumnarBatch}
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.rdd.RDD

case class ArrowEvalPythonExecTransformer(udfs: Seq[PythonUDF], resultAttrs: Seq[Attribute],
                                          child: SparkPlan, evalType: Int)
    extends EvalPythonExec with TransformSupport {
  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "output_batches"),
    "numInputBatches" -> SQLMetrics.createMetric(sparkContext, "input_batches"),
    "processTime" -> SQLMetrics.createTimingMetric(sparkContext, "totaltime_arrow_udf"))

  private val batchSize = conf.arrowMaxRecordsPerBatch
  private val sessionLocalTimeZone = conf.sessionLocalTimeZone
  private val pythonRunnerConf = ArrowUtils.getPythonRunnerConfMap(conf)

  override def supportsColumnar: Boolean = true
  
  protected def evaluate(
      funcs: Seq[ChainedPythonFunctions],
      argOffsets: Array[Array[Int]],
      iter: Iterator[InternalRow],
      schema: StructType,
      context: TaskContext): Iterator[InternalRow] = {
    throw new NotImplementedError("evaluate Internal row is not supported")
  }

  protected def evaluateColumnar(
    funcs: Seq[ChainedPythonFunctions],
    argOffsets: Array[Array[Int]],
    iter: Iterator[ColumnarBatch],
    schema: StructType,
    context: TaskContext): Iterator[ColumnarBatch] = {

    val outputTypes = output.drop(child.output.length).map(_.dataType)

    // Use Coalecse to improve performance in future
    val batchIter = new CloseableColumnBatchIterator(iter)

    val columnarBatchIter = new ColumnarArrowPythonRunner(
      funcs,
      evalType,
      argOffsets,
      schema,
      sessionLocalTimeZone,
      pythonRunnerConf).compute(batchIter, context.partitionId(), context)

    columnarBatchIter.map { batch =>
      val actualDataTypes = (0 until batch.numCols()).map(i => batch.column(i).dataType())
      assert(outputTypes == actualDataTypes, "Invalid schema from arrow_udf: " +
        s"expected ${outputTypes.mkString(", ")}, got ${actualDataTypes.mkString(", ")}")
      batch
    }
  }

  private def collectFunctions(udf: PythonUDF): (ChainedPythonFunctions, Seq[Expression]) = {
    udf.children match {
      case Seq(u: PythonUDF) =>
        val (chained, children) = collectFunctions(u)
        (ChainedPythonFunctions(chained.funcs ++ Seq(udf.func)), children)
      case children =>
        // There should not be any other UDFs, or the children can't be evaluated directly.
        assert(children.forall(_.find(_.isInstanceOf[PythonUDF]).isEmpty))
        (ChainedPythonFunctions(Seq(udf.func)), udf.children)
    }
  }

  protected override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecuteColumnar().")
  }

  protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)

  override def columnarInputRDDs: Seq[RDD[ColumnarBatch]] = {
    throw new UnsupportedOperationException(s"This operator doesn't support inputRDDs.")
  }

  override def getBuildPlans: Seq[(SparkPlan, SparkPlan)] = {
    throw new UnsupportedOperationException(s"This operator doesn't support getBuildPlans.")
  }

  override def getStreamedLeafPlan: SparkPlan = {
    throw new UnsupportedOperationException(s"This operator doesn't support getStreamedLeafPlan.")
  }

  override def getChild: SparkPlan = {
    throw new UnsupportedOperationException(s"This operator doesn't support getChild.")
  }

  override def doValidate(): Boolean = false

  override def doTransform(args: Object): TransformContext = {
    throw new UnsupportedOperationException(s"This operator doesn't support doTransform.")
  }

  override def doTransform(args: java.lang.Object,
                           index: java.lang.Integer,
                           paths: java.util.ArrayList[String],
                           starts: java.util.ArrayList[java.lang.Long],
                           lengths: java.util.ArrayList[java.lang.Long]): TransformContext = {
    throw new UnsupportedOperationException(s"This operator doesn't support doTransform.")
  }
}
