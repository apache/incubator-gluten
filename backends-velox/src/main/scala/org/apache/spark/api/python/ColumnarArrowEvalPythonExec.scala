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
package org.apache.spark.api.python

import org.apache.gluten.backendsapi.arrow.ArrowBatchTypes.ArrowJavaBatchType
import org.apache.gluten.columnarbatch.ColumnarBatches
import org.apache.gluten.execution.ValidatablePlan
import org.apache.gluten.extension.ValidationResult
import org.apache.gluten.extension.columnar.transition.{Convention, ConventionReq}
import org.apache.gluten.iterator.Iterators
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators
import org.apache.gluten.utils.PullOutProjectHelper
import org.apache.gluten.vectorized.ArrowWritableColumnVector

import org.apache.spark.{ContextAwareIterator, SparkEnv, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.python.{ArrowEvalPythonExec, BasePythonRunnerShim, EvalPythonExec, PythonUDFRunner}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.utils.{SparkArrowUtil, SparkSchemaUtil, SparkVectorUtil}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.util.Utils

import org.apache.arrow.vector.{VectorLoader, VectorSchemaRoot}
import org.apache.arrow.vector.ipc.{ArrowStreamReader, ArrowStreamWriter}

import java.io.{DataInputStream, DataOutputStream}
import java.net.Socket
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class ColumnarArrowPythonRunner(
    funcs: Seq[ChainedPythonFunctions],
    evalType: Int,
    argOffsets: Array[Array[Int]],
    schema: StructType,
    timeZoneId: String,
    conf: Map[String, String])
  extends BasePythonRunnerShim(funcs.toSeq, evalType, argOffsets) {

  override val simplifiedTraceback: Boolean = SQLConf.get.pysparkSimplifiedTraceback

  override val bufferSize: Int = SQLConf.get.pandasUDFBufferSize
  require(
    bufferSize >= 4,
    "Pandas execution requires more than 4 bytes. Please set higher buffer. " +
      s"Please change '${SQLConf.PANDAS_UDF_BUFFER_SIZE.key}'.")

  protected def newReaderIterator(
      stream: DataInputStream,
      writerThread: WriterThread,
      startTime: Long,
      env: SparkEnv,
      worker: Socket,
      pid: scala.Option[scala.Int],
      releasedOrClosed: AtomicBoolean,
      context: TaskContext): Iterator[ColumnarBatch] = {

    new ReaderIterator(
      stream,
      writerThread,
      startTime,
      env,
      worker,
      None,
      releasedOrClosed,
      context) {
      private val allocator = ArrowBufferAllocators.contextInstance()

      private var reader: ArrowStreamReader = _
      private var root: VectorSchemaRoot = _
      private var schema: StructType = _
      private var vectors: Array[ColumnVector] = _

      context.addTaskCompletionListener[Unit] {
        _ =>
          if (reader != null) {
            reader.close(false)
          }
          if (root != null) {
            root.close()
          }
      }

      private var batchLoaded = true

      override protected def read(): ColumnarBatch = {
        if (writerThread.exception.isDefined) {
          throw writerThread.exception.get
        }
        try {
          if (reader != null && batchLoaded) {
            batchLoaded = reader.loadNextBatch()
            if (batchLoaded) {
              val batch = new ColumnarBatch(vectors)
              batch.setNumRows(root.getRowCount)
              batch
            } else {
              reader.close(false)
              // Reach end of stream. Call `read()` again to read control data.
              read()
            }
          } else {
            stream.readInt() match {
              case SpecialLengths.START_ARROW_STREAM =>
                reader = new ArrowStreamReader(stream, allocator)
                root = reader.getVectorSchemaRoot()
                schema = SparkArrowUtil.fromArrowSchema(root.getSchema())
                vectors = ArrowWritableColumnVector
                  .loadColumns(root.getRowCount, root.getFieldVectors)
                  .toArray[ColumnVector]
                read()
              case SpecialLengths.TIMING_DATA =>
                handleTimingData()
                read()
              case SpecialLengths.PYTHON_EXCEPTION_THROWN =>
                throw handlePythonException()
              case SpecialLengths.END_OF_DATA_SECTION =>
                handleEndOfDataSection()
                null
            }
          }
        } catch handleException
      }
    }
  }

  override protected def newWriterThread(
      env: SparkEnv,
      worker: Socket,
      inputIterator: Iterator[ColumnarBatch],
      partitionIndex: Int,
      context: TaskContext): WriterThread = {
    new WriterThread(env, worker, inputIterator, partitionIndex, context) {
      override protected def writeCommand(dataOut: DataOutputStream): Unit = {
        // Write config for the worker as a number of key -> value pairs of strings
        dataOut.writeInt(conf.size)
        for ((k, v) <- conf) {
          PythonRDD.writeUTF(k, dataOut)
          PythonRDD.writeUTF(v, dataOut)
        }
        PythonUDFRunner.writeUDFs(dataOut, funcs, argOffsets)
      }

      override protected def writeIteratorToStream(dataOut: DataOutputStream): Unit = {
        var numRows: Long = 0
        val arrowSchema = SparkSchemaUtil.toArrowSchema(schema, timeZoneId)
        val allocator = ArrowBufferAllocators.contextInstance()
        val root = VectorSchemaRoot.create(arrowSchema, allocator)

        Utils.tryWithSafeFinally {
          val loader = new VectorLoader(root)
          val writer = new ArrowStreamWriter(root, null, dataOut)
          writer.start()
          while (inputIterator.hasNext) {
            val nextBatch = inputIterator.next()
            numRows += nextBatch.numRows

            val cols = (0 until nextBatch.numCols).toList.map(
              i =>
                nextBatch
                  .asInstanceOf[ColumnarBatch]
                  .column(i)
                  .asInstanceOf[ArrowWritableColumnVector]
                  .getValueVector)
            val nextRecordBatch =
              SparkVectorUtil.toArrowRecordBatch(nextBatch.numRows, cols)
            loader.load(nextRecordBatch)
            writer.writeBatch()
            if (nextRecordBatch != null) {
              nextRecordBatch.close()
            }
          }
          // end writes footer to the output stream and doesn't clean any resources.
          // It could throw exception if the output stream is closed, so it should be
          // in the try block.
          writer.end()
        } {
          root.close()
          // allocator can't close now or the data will loss
          // allocator.close()
        }
      }
    }
  }
}

case class ColumnarArrowEvalPythonExec(
    udfs: Seq[PythonUDF],
    resultAttrs: Seq[Attribute],
    child: SparkPlan,
    evalType: Int)
  extends EvalPythonExec
  with ValidatablePlan {

  override def batchType(): Convention.BatchType = ArrowJavaBatchType

  override def rowType0(): Convention.RowType = Convention.RowType.None

  override protected def doValidateInternal(): ValidationResult = {
    val (_, inputs) = udfs.map(collectFunctions).unzip
    inputs.foreach {
      input =>
        input.foreach {
          case e: AttributeReference if child.output.exists(_.exprId == e.exprId) =>
          // Valid case, continue validation
          case _: AttributeReference =>
            return ValidationResult.failed("Expression Id does not exist for AttributeReference")
          case _ =>
            return ValidationResult.failed("UDF input is not an instance of AttributeReference")
        }
    }
    super.doValidateInternal()
  }

  override def requiredChildConvention(): Seq[ConventionReq] = List(
    ConventionReq.ofBatch(ConventionReq.BatchType.Is(ArrowJavaBatchType)))

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "output_batches"),
    "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
    "processTime" -> SQLMetrics.createTimingMetric(sparkContext, "totaltime_arrow_udf")
  )

  override protected def evaluate(
      funcs: Seq[ChainedPythonFunctions],
      argOffsets: Array[Array[Int]],
      iter: Iterator[InternalRow],
      schema: StructType,
      context: TaskContext): Iterator[InternalRow] = {
    throw new IllegalStateException(
      "ColumnarArrowEvalPythonExec doesn't support evaluate InternalRow.")
  }

  private val sessionLocalTimeZone = conf.sessionLocalTimeZone

  private def getPythonRunnerConfMap(conf: SQLConf): Map[String, String] = {
    val timeZoneConf = Seq(SQLConf.SESSION_LOCAL_TIMEZONE.key -> conf.sessionLocalTimeZone)
    val pandasColsByName = Seq(
      SQLConf.PANDAS_GROUPED_MAP_ASSIGN_COLUMNS_BY_NAME.key ->
        conf.pandasGroupedMapAssignColumnsByName.toString)
    val arrowSafeTypeCheck = Seq(
      SQLConf.PANDAS_ARROW_SAFE_TYPE_CONVERSION.key ->
        conf.arrowSafeTypeConversion.toString)
    Map(timeZoneConf.toSeq ++ pandasColsByName.toSeq ++ arrowSafeTypeCheck: _*)
  }

  private val pythonRunnerConf = getPythonRunnerConfMap(conf)

  protected def evaluateColumnar(
      funcs: Seq[ChainedPythonFunctions],
      argOffsets: Array[Array[Int]],
      iter: Iterator[ColumnarBatch],
      schema: StructType,
      context: TaskContext): Iterator[ColumnarBatch] = {

    val outputTypes = output.drop(child.output.length).map(_.dataType)

    val columnarBatchIter = new ColumnarArrowPythonRunner(
      funcs,
      evalType,
      argOffsets,
      schema,
      sessionLocalTimeZone,
      pythonRunnerConf).compute(iter, context.partitionId(), context)

    columnarBatchIter.map {
      batch =>
        val actualDataTypes = (0 until batch.numCols()).map(i => batch.column(i).dataType())
        assert(
          outputTypes == actualDataTypes,
          "Invalid schema from arrow_udf: " +
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
        (ChainedPythonFunctions(Seq(udf.func).toSeq), udf.children)
    }
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric("numOutputRows")
    val numOutputBatches = longMetric("numOutputBatches")
    val numInputRows = longMetric("numInputRows")
    val procTime = longMetric("processTime")
    val inputRDD = child.executeColumnar()
    inputRDD.mapPartitions {
      iter =>
        val context = TaskContext.get()
        val (pyFuncs, inputs) = udfs.map(collectFunctions).unzip
        // We only write the referred cols by UDFs to python worker. So we need
        // get corresponding offsets
        val allInputs = new ArrayBuffer[Expression]
        val dataTypes = new ArrayBuffer[DataType]
        val originalOffsets = new ArrayBuffer[Int]
        val argOffsets = inputs.map {
          input =>
            input.map {
              e =>
                if (allInputs.exists(_.semanticEquals(e))) {
                  allInputs.indexWhere(_.semanticEquals(e))
                } else {
                  val offset = child.output.indexWhere(
                    _.exprId.equals(e.asInstanceOf[AttributeReference].exprId))
                  originalOffsets += offset
                  allInputs += e
                  dataTypes += e.dataType
                  allInputs.length - 1
                }
            }.toArray
        }.toArray
        val schema = StructType(dataTypes.zipWithIndex.map {
          case (dt, i) =>
            StructField(s"_$i", dt)
        }.toSeq)

        val contextAwareIterator = new ContextAwareIterator(context, iter)
        val inputCbCache = new ArrayBuffer[ColumnarBatch]()
        var start_time: Long = 0
        val inputBatchIter = contextAwareIterator.map {
          inputCb =>
            start_time = System.nanoTime()
            ColumnarBatches.checkLoaded(inputCb)
            ColumnarBatches.retain(inputCb)
            // 0. cache input for later merge
            inputCbCache += inputCb
            numInputRows += inputCb.numRows
            // We only need to pass the referred cols data to python worker for evaluation.
            var colsForEval = new ArrayBuffer[ColumnVector]()
            for (i <- originalOffsets) {
              colsForEval += inputCb.column(i)
            }
            new ColumnarBatch(colsForEval.toArray, inputCb.numRows())
        }

        val outputColumnarBatchIterator =
          evaluateColumnar(pyFuncs, argOffsets, inputBatchIter, schema, context)
        val res =
          outputColumnarBatchIterator.zipWithIndex.map {
            case (outputCb, batchId) =>
              val inputCb = inputCbCache(batchId)
              val joinedVectors = (0 until inputCb.numCols).toArray.map(
                i => inputCb.column(i)) ++ (0 until outputCb.numCols).toArray.map(
                i => outputCb.column(i))
              // Columns in outputCb has random 0 or 1 refCnt and will fail checks in ensureOffload,
              // so we do a hard reset here.
              (0 until joinedVectors.length).foreach(
                i => {
                  adjustRefCnt(joinedVectors(i).asInstanceOf[ArrowWritableColumnVector], 1)
                })
              val numRows = inputCb.numRows
              numOutputBatches += 1
              numOutputRows += numRows
              val batch = new ColumnarBatch(joinedVectors, numRows)
              ColumnarBatches.checkLoaded(batch)
              procTime += (System.nanoTime() - start_time) / 1000000
              batch
          }
        Iterators
          .wrap(res)
          .recycleIterator {
            inputCbCache.foreach(ColumnarBatches.release(_))
          }
          .recyclePayload(_.close())
          .create()
    }
  }

  private def adjustRefCnt(vector: ArrowWritableColumnVector, to: Long): Unit = {
    val from = vector.refCnt()
    if (from == to) {
      return
    }
    if (from > to) {
      do {
        vector.close()
      } while (vector.refCnt() != to)
      return
    }
    // from < to
    do {
      vector.retain()
    } while (vector.refCnt() != to)
  }

  override protected def withNewChildInternal(newChild: SparkPlan): ColumnarArrowEvalPythonExec =
    copy(udfs, resultAttrs, newChild)
}

object PullOutArrowEvalPythonPreProjectHelper extends PullOutProjectHelper {
  private def collectFunctions(udf: PythonUDF): (ChainedPythonFunctions, Seq[Expression]) = {
    udf.children match {
      case Seq(u: PythonUDF) =>
        val (chained, children) = collectFunctions(u)
        (ChainedPythonFunctions(chained.funcs ++ Seq(udf.func)), children)
      case children =>
        (ChainedPythonFunctions(Seq(udf.func).toSeq), udf.children)
    }
  }

  private def rewriteUDF(
      udf: PythonUDF,
      expressionMap: mutable.HashMap[Expression, NamedExpression]): PythonUDF = {
    udf.children match {
      case Seq(u: PythonUDF) =>
        udf
          .withNewChildren(udf.children.toIndexedSeq.map {
            func => rewriteUDF(func.asInstanceOf[PythonUDF], expressionMap)
          })
          .asInstanceOf[PythonUDF]
      case children =>
        val newUDFChildren = udf.children.map {
          case literal: Literal => literal
          case other => replaceExpressionWithAttribute(other, expressionMap)
        }
        udf.withNewChildren(newUDFChildren).asInstanceOf[PythonUDF]
    }
  }

  def pullOutPreProject(arrowEvalPythonExec: ArrowEvalPythonExec): SparkPlan = {
    // pull out preproject
    val (_, inputs) = arrowEvalPythonExec.udfs.map(collectFunctions).unzip
    val expressionMap = new mutable.HashMap[Expression, NamedExpression]()
    // flatten all the arguments
    val allInputs = new ArrayBuffer[Expression]
    for (input <- inputs) {
      input.map {
        e =>
          if (!allInputs.exists(_.semanticEquals(e))) {
            allInputs += e
            replaceExpressionWithAttribute(e, expressionMap)
          }
      }
    }
    if (!expressionMap.isEmpty) {
      // Need preproject.
      val preProject = ProjectExec(
        eliminateProjectList(arrowEvalPythonExec.child.outputSet, expressionMap.values.toSeq),
        arrowEvalPythonExec.child)
      val newUDFs = arrowEvalPythonExec.udfs.map(f => rewriteUDF(f, expressionMap))
      arrowEvalPythonExec.copy(udfs = newUDFs, child = preProject)
    } else {
      arrowEvalPythonExec
    }
  }
}
