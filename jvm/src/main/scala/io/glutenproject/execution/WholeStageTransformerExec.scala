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

import java.util
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import com.google.common.collect.Lists
import io.glutenproject.GlutenConfig
import io.glutenproject.expression._
import io.glutenproject.substrait.SubstraitContext
import io.glutenproject.substrait.plan.{PlanBuilder, PlanNode}
import io.glutenproject.substrait.rel.{ExtensionTableBuilder, LocalFilesBuilder, RelNode}
import io.glutenproject.vectorized._
import org.apache.arrow.gandiva.expression._
import org.apache.arrow.vector.types.pojo.{ArrowType, Field}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.execution.datasources.v2.arrow.SparkMemoryUtils
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.util.{ExecutorManager, UserAddedJarUtils}

case class TransformContext(
    inputAttributes: Seq[Attribute],
    outputAttributes: Seq[Attribute],
    root: RelNode)

case class WholestageTransformContext(
    inputAttributes: Seq[Attribute],
    outputAttributes: Seq[Attribute],
    root: PlanNode,
    substraitContext: SubstraitContext = null)

trait TransformSupport extends SparkPlan {

  /**
   * Validate whether this SparkPlan supports to be transformed into substrait node in Native Code.
   */
  def doValidate(): Boolean = false

  /**
   * Returns all the RDDs of ColumnarBatch which generates the input rows.
   *
   * @note Right now we support up to two RDDs
   */
  def columnarInputRDDs: Seq[RDD[ColumnarBatch]]

  def getBuildPlans: Seq[(SparkPlan, SparkPlan)]

  def getStreamedLeafPlan: SparkPlan

  def getChild: SparkPlan

  def doTransform(context: SubstraitContext): TransformContext = {
    throw new UnsupportedOperationException(
      s"This operator doesn't support doTransform with SubstraitContext.")
  }

  def dependentPlanCtx: TransformContext = null

  def updateMetrics(out_num_rows: Long, process_time: Long): Unit = {}
}

case class WholeStageTransformerExec(child: SparkPlan)(val transformStageId: Int)
    extends UnaryExecNode
    with TransformSupport {

  val sparkConf = sparkContext.getConf
  val numaBindingInfo = GlutenConfig.getConf.numaBindingInfo
  val enableColumnarSortMergeJoinLazyRead: Boolean =
    GlutenConfig.getConf.enableColumnarSortMergeJoinLazyRead

  var fakeArrowOutput = false

  def setFakeOutput(): Unit = {
    fakeArrowOutput = true
  }

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "totalTime" -> SQLMetrics.createTimingMetric(sparkContext, "totaltime_wholestagetransform"),
    "buildTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to build dependencies"),
    "pipelineTime" -> SQLMetrics.createTimingMetric(sparkContext, "duration"))

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def supportsColumnar: Boolean = GlutenConfig.getConf.enableColumnarIterator

  override def otherCopyArgs: Seq[AnyRef] = Seq(transformStageId.asInstanceOf[Integer])
  override def generateTreeString(
      depth: Int,
      lastChildren: Seq[Boolean],
      append: String => Unit,
      verbose: Boolean,
      prefix: String = "",
      addSuffix: Boolean = false,
      maxFields: Int,
      printNodeId: Boolean,
      indent: Int = 0): Unit = {
    val res = child.generateTreeString(
      depth,
      lastChildren,
      append,
      verbose,
      if (printNodeId) "* " else s"*($transformStageId) ",
      false,
      maxFields,
      printNodeId,
      indent)
    res
  }

  override def nodeName: String = s"WholeStageCodegenTransformer (${transformStageId})"
  def uploadAndListJars(signature: String): Seq[String] =
    if (signature != "") {
      if (sparkContext.listJars.filter(path => path.contains(s"${signature}.jar")).isEmpty) {
        val tempDir = GlutenConfig.getRandomTempDir
        val jarFileName =
          s"${tempDir}/tmp/spark-columnar-plugin-codegen-precompile-${signature}.jar"
        sparkContext.addJar(jarFileName)
      }
      sparkContext.listJars.filter(path => path.contains(s"${signature}.jar"))
    } else {
      Seq()
    }

  def doWholestageTransform(): WholestageTransformContext = {
    val substraitContext = new SubstraitContext
    val childCtx = child
      .asInstanceOf[TransformSupport]
      .doTransform(substraitContext)
    if (childCtx == null) {
      throw new NullPointerException(
        s"ColumnarWholestageTransformer can't doTansform on ${child}")
    }
    val outNames = new java.util.ArrayList[String]()
    // Use the first item in output names to specify the output format of the WS computing.
    // When the next operator is ArrowColumnarToRow, fake Arrow output will be returned.
    // TODO: Use a more proper way to send some self-assigned parameters to native.
    if (GlutenConfig.getConf.isVeloxBackend) {
      if (fakeArrowOutput) {
        outNames.add("fake_arrow_output")
      } else {
        outNames.add("real_arrow_output")
      }
    }
    for (attr <- childCtx.outputAttributes) {
      outNames.add(attr.name)
    }
    val planNode =
      PlanBuilder.makePlan(substraitContext, Lists.newArrayList(childCtx.root), outNames)

    WholestageTransformContext(
      childCtx.inputAttributes,
      childCtx.outputAttributes,
      planNode,
      substraitContext)
  }

  override def getBuildPlans: Seq[(SparkPlan, SparkPlan)] = {
    child.asInstanceOf[TransformSupport].getBuildPlans
  }

  override def getStreamedLeafPlan: SparkPlan = {
    child.asInstanceOf[TransformSupport].getStreamedLeafPlan
  }

  override def getChild: SparkPlan = child

  override def updateMetrics(out_num_rows: Long, process_time: Long): Unit = {}

  var metricsUpdated: Boolean = false
  def updateMetrics(nativeIterator: BatchIterator): Unit = {
    if (metricsUpdated) return
    try {
      val metrics = nativeIterator.getMetrics
      var curChild = child
      var idx = metrics.output_length_list.length - 1
      var child_process_time: Long = 0
      while (idx >= 0 && curChild.isInstanceOf[TransformSupport]) {
        curChild
          .asInstanceOf[TransformSupport]
          .updateMetrics(
            metrics.output_length_list(idx),
            metrics.process_time_list(idx) - child_process_time)
        child_process_time = metrics.process_time_list(idx)
        idx -= 1
        curChild = curChild.asInstanceOf[TransformSupport].getChild
      }
      metricsUpdated = true
    } catch {
      case e: NullPointerException =>
        logWarning(s"updateMetrics failed due to NullPointerException!")
    }
  }

  def prepareRelationFunction(
      keyAttributes: Seq[Attribute],
      outputAttributes: Seq[Attribute]): TreeNode = {
    val outputFieldList: List[Field] = outputAttributes.toList.map(attr => {
      Field
        .nullable(
          s"${attr.name.toUpperCase()}#${attr.exprId.id}",
          CodeGeneration.getResultType(attr.dataType))
    })

    val keyFieldList: List[Field] = keyAttributes.toList.map(attr => {
      val field = Field
        .nullable(
          s"${attr.name.toUpperCase()}#${attr.exprId.id}",
          CodeGeneration.getResultType(attr.dataType))
      if (outputFieldList.indexOf(field) == -1) {
        throw new UnsupportedOperationException(
          s"CachedRelation not found" +
            s"${attr.name.toUpperCase()}#${attr.exprId.id} in ${outputAttributes}")
      }
      field
    })

    val key_args_node = TreeBuilder.makeFunction(
      "key_field",
      keyFieldList
        .map(field => {
          TreeBuilder.makeField(field)
        })
        .asJava,
      new ArrowType.Int(32, true) /*dummy ret type, won't be used*/ )

    val cachedRelationFuncName = "CachedRelation"
    val cached_relation_func = TreeBuilder.makeFunction(
      cachedRelationFuncName,
      Lists.newArrayList(key_args_node),
      new ArrowType.Int(32, true) /*dummy ret type, won't be used*/ )

    TreeBuilder.makeFunction(
      "standalone",
      Lists.newArrayList(cached_relation_func),
      new ArrowType.Int(32, true))
  }

  /**
   * Return built cpp library's signature
   */
  def doBuild(): String = {
    ""
  }

  override def columnarInputRDDs: Seq[RDD[ColumnarBatch]] = child match {
    case c: TransformSupport =>
      c.columnarInputRDDs
    case _ =>
      throw new UnsupportedOperationException
  }

  def checkBatchScanExecTransformerChild(): Option[BasicScanExecTransformer] = {
    var current_op = child
    while (current_op.isInstanceOf[TransformSupport] &&
           !current_op.isInstanceOf[BasicScanExecTransformer] &&
           current_op.asInstanceOf[TransformSupport].getChild != null) {
      current_op = current_op.asInstanceOf[TransformSupport].getChild
    }
    if (current_op != null &&
        current_op.isInstanceOf[BasicScanExecTransformer]) {
      current_op match {
        case op: BatchScanExecTransformer =>
          op.partitions
          Some(current_op.asInstanceOf[BatchScanExecTransformer])
        case op: FileSourceScanExecTransformer =>
          Some(current_op.asInstanceOf[FileSourceScanExecTransformer])
      }
    } else {
      None
    }
  }

  override def doExecute(): RDD[InternalRow] = {
    val numOutputBatches = child.longMetric("numOutputBatches")
    val pipelineTime = longMetric("pipelineTime")

    // check if BatchScan exists
    val current_op = checkBatchScanExecTransformerChild()
    if (current_op.isDefined) {
      // If containing scan exec transformer, a new RDD is created.
      val fileScan = current_op.get
      val wsCxt = doWholestageTransform()

      val startTime = System.nanoTime()
      val substraitPlanPartition = fileScan.getPartitions.map {
        case p: NativeMergeTreePartition =>
          val extensionTableNode =
            ExtensionTableBuilder.makeExtensionTable(
              p.minParts,
              p.maxParts,
              p.database,
              p.table,
              p.tablePath)
          wsCxt.substraitContext.setExtensionTableNode(extensionTableNode)
          // logWarning(s"The substrait plan for partition " +
          //   s"${p.index}:\n${wsCxt.root.toProtobuf.toString}")
          p.copySubstraitPlan(wsCxt.root.toProtobuf.toByteArray)
        case FilePartition(index, files) =>
          val paths = new java.util.ArrayList[String]()
          val starts = new java.util.ArrayList[java.lang.Long]()
          val lengths = new java.util.ArrayList[java.lang.Long]()
          files.foreach { f =>
            paths.add(f.filePath)
            starts.add(new java.lang.Long(f.start))
            lengths.add(new java.lang.Long(f.length))
          }
          val localFilesNode = LocalFilesBuilder.makeLocalFiles(index, paths, starts, lengths)
          wsCxt.substraitContext.setLocalFilesNode(localFilesNode)
          val substraitPlan = wsCxt.root.toProtobuf
          /*
          val out = new DataOutputStream(new FileOutputStream("/tmp/SubStraitTest-Q6.dat",
                      false));
          out.write(substraitPlan.toByteArray());
          out.flush();
           */
          // logWarning(s"The substrait plan for partition ${index}:\n${substraitPlan.toString}")
          NativeFilePartition(index, files, substraitPlan.toByteArray)
        case p => p
      }
      logWarning(
        s"Generated substrait plan tooks: ${(System.nanoTime() - startTime) / 1000000} ms")

      val wsRDD = new NativeWholestageRowRDD(sparkContext, substraitPlanPartition, false)
      wsRDD
    } else {
      sparkContext.emptyRDD
    }
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val signature = doBuild()
    val listJars = uploadAndListJars(signature)

    val numOutputBatches = child.metrics.get("numOutputBatches")
    val pipelineTime = longMetric("pipelineTime")

    var build_elapse: Long = 0
    var eval_elapse: Long = 0
    // we should zip all dependent RDDs to current main RDD
    // TODO: Does it still need these parameters?
    val streamedSortPlan = getStreamedLeafPlan
    val dependentKernels: ListBuffer[ExpressionEvaluator] = ListBuffer()
    val dependentKernelIterators: ListBuffer[BatchIterator] = ListBuffer()
    val buildRelationBatchHolder: ListBuffer[ColumnarBatch] = ListBuffer()
    val serializableObjectHolder: ListBuffer[SerializableObject] = ListBuffer()
    val relationHolder: ListBuffer[ColumnarHashedRelation] = ListBuffer()

    // check if BatchScan exists
    val current_op = checkBatchScanExecTransformerChild()
    if (current_op.isDefined) {
      // If containing scan exec transformer, a new RDD is created.
      // TODO: Remove ?
      val execTempDir = GlutenConfig.getTempFile
      val jarList = listJars.map(jarUrl => {
        logWarning(s"Get Codegened library Jar ${jarUrl}")
        UserAddedJarUtils.fetchJarFromSpark(
          jarUrl,
          execTempDir,
          s"spark-columnar-plugin-codegen-precompile-${signature}.jar",
          sparkConf)
        s"${execTempDir}/spark-columnar-plugin-codegen-precompile-${signature}.jar"
      })
      val fileScan = current_op.get
      val wsCxt = doWholestageTransform()

      val startTime = System.nanoTime()
      val substraitPlanPartition = fileScan.getPartitions.map {
        case p: NativeMergeTreePartition =>
          val extensionTableNode =
            ExtensionTableBuilder.makeExtensionTable(
              p.minParts,
              p.maxParts,
              p.database,
              p.table,
              p.tablePath)
          wsCxt.substraitContext.setExtensionTableNode(extensionTableNode)
//           logWarning(s"The substrait plan for partition " +
//             s"${p.index}:\n${wsCxt.root.toProtobuf.toString}")
          p.copySubstraitPlan(wsCxt.root.toProtobuf.toByteArray)
        case FilePartition(index, files) =>
          val paths = new java.util.ArrayList[String]()
          val starts = new java.util.ArrayList[java.lang.Long]()
          val lengths = new java.util.ArrayList[java.lang.Long]()
          files.foreach { f =>
            paths.add(f.filePath)
            starts.add(new java.lang.Long(f.start))
            lengths.add(new java.lang.Long(f.length))
          }

          val localFilesNode = LocalFilesBuilder.makeLocalFiles(index, paths, starts, lengths)
          wsCxt.substraitContext.setLocalFilesNode(localFilesNode)
          val substraitPlan = wsCxt.root.toProtobuf

          logInfo(s"The substrait plan for partition ${index}:\n${substraitPlan.toString}")
          NativeFilePartition(index, files, substraitPlan.toByteArray)
        case p => p
      }
      logWarning(
        s"Generating the Substrait plan took: ${(System.nanoTime() - startTime) / 1000000} ms.")

      val wsRDD = new NativeWholeStageColumnarRDD(
        sparkContext,
        substraitPlanPartition,
        true,
        wsCxt.outputAttributes,
        jarList,
        dependentKernelIterators)
      numOutputBatches match {
        case Some(batches) => wsRDD.foreach { _ => batches += 1 }
        case None =>
      }
      wsRDD
    } else {
      val inputRDDs = columnarInputRDDs
      val resCtx = doWholestageTransform()
      logInfo(s"Generated substrait plan:\n${resCtx.root.toProtobuf.toString}")

      if (!GlutenConfig.getConf.isClickHouseBackend) {
        val createNativeIterator = (inputIterators: Seq[Iterator[ColumnarBatch]]) => {
          ExecutorManager.tryTaskSet(numaBindingInfo)

          val execTempDir = GlutenConfig.getTempFile
          val fetchedJars = listJars.map(jarUrl => {
            logWarning(s"Get Codegened library Jar ${jarUrl}")
            UserAddedJarUtils.fetchJarFromSpark(
              jarUrl,
              execTempDir,
              s"spark-columnar-plugin-codegen-precompile-${signature}.jar",
              sparkConf)
            s"${execTempDir}/spark-columnar-plugin-codegen-precompile-${signature}.jar"
          })

          val beforeBuild = System.nanoTime()
          val transKernel = new ExpressionEvaluator(fetchedJars.asJava)
          val columnarNativeIterator =
            new util.ArrayList[ColumnarNativeIterator](inputIterators.map { iter =>
              new ColumnarNativeIterator(iter.asJava)
            }.asJava)
          val nativeResultIterator =
            transKernel.createKernelWithBatchIterator(resCtx.root, columnarNativeIterator)
          val buildElapse = System.nanoTime() - beforeBuild

          var evalElapse: Long = 0
          val schema = ArrowUtils.fromAttributes(resCtx.outputAttributes)
          val resIter = new Iterator[ColumnarBatch] {
            override def hasNext: Boolean = {
              val nativeHasNext = nativeResultIterator.hasNext
              if (!nativeHasNext) {
                // TODO: update metrics when reach to the end
              }
              nativeHasNext
            }

            override def next(): ColumnarBatch = {
              val beforeEval = System.nanoTime()
              val recordBatch = nativeResultIterator.next
              if (recordBatch == null) {
                evalElapse += System.nanoTime() - beforeEval
                val resultColumnVectors =
                  ArrowWritableColumnVector.allocateColumns(0, schema)
                return new ColumnarBatch(resultColumnVectors.map(_.asInstanceOf[ColumnVector]), 0)
              }
              val recordBatchSchema = ConverterUtils.toArrowSchema(resCtx.outputAttributes)
              val columns = ConverterUtils.fromArrowRecordBatch(recordBatchSchema, recordBatch)
              ConverterUtils.releaseArrowRecordBatch(recordBatch)
              evalElapse += System.nanoTime() - beforeEval
              new ColumnarBatch(
                columns.map(v => v.asInstanceOf[ColumnVector]),
                recordBatch.getLength)
            }
          }

          SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit](_ => {
            nativeResultIterator.close()
            pipelineTime += TimeUnit.NANOSECONDS.toMillis(evalElapse + buildElapse)
          })

          ColumnarFactory.createClosableIterator(resIter)
        }
        new WholeStageZippedPartitionsRDD(sparkContext, inputRDDs, createNativeIterator)

      } else {
        inputRDDs.head.mapPartitions { iter =>
          GlutenConfig.getConf
          val transKernel = new ExpressionEvaluator()
          val inBatchIter = new CHColumnarNativeIterator(iter.asJava)
          val inBatchIters = new java.util.ArrayList[ColumnarNativeIterator]()
          inBatchIters.add(inBatchIter)
          // we need to complete dependency RDD's firstly
          val beforeBuild = System.nanoTime()
          val nativeIterator =
            transKernel.createKernelWithBatchIterator(resCtx.root, inBatchIters)
          build_elapse += System.nanoTime() - beforeBuild
          val resIter = streamedSortPlan match {
            case t: TransformSupport =>
              new Iterator[ColumnarBatch] {
                override def hasNext: Boolean = {
                  val res = nativeIterator.hasNext
                  res
                }

                override def next(): ColumnarBatch = {
                  val beforeEval = System.nanoTime()
                  nativeIterator.chNext()
                }
              }
            case _ =>
              throw new UnsupportedOperationException(
                s"streamedSortPlan should support transformation")
          }
          var closed = false

          def close = {
            closed = true
            pipelineTime += (eval_elapse + build_elapse) / 1000000
            buildRelationBatchHolder.foreach(_.close) // fixing: ref cnt goes nagative
            dependentKernels.foreach(_.close)
            dependentKernelIterators.foreach(_.close)
            nativeIterator.close()
            relationHolder.clear()
          }

          SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit](_ => {
            close
          })
          ColumnarFactory.createClosableIterator(resIter)
        }
      }
    }
  }
}
