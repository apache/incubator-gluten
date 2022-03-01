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

package com.intel.oap.execution

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import com.google.common.collect.Lists
import com.intel.oap.GazelleJniConfig
import com.intel.oap.expression._
import com.intel.oap.substrait.extensions.{MappingBuilder, MappingNode}
import com.intel.oap.substrait.plan.{PlanBuilder, PlanNode}
import com.intel.oap.substrait.rel.{ExtensionTableBuilder, LocalFilesBuilder, RelNode}
import com.intel.oap.substrait.SubstraitContext
import com.intel.oap.vectorized._
import org.apache.arrow.gandiva.expression._
import org.apache.arrow.vector.types.pojo.{ArrowType, Field}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.v2.arrow.SparkMemoryUtils
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.util.{ExecutorManager, UserAddedJarUtils}

case class TransformContext(inputAttributes: Seq[Attribute],
                            outputAttributes: Seq[Attribute], root: RelNode)

case class WholestageTransformContext(inputAttributes: Seq[Attribute],
                                      outputAttributes: Seq[Attribute], root: PlanNode,
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
  val numaBindingInfo = GazelleJniConfig.getConf.numaBindingInfo
  val enableColumnarSortMergeJoinLazyRead: Boolean =
    GazelleJniConfig.getConf.enableColumnarSortMergeJoinLazyRead

  val clickhouseMergeTreeTablePath = GazelleJniConfig.getConf.clickhouseMergeTreeTablePath
  val clickhouseMergeTreeEnabled = GazelleJniConfig.getConf.clickhouseMergeTreeEnabled
  val clickhouseMergeTreeDatabase = GazelleJniConfig.getConf.clickhouseMergeTreeDatabase
  val clickhouseMergeTreeTable = GazelleJniConfig.getConf.clickhouseMergeTreeTable

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "totalTime" -> SQLMetrics.createTimingMetric(sparkContext, "totaltime_wholestagetransform"),
    "buildTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to build dependencies"),
    "pipelineTime" -> SQLMetrics.createTimingMetric(sparkContext, "duration"))

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def supportsColumnar: Boolean = GazelleJniConfig.getConf.enableColumnarIterator

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
        val tempDir = GazelleJniConfig.getRandomTempDir
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
    val childCtx = child.asInstanceOf[TransformSupport]
      .doTransform(substraitContext)
    if (childCtx == null) {
      throw new NullPointerException(
        s"ColumnarWholestageTransformer can't doTansform on ${child}")
    }
    val mappingNodes = new java.util.ArrayList[MappingNode]()
    val mapIter = substraitContext.registeredFunction.entrySet().iterator()
    while(mapIter.hasNext) {
      val entry = mapIter.next()
      val mappingNode = MappingBuilder.makeFunctionMapping(entry.getKey, entry.getValue)
      mappingNodes.add(mappingNode)
    }
    val relNodes = Lists.newArrayList(childCtx.root)
    val planNode = PlanBuilder.makePlan(mappingNodes, relNodes)

    WholestageTransformContext(childCtx.inputAttributes,
      childCtx.outputAttributes, planNode, substraitContext)
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
        if (curChild.isInstanceOf[ConditionProjectExecTransformer]) {
          // see if this condition projector did filter, if so, we need to skip metrics
          val condProj = curChild.asInstanceOf[ConditionProjectExecTransformer]
          if (condProj.condition != null &&
              (condProj.projectList != null && condProj.projectList.nonEmpty)) {
            idx -= 1
          }
        }
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
        .nullable(s"${attr.name.toUpperCase()}#${attr.exprId.id}",
        CodeGeneration.getResultType(attr.dataType))
    })

    val keyFieldList: List[Field] = keyAttributes.toList.map(attr => {
      val field = Field
        .nullable(s"${attr.name.toUpperCase()}#${attr.exprId.id}",
          CodeGeneration.getResultType(attr.dataType))
      if (outputFieldList.indexOf(field) == -1) {
        throw new UnsupportedOperationException(s"CachedRelation not found" +
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

  def prepareLazyReadFunction(): TreeNode = {
    val lazyReadFuncName = "LazyRead"
    val lazy_read_func = TreeBuilder.makeFunction(
      lazyReadFuncName,
      Lists.newArrayList(),
      new ArrowType.Int(32, true) /*dummy ret type, won't be used*/ )
    TreeBuilder.makeFunction(
      "standalone",
      Lists.newArrayList(lazy_read_func),
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
        case FilePartition(index, files) =>
          val substraitPlan = if (clickhouseMergeTreeEnabled) {
            val extensionTableNode = ExtensionTableBuilder.makeExtensionTable(index,
              clickhouseMergeTreeDatabase, clickhouseMergeTreeTable, clickhouseMergeTreeTablePath)
            wsCxt.substraitContext.setExtensionTableNode(extensionTableNode)
            wsCxt.root.toProtobuf
          } else {
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
            wsCxt.root.toProtobuf
          }
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

    val numOutputBatches = child.longMetric("numOutputBatches")
    val pipelineTime = longMetric("pipelineTime")

    var build_elapse: Long = 0
    var eval_elapse: Long = 0
    // we should zip all dependent RDDs to current main RDD
    // TODO: Does it still need these parameters?
    val buildPlans = getBuildPlans
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
      val execTempDir = GazelleJniConfig.getTempFile
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
        s"Generated substrait plan tooks: ${(System.nanoTime() - startTime) / 1000000} ms")

      val wsRDD = new NativeWholeStageColumnarRDD(sparkContext, substraitPlanPartition, true,
        wsCxt.inputAttributes, wsCxt.outputAttributes, jarList, dependentKernelIterators)
      wsRDD.map{ r =>
        numOutputBatches += 1
        r
      }
    } else {
      val inputRDDs = columnarInputRDDs
      var curRDD = inputRDDs.head
      val resCtx = doWholestageTransform()
      val inputAttributes = resCtx.inputAttributes
      val outputAttributes = resCtx.outputAttributes
      val rootNode = resCtx.root

      curRDD.mapPartitions { iter =>
        ExecutorManager.tryTaskSet(numaBindingInfo)
        GazelleJniConfig.getConf
        val execTempDir = GazelleJniConfig.getTempFile
        val jarList = listJars.map(jarUrl => {
          logWarning(s"Get Codegened library Jar ${jarUrl}")
          UserAddedJarUtils.fetchJarFromSpark(
            jarUrl,
            execTempDir,
            s"spark-columnar-plugin-codegen-precompile-${signature}.jar",
            sparkConf)
          s"${execTempDir}/spark-columnar-plugin-codegen-precompile-${signature}.jar"
        })
        // FIXME: pass iter to native with Substrait
        val lazyReadFunction = prepareLazyReadFunction()
        val lazyReadExpr =
          TreeBuilder.makeExpression(
            lazyReadFunction,
            Field.nullable("result", new ArrowType.Int(32, true)))
        val transKernel = new ExpressionEvaluator(jarList.toList.asJava)
        val inBatchIter = new ColumnarNativeIterator(iter.asJava)
        val inBatchIters = new java.util.ArrayList[ColumnarNativeIterator]()
        inBatchIters.add(inBatchIter);
        // we need to complete dependency RDD's firstly
        val beforeBuild = System.nanoTime()
        val inputSchema = ConverterUtils.toArrowSchema(inputAttributes)
        val outputSchema = ConverterUtils.toArrowSchema(outputAttributes)
        val nativeIterator = transKernel.createKernelWithBatchIterator(rootNode, inBatchIters)
        build_elapse += System.nanoTime() - beforeBuild
        val resultStructType = ArrowUtils.fromArrowSchema(outputSchema)
        val resIter = streamedSortPlan match {
          case t: TransformSupport =>
            new Iterator[ColumnarBatch] {
              override def hasNext: Boolean = {
                val res = nativeIterator.hasNext
                // if (res == false) updateMetrics(nativeIterator)
                res
              }

              override def next(): ColumnarBatch = {
                val beforeEval = System.nanoTime()
                val output_rb = nativeIterator.next
                if (output_rb == null) {
                  eval_elapse += System.nanoTime() - beforeEval
                  val resultColumnVectors =
                    ArrowWritableColumnVector.allocateColumns(0, resultStructType).toArray
                  return new ColumnarBatch(resultColumnVectors.map(_.asInstanceOf[ColumnVector]), 0)
                }
                val outputNumRows = output_rb.getLength
                val outSchema = ConverterUtils.toArrowSchema(resCtx.outputAttributes)
                val output = ConverterUtils.fromArrowRecordBatch(outSchema, output_rb)
                ConverterUtils.releaseArrowRecordBatch(output_rb)
                eval_elapse += System.nanoTime() - beforeEval
                new ColumnarBatch(output.map(v => v.asInstanceOf[ColumnVector]), outputNumRows)
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
          // nativeKernel.close
          nativeIterator.close
          relationHolder.clear()
        }
        SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit](_ => {
          close
        })
        new CloseableColumnBatchIterator(resIter)
      }
    }
  }
}
