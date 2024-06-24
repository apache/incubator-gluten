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
package org.apache.gluten.backendsapi.velox

import org.apache.gluten.GlutenConfig
import org.apache.gluten.backendsapi.SparkPlanExecApi
import org.apache.gluten.datasource.ArrowConvertorRule
import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.execution._
import org.apache.gluten.expression._
import org.apache.gluten.expression.ExpressionNames.{TRANSFORM_KEYS, TRANSFORM_VALUES}
import org.apache.gluten.expression.aggregate.{HLLAdapter, VeloxBloomFilterAggregate, VeloxCollectList, VeloxCollectSet}
import org.apache.gluten.extension._
import org.apache.gluten.extension.columnar.TransformHints
import org.apache.gluten.extension.columnar.transition.Convention
import org.apache.gluten.extension.columnar.transition.ConventionFunc.BatchOverride
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.vectorized.{ColumnarBatchSerializer, ColumnarBatchSerializeResult}

import org.apache.spark.{ShuffleDependency, SparkException}
import org.apache.spark.api.python.{ColumnarArrowEvalPythonExec, PullOutArrowEvalPythonPreProjectHelper}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.{GenShuffleWriterParameters, GlutenShuffleWriterWrapper}
import org.apache.spark.shuffle.utils.ShuffleUtil
import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.optimizer.BuildSide
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.{BuildSideRelation, HashedRelationBroadcastMode}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.python.ArrowEvalPythonExec
import org.apache.spark.sql.execution.utils.ExecUtil
import org.apache.spark.sql.expression.{UDFExpression, UDFResolver, UserDefinedAggregateFunction}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.commons.lang3.ClassUtils

import javax.ws.rs.core.UriBuilder

import scala.collection.mutable.ListBuffer

class VeloxSparkPlanExecApi extends SparkPlanExecApi {

  /** The columnar-batch type this backend is using. */
  override def batchType: Convention.BatchType = {
    VeloxBatch
  }

  /**
   * Overrides [[org.apache.gluten.extension.columnar.transition.ConventionFunc]] Gluten is using to
   * determine the convention (its row-based processing / columnar-batch processing support) of a
   * plan with a user-defined function that accepts a plan then returns batch type it outputs.
   */
  override def batchTypeFunc(): BatchOverride = {
    case i: InMemoryTableScanExec
        if i.supportsColumnar && i.relation.cacheBuilder.serializer
          .isInstanceOf[ColumnarCachedBatchSerializer] =>
      VeloxBatch
  }

  /** Transform GetArrayItem to Substrait. */
  override def genGetArrayItemTransformer(
      substraitExprName: String,
      left: ExpressionTransformer,
      right: ExpressionTransformer,
      original: Expression): ExpressionTransformer = {
    GenericExpressionTransformer(substraitExprName, Seq(left, right), original)
  }

  /** Transform NaNvl to Substrait. */
  override def genNaNvlTransformer(
      substraitExprName: String,
      left: ExpressionTransformer,
      right: ExpressionTransformer,
      original: NaNvl): ExpressionTransformer = {
    val condExpr = IsNaN(original.left)
    val condFuncName = ExpressionMappings.expressionsMap(classOf[IsNaN])
    val newExpr = If(condExpr, original.right, original.left)
    IfTransformer(
      substraitExprName,
      GenericExpressionTransformer(condFuncName, Seq(left), condExpr),
      right,
      left,
      newExpr
    )
  }

  /** Transform Uuid to Substrait. */
  override def genUuidTransformer(
      substraitExprName: String,
      original: Uuid): ExpressionTransformer = {
    GenericExpressionTransformer(
      substraitExprName,
      Seq(LiteralTransformer(original.randomSeed.get)),
      original)
  }

  override def genTryArithmeticTransformer(
      substraitExprName: String,
      left: ExpressionTransformer,
      right: ExpressionTransformer,
      original: TryEval,
      checkArithmeticExprName: String): ExpressionTransformer = {
    if (SparkShimLoader.getSparkShims.withAnsiEvalMode(original.child)) {
      throw new GlutenNotSupportException(
        s"${original.child.prettyName} with ansi mode is not supported")
    }
    original.child.dataType match {
      case LongType | IntegerType | ShortType | ByteType =>
      case _ => throw new GlutenNotSupportException(s"$substraitExprName is not supported")
    }
    // Offload to velox for only IntegralTypes.
    GenericExpressionTransformer(
      substraitExprName,
      Seq(GenericExpressionTransformer(checkArithmeticExprName, Seq(left, right), original)),
      original)
  }

  /**
   * Map arithmetic expr to different functions: substraitExprName or try(checkArithmeticExprName)
   * based on EvalMode.
   */
  override def genArithmeticTransformer(
      substraitExprName: String,
      left: ExpressionTransformer,
      right: ExpressionTransformer,
      original: Expression,
      checkArithmeticExprName: String): ExpressionTransformer = {
    if (SparkShimLoader.getSparkShims.withTryEvalMode(original)) {
      original.dataType match {
        case LongType | IntegerType | ShortType | ByteType =>
        case _ =>
          throw new GlutenNotSupportException(s"$substraitExprName with try mode is not supported")
      }
      // Offload to velox for only IntegralTypes.
      GenericExpressionTransformer(
        ExpressionMappings.expressionsMap(classOf[TryEval]),
        Seq(GenericExpressionTransformer(checkArithmeticExprName, Seq(left, right), original)),
        original)
    } else if (SparkShimLoader.getSparkShims.withAnsiEvalMode(original)) {
      throw new GlutenNotSupportException(s"$substraitExprName with ansi mode is not supported")
    } else {
      GenericExpressionTransformer(substraitExprName, Seq(left, right), original)
    }
  }

  /** Transform map_entries to Substrait. */
  override def genMapEntriesTransformer(
      substraitExprName: String,
      child: ExpressionTransformer,
      expr: Expression): ExpressionTransformer = {
    GenericExpressionTransformer(substraitExprName, Seq(child), expr)
  }

  /** Transform array filter to Substrait. */
  override def genArrayFilterTransformer(
      substraitExprName: String,
      argument: ExpressionTransformer,
      function: ExpressionTransformer,
      expr: ArrayFilter): ExpressionTransformer = {
    expr.function match {
      case LambdaFunction(_, arguments, _) if arguments.size == 2 =>
        throw new GlutenNotSupportException(
          "filter on array with lambda using index argument is not supported yet")
      case _ => GenericExpressionTransformer(substraitExprName, Seq(argument, function), expr)
    }
  }

  /** Transform array forall to Substrait. */
  override def genArrayForAllTransformer(
      substraitExprName: String,
      argument: ExpressionTransformer,
      function: ExpressionTransformer,
      expr: ArrayForAll): ExpressionTransformer = {
    expr.function match {
      case LambdaFunction(_, arguments, _) if arguments.size == 2 =>
        throw new GlutenNotSupportException(
          "forall on array with lambda using index argument is not supported yet")
      case _ => GenericExpressionTransformer(substraitExprName, Seq(argument, function), expr)
    }
  }

  /** Transform array exists to Substrait */
  override def genArrayExistsTransformer(
      substraitExprName: String,
      argument: ExpressionTransformer,
      function: ExpressionTransformer,
      expr: ArrayExists): ExpressionTransformer = {
    expr.function match {
      case LambdaFunction(_, arguments, _) if arguments.size == 2 =>
        throw new GlutenNotSupportException(
          "exists on array with lambda using index argument is not supported yet")
      case _ => GenericExpressionTransformer(substraitExprName, Seq(argument, function), expr)
    }
  }

  /** Transform array transform to Substrait. */
  override def genArrayTransformTransformer(
      substraitExprName: String,
      argument: ExpressionTransformer,
      function: ExpressionTransformer,
      expr: ArrayTransform): ExpressionTransformer = {
    expr.function match {
      case LambdaFunction(_, arguments, _) if arguments.size == 2 =>
        throw new GlutenNotSupportException(
          "transform on array with lambda using index argument is not supported yet")
      case _ => GenericExpressionTransformer(substraitExprName, Seq(argument, function), expr)
    }
  }

  /** Transform posexplode to Substrait. */
  override def genPosExplodeTransformer(
      substraitExprName: String,
      child: ExpressionTransformer,
      original: PosExplode,
      attrSeq: Seq[Attribute]): ExpressionTransformer = {
    GenericExpressionTransformer(substraitExprName, Seq(child), attrSeq.head)
  }

  /** Transform inline to Substrait. */
  override def genInlineTransformer(
      substraitExprName: String,
      child: ExpressionTransformer,
      expr: Expression): ExpressionTransformer = {
    GenericExpressionTransformer(substraitExprName, Seq(child), expr)
  }

  override def genLikeTransformer(
      substraitExprName: String,
      left: ExpressionTransformer,
      right: ExpressionTransformer,
      original: Like): ExpressionTransformer = {
    GenericExpressionTransformer(
      substraitExprName,
      Seq(left, right, LiteralTransformer(original.escapeChar)),
      original)
  }

  /** Transform make_timestamp to Substrait. */
  override def genMakeTimestampTransformer(
      substraitExprName: String,
      children: Seq[ExpressionTransformer],
      expr: Expression): ExpressionTransformer = {
    GenericExpressionTransformer(substraitExprName, children, expr)
  }

  override def genDateDiffTransformer(
      substraitExprName: String,
      endDate: ExpressionTransformer,
      startDate: ExpressionTransformer,
      original: DateDiff): ExpressionTransformer = {
    GenericExpressionTransformer(substraitExprName, Seq(endDate, startDate), original)
  }

  override def genPreciseTimestampConversionTransformer(
      substraitExprName: String,
      children: Seq[ExpressionTransformer],
      expr: PreciseTimestampConversion): ExpressionTransformer = {
    // Expression used internally to convert the TimestampType to Long and back without losing
    // precision, i.e. in microseconds.
    val (newSubstraitName, newExpr) = expr match {
      case _ @PreciseTimestampConversion(_, TimestampType, LongType) =>
        (ExpressionMappings.expressionsMap(classOf[UnixMicros]), UnixMicros(expr.child))
      case _ @PreciseTimestampConversion(_, LongType, TimestampType) =>
        (
          ExpressionMappings.expressionsMap(classOf[MicrosToTimestamp]),
          MicrosToTimestamp(expr.child))
      case _ =>
        // TimestampNTZType is not supported here.
        throw new GlutenNotSupportException("PreciseTimestampConversion is not supported")
    }
    GenericExpressionTransformer(newSubstraitName, children, newExpr)
  }

  /**
   * Generate FilterExecTransformer.
   *
   * @param condition
   *   : the filter condition
   * @param child
   *   : the child of FilterExec
   * @return
   *   the transformer of FilterExec
   */
  override def genFilterExecTransformer(
      condition: Expression,
      child: SparkPlan): FilterExecTransformerBase = {
    FilterExecTransformer(condition, child)
  }

  /** Generate HashAggregateExecTransformer. */
  override def genHashAggregateExecTransformer(
      requiredChildDistributionExpressions: Option[Seq[Expression]],
      groupingExpressions: Seq[NamedExpression],
      aggregateExpressions: Seq[AggregateExpression],
      aggregateAttributes: Seq[Attribute],
      initialInputBufferOffset: Int,
      resultExpressions: Seq[NamedExpression],
      child: SparkPlan): HashAggregateExecBaseTransformer =
    RegularHashAggregateExecTransformer(
      requiredChildDistributionExpressions,
      groupingExpressions,
      aggregateExpressions,
      aggregateAttributes,
      initialInputBufferOffset,
      resultExpressions,
      child)

  /** Generate HashAggregateExecPullOutHelper */
  override def genHashAggregateExecPullOutHelper(
      aggregateExpressions: Seq[AggregateExpression],
      aggregateAttributes: Seq[Attribute]): HashAggregateExecPullOutBaseHelper =
    HashAggregateExecPullOutHelper(aggregateExpressions, aggregateAttributes)

  override def genColumnarShuffleExchange(shuffle: ShuffleExchangeExec): SparkPlan = {
    def allowHashOnMap[T](f: => T): T = {
      val originalAllowHash = SQLConf.get.getConf(SQLConf.LEGACY_ALLOW_HASH_ON_MAPTYPE)
      try {
        SQLConf.get.setConf(SQLConf.LEGACY_ALLOW_HASH_ON_MAPTYPE, true)
        f
      } finally {
        SQLConf.get.setConf(SQLConf.LEGACY_ALLOW_HASH_ON_MAPTYPE, originalAllowHash)
      }
    }

    def maybeAddAppendBatchesExec(plan: SparkPlan): SparkPlan = {
      if (GlutenConfig.getConf.veloxCoalesceBatchesBeforeShuffle) {
        VeloxAppendBatchesExec(plan, GlutenConfig.getConf.veloxMinBatchSizeForShuffle)
      } else {
        plan
      }
    }

    val child = shuffle.child

    shuffle.outputPartitioning match {
      case HashPartitioning(exprs, _) =>
        val hashExpr = new Murmur3Hash(exprs)
        val projectList = Seq(Alias(hashExpr, "hash_partition_key")()) ++ child.output
        val projectTransformer = ProjectExecTransformer(projectList, child)
        val validationResult = projectTransformer.doValidate()
        if (validationResult.isValid) {
          val newChild = maybeAddAppendBatchesExec(projectTransformer)
          ColumnarShuffleExchangeExec(shuffle, newChild, newChild.output.drop(1))
        } else {
          TransformHints.tagNotTransformable(shuffle, validationResult)
          shuffle.withNewChildren(child :: Nil)
        }
      case RoundRobinPartitioning(num) if SQLConf.get.sortBeforeRepartition && num > 1 =>
        // scalastyle:off line.size.limit
        // Temporarily allow hash on map if it's disabled, otherwise HashExpression will fail to get
        // resolved if its child contains map type.
        // See https://github.com/apache/spark/blob/609bd4839e5d504917de74ed1cb9c23645fba51f/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/hash.scala#L279-L283
        // scalastyle:on line.size.limit
        allowHashOnMap {
          // Velox hash expression does not support null type and we also do not need to sort
          // null type since the value always be null.
          val columnsForHash = child.output.filterNot(_.dataType == NullType)
          if (columnsForHash.isEmpty) {
            val newChild = maybeAddAppendBatchesExec(child)
            ColumnarShuffleExchangeExec(shuffle, newChild, newChild.output)
          } else {
            val hashExpr = new Murmur3Hash(columnsForHash)
            val projectList = Seq(Alias(hashExpr, "hash_partition_key")()) ++ child.output
            val projectTransformer = ProjectExecTransformer(projectList, child)
            val projectBeforeSortValidationResult = projectTransformer.doValidate()
            // Make sure we support offload hash expression
            val projectBeforeSort = if (projectBeforeSortValidationResult.isValid) {
              projectTransformer
            } else {
              val project = ProjectExec(projectList, child)
              TransformHints.tagNotTransformable(project, projectBeforeSortValidationResult)
              project
            }
            val sortOrder = SortOrder(projectBeforeSort.output.head, Ascending)
            val sortByHashCode =
              SortExecTransformer(Seq(sortOrder), global = false, projectBeforeSort)
            val dropSortColumnTransformer =
              ProjectExecTransformer(projectList.drop(1), sortByHashCode)
            val validationResult = dropSortColumnTransformer.doValidate()
            if (validationResult.isValid) {
              val newChild = maybeAddAppendBatchesExec(dropSortColumnTransformer)
              ColumnarShuffleExchangeExec(shuffle, newChild, newChild.output)
            } else {
              TransformHints.tagNotTransformable(shuffle, validationResult)
              shuffle.withNewChildren(child :: Nil)
            }
          }
        }
      case _ =>
        val newChild = maybeAddAppendBatchesExec(child)
        ColumnarShuffleExchangeExec(shuffle, newChild, null)
    }
  }

  /** Generate ShuffledHashJoinExecTransformer. */
  override def genShuffledHashJoinExecTransformer(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType,
      buildSide: BuildSide,
      condition: Option[Expression],
      left: SparkPlan,
      right: SparkPlan,
      isSkewJoin: Boolean): ShuffledHashJoinExecTransformerBase =
    ShuffledHashJoinExecTransformer(
      leftKeys,
      rightKeys,
      joinType,
      buildSide,
      condition,
      left,
      right,
      isSkewJoin)

  /** Generate BroadcastHashJoinExecTransformer. */
  override def genBroadcastHashJoinExecTransformer(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType,
      buildSide: BuildSide,
      condition: Option[Expression],
      left: SparkPlan,
      right: SparkPlan,
      isNullAwareAntiJoin: Boolean = false): BroadcastHashJoinExecTransformerBase =
    BroadcastHashJoinExecTransformer(
      leftKeys,
      rightKeys,
      joinType,
      buildSide,
      condition,
      left,
      right,
      isNullAwareAntiJoin)

  override def genSampleExecTransformer(
      lowerBound: Double,
      upperBound: Double,
      withReplacement: Boolean,
      seed: Long,
      child: SparkPlan): SampleExecTransformer = {
    SampleExecTransformer(lowerBound, upperBound, withReplacement, seed, child)
  }

  override def genSortMergeJoinExecTransformer(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType,
      condition: Option[Expression],
      left: SparkPlan,
      right: SparkPlan,
      isSkewJoin: Boolean = false,
      projectList: Seq[NamedExpression] = null): SortMergeJoinExecTransformerBase = {
    SortMergeJoinExecTransformer(
      leftKeys,
      rightKeys,
      joinType,
      condition,
      left,
      right,
      isSkewJoin,
      projectList
    )
  }
  override def genCartesianProductExecTransformer(
      left: SparkPlan,
      right: SparkPlan,
      condition: Option[Expression]): CartesianProductExecTransformer = {
    CartesianProductExecTransformer(
      ColumnarCartesianProductBridge(left),
      ColumnarCartesianProductBridge(right),
      condition
    )
  }

  override def genBroadcastNestedLoopJoinExecTransformer(
      left: SparkPlan,
      right: SparkPlan,
      buildSide: BuildSide,
      joinType: JoinType,
      condition: Option[Expression]): BroadcastNestedLoopJoinExecTransformer =
    VeloxBroadcastNestedLoopJoinExecTransformer(
      left,
      right,
      buildSide,
      joinType,
      condition
    )

  override def genHashExpressionTransformer(
      substraitExprName: String,
      exprs: Seq[ExpressionTransformer],
      original: HashExpression[_]): ExpressionTransformer = {
    VeloxHashExpressionTransformer(substraitExprName, exprs, original)
  }

  /**
   * Generate ShuffleDependency for ColumnarShuffleExchangeExec.
   *
   * @return
   */
  // scalastyle:off argcount
  override def genShuffleDependency(
      rdd: RDD[ColumnarBatch],
      childOutputAttributes: Seq[Attribute],
      projectOutputAttributes: Seq[Attribute],
      newPartitioning: Partitioning,
      serializer: Serializer,
      writeMetrics: Map[String, SQLMetric],
      metrics: Map[String, SQLMetric],
      isSort: Boolean): ShuffleDependency[Int, ColumnarBatch, ColumnarBatch] = {
    // scalastyle:on argcount
    ExecUtil.genShuffleDependency(
      rdd,
      childOutputAttributes,
      newPartitioning,
      serializer,
      writeMetrics,
      metrics,
      isSort)
  }
  // scalastyle:on argcount

  /**
   * Generate ColumnarShuffleWriter for ColumnarShuffleManager.
   *
   * @return
   */
  override def genColumnarShuffleWriter[K, V](
      parameters: GenShuffleWriterParameters[K, V]): GlutenShuffleWriterWrapper[K, V] = {
    ShuffleUtil.genColumnarShuffleWriter(parameters)
  }

  override def createColumnarWriteFilesExec(
      child: SparkPlan,
      fileFormat: FileFormat,
      partitionColumns: Seq[Attribute],
      bucketSpec: Option[BucketSpec],
      options: Map[String, String],
      staticPartitions: TablePartitionSpec): SparkPlan = {
    VeloxColumnarWriteFilesExec(
      child,
      fileFormat,
      partitionColumns,
      bucketSpec,
      options,
      staticPartitions)
  }

  override def createColumnarArrowEvalPythonExec(
      udfs: Seq[PythonUDF],
      resultAttrs: Seq[Attribute],
      child: SparkPlan,
      evalType: Int): SparkPlan = {
    ColumnarArrowEvalPythonExec(udfs, resultAttrs, child, evalType)
  }

  /**
   * Generate ColumnarBatchSerializer for ColumnarShuffleExchangeExec.
   *
   * @return
   */
  override def createColumnarBatchSerializer(
      schema: StructType,
      metrics: Map[String, SQLMetric],
      isSort: Boolean): Serializer = {
    val numOutputRows = metrics("numOutputRows")
    val deserializeTime = metrics("deserializeTime")
    val readBatchNumRows = metrics("avgReadBatchNumRows")
    val decompressTime: Option[SQLMetric] = if (!isSort) {
      Some(metrics("decompressTime"))
    } else {
      None
    }
    if (GlutenConfig.getConf.isUseCelebornShuffleManager) {
      val clazz = ClassUtils.getClass("org.apache.spark.shuffle.CelebornColumnarBatchSerializer")
      val constructor =
        clazz.getConstructor(classOf[StructType], classOf[SQLMetric], classOf[SQLMetric])
      constructor.newInstance(schema, readBatchNumRows, numOutputRows).asInstanceOf[Serializer]
    } else {
      new ColumnarBatchSerializer(
        schema,
        readBatchNumRows,
        numOutputRows,
        deserializeTime,
        decompressTime,
        isSort)
    }
  }

  /** Create broadcast relation for BroadcastExchangeExec */
  override def createBroadcastRelation(
      mode: BroadcastMode,
      child: SparkPlan,
      numOutputRows: SQLMetric,
      dataSize: SQLMetric): BuildSideRelation = {
    val serialized: Array[ColumnarBatchSerializeResult] = child
      .executeColumnar()
      .mapPartitions(itr => Iterator(BroadcastUtils.serializeStream(itr)))
      .filter(_.getNumRows != 0)
      .collect
    val rawSize = serialized.map(_.getSerialized.length).sum
    if (rawSize >= BroadcastExchangeExec.MAX_BROADCAST_TABLE_BYTES) {
      throw new SparkException(
        s"Cannot broadcast the table that is larger than 8GB: ${rawSize >> 30} GB")
    }
    numOutputRows += serialized.map(_.getNumRows).sum
    dataSize += rawSize
    ColumnarBuildSideRelation(child.output, serialized.map(_.getSerialized))
  }

  override def doCanonicalizeForBroadcastMode(mode: BroadcastMode): BroadcastMode = {
    mode match {
      case hash: HashedRelationBroadcastMode =>
        // Node: It's different with vanilla Spark.
        // Vanilla Spark build HashRelation at driver side, so it is build keys sensitive.
        // But we broadcast byte array and build HashRelation at executor side,
        // the build keys are actually meaningless for the broadcast value.
        // This change allows us reuse broadcast exchange for different build keys with same table.
        hash.copy(key = Seq.empty)
      case _ => mode.canonicalized
    }
  }

  /**
   * * Expressions.
   */

  /** Generate StringSplit transformer. */
  override def genStringSplitTransformer(
      substraitExprName: String,
      srcExpr: ExpressionTransformer,
      regexExpr: ExpressionTransformer,
      limitExpr: ExpressionTransformer,
      original: StringSplit): ExpressionTransformer = {
    // In velox, split function just support tow args, not support limit arg for now
    VeloxStringSplitTransformer(substraitExprName, srcExpr, regexExpr, limitExpr, original)
  }

  /**
   * Generate Alias transformer.
   *
   * @return
   *   a transformer for alias
   */
  override def genAliasTransformer(
      substraitExprName: String,
      child: ExpressionTransformer,
      original: Expression): ExpressionTransformer =
    VeloxAliasTransformer(substraitExprName, child, original)

  /** Generate an expression transformer to transform GetMapValue to Substrait. */
  override def genGetMapValueTransformer(
      substraitExprName: String,
      left: ExpressionTransformer,
      right: ExpressionTransformer,
      original: GetMapValue): ExpressionTransformer = {
    GenericExpressionTransformer(
      ExpressionMappings.expressionsMap(classOf[ElementAt]),
      Seq(left, right),
      original)
  }

  override def genStringToMapTransformer(
      substraitExprName: String,
      children: Seq[ExpressionTransformer],
      expr: Expression): ExpressionTransformer = {
    if (
      SQLConf.get.getConf(SQLConf.MAP_KEY_DEDUP_POLICY)
        != SQLConf.MapKeyDedupPolicy.EXCEPTION.toString
    ) {
      throw new GlutenNotSupportException("Only EXCEPTION policy is supported!")
    }
    GenericExpressionTransformer(substraitExprName, children, expr)
  }

  /** Generate an expression transformer to transform NamedStruct to Substrait. */
  override def genNamedStructTransformer(
      substraitExprName: String,
      children: Seq[ExpressionTransformer],
      original: CreateNamedStruct,
      attributeSeq: Seq[Attribute]): ExpressionTransformer = {
    VeloxNamedStructTransformer(substraitExprName, original, attributeSeq)
  }

  /** Generate an ExpressionTransformer to transform GetStructFiled expression. */
  override def genGetStructFieldTransformer(
      substraitExprName: String,
      childTransformer: ExpressionTransformer,
      ordinal: Int,
      original: GetStructField): ExpressionTransformer = {
    VeloxGetStructFieldTransformer(substraitExprName, childTransformer, original)
  }

  /**
   * To align with spark in casting string type input to other types, add trim node for trimming
   * space or whitespace. See spark's Cast.scala.
   */
  override def genCastWithNewChild(c: Cast): Cast = {
    // scalastyle:off nonascii
    // Common whitespace to be trimmed, including: ' ', '\n', '\r', '\f', etc.
    val trimWhitespaceStr = " \t\n\u000B\u000C\u000D\u001C\u001D\u001E\u001F"
    // Space separator.
    val trimSpaceSepStr = "\u1680\u2008\u2009\u200A\u205F\u3000" +
      ('\u2000' to '\u2006').toList.mkString
    // Line separator.
    val trimLineSepStr = "\u2028"
    // Paragraph separator.
    val trimParaSepStr = "\u2029"
    // Needs to be trimmed for casting to float/double/decimal
    val trimSpaceStr = ('\u0000' to '\u0020').toList.mkString
    // scalastyle:on nonascii
    c.dataType match {
      case BinaryType | _: ArrayType | _: MapType | _: StructType | _: UserDefinedType[_] =>
        c
      case FloatType | DoubleType | _: DecimalType =>
        c.child.dataType match {
          case StringType =>
            val trimNode = StringTrim(c.child, Some(Literal(trimSpaceStr)))
            c.withNewChildren(Seq(trimNode)).asInstanceOf[Cast]
          case _ =>
            c
        }
      case _ =>
        c.child.dataType match {
          case StringType =>
            val trimNode = StringTrim(
              c.child,
              Some(
                Literal(trimWhitespaceStr +
                  trimSpaceSepStr + trimLineSepStr + trimParaSepStr)))
            c.withNewChildren(Seq(trimNode)).asInstanceOf[Cast]
          case _ =>
            c
        }
    }
  }

  /**
   * * Rules and strategies.
   */

  /**
   * Generate extended DataSourceV2 Strategy.
   *
   * @return
   */
  override def genExtendedDataSourceV2Strategies(): List[SparkSession => Strategy] = List()

  /**
   * Generate extended query stage preparation rules.
   *
   * @return
   */
  override def genExtendedQueryStagePrepRules(): List[SparkSession => Rule[SparkPlan]] = List()

  /**
   * Generate extended Analyzer.
   *
   * @return
   */
  override def genExtendedAnalyzers(): List[SparkSession => Rule[LogicalPlan]] = List()

  /**
   * Generate extended Optimizer. Currently only for Velox backend.
   *
   * @return
   */
  override def genExtendedOptimizers(): List[SparkSession => Rule[LogicalPlan]] = List(
    CollectRewriteRule.apply,
    HLLRewriteRule.apply
  )

  /**
   * Generate extended columnar pre-rules, in the validation phase.
   *
   * @return
   */
  override def genExtendedColumnarValidationRules(): List[SparkSession => Rule[SparkPlan]] = {
    val buf: ListBuffer[SparkSession => Rule[SparkPlan]] =
      ListBuffer(BloomFilterMightContainJointRewriteRule.apply, ArrowScanReplaceRule.apply)
    if (GlutenConfig.getConf.enableInputFileNameReplaceRule) {
      buf += InputFileNameReplaceRule.apply
    }
    buf.result
  }

  /**
   * Generate extended columnar pre-rules.
   *
   * @return
   */
  override def genExtendedColumnarTransformRules(): List[SparkSession => Rule[SparkPlan]] = {
    val buf: ListBuffer[SparkSession => Rule[SparkPlan]] = ListBuffer()
    if (GlutenConfig.getConf.enableVeloxFlushablePartialAggregation) {
      buf += FlushableHashAggregateRule.apply
    }
    buf.result
  }

  /**
   * Generate extended columnar post-rules.
   *
   * @return
   */
  override def genExtendedColumnarPostRules(): List[SparkSession => Rule[SparkPlan]] = {
    SparkShimLoader.getSparkShims.getExtendedColumnarPostRules() ::: List()
  }

  override def genInjectPostHocResolutionRules(): List[SparkSession => Rule[LogicalPlan]] = {
    List(ArrowConvertorRule)
  }

  /**
   * Generate extended Strategy.
   *
   * @return
   */
  override def genExtendedStrategies(): List[SparkSession => Strategy] = {
    List()
  }

  /** Define backend specfic expression mappings. */
  override def extraExpressionMappings: Seq[Sig] = {
    Seq(
      Sig[HLLAdapter](ExpressionNames.APPROX_DISTINCT),
      Sig[UDFExpression](ExpressionNames.UDF_PLACEHOLDER),
      Sig[UserDefinedAggregateFunction](ExpressionNames.UDAF_PLACEHOLDER),
      Sig[NaNvl](ExpressionNames.NANVL),
      Sig[VeloxCollectList](ExpressionNames.COLLECT_LIST),
      Sig[VeloxCollectSet](ExpressionNames.COLLECT_SET),
      Sig[VeloxBloomFilterMightContain](ExpressionNames.MIGHT_CONTAIN),
      Sig[VeloxBloomFilterAggregate](ExpressionNames.BLOOM_FILTER_AGG),
      Sig[TransformKeys](TRANSFORM_KEYS),
      Sig[TransformValues](TRANSFORM_VALUES),
      // For test purpose.
      Sig[VeloxDummyExpression](VeloxDummyExpression.VELOX_DUMMY_EXPRESSION)
    )
  }

  override def genInjectedFunctions()
      : Seq[(FunctionIdentifier, ExpressionInfo, FunctionBuilder)] = {
    UDFResolver.getFunctionSignatures
  }

  override def rewriteSpillPath(path: String): String = {
    val fs = GlutenConfig.getConf.veloxSpillFileSystem
    fs match {
      case "local" =>
        path
      case "heap-over-local" =>
        val rewritten = UriBuilder
          .fromPath(path)
          .scheme("jol")
          .toString
        rewritten
      case other =>
        throw new IllegalStateException(s"Unsupported fs: $other")
    }
  }

  override def genGenerateTransformer(
      generator: Generator,
      requiredChildOutput: Seq[Attribute],
      outer: Boolean,
      generatorOutput: Seq[Attribute],
      child: SparkPlan
  ): GenerateExecTransformerBase = {
    GenerateExecTransformer(generator, requiredChildOutput, outer, generatorOutput, child)
  }

  override def genPreProjectForGenerate(generate: GenerateExec): SparkPlan = {
    PullOutGenerateProjectHelper.pullOutPreProject(generate)
  }

  override def genPostProjectForGenerate(generate: GenerateExec): SparkPlan = {
    PullOutGenerateProjectHelper.pullOutPostProject(generate)
  }

  override def genPreProjectForArrowEvalPythonExec(
      arrowEvalPythonExec: ArrowEvalPythonExec): SparkPlan = {
    PullOutArrowEvalPythonPreProjectHelper.pullOutPreProject(arrowEvalPythonExec)
  }

  override def maybeCollapseTakeOrderedAndProject(plan: SparkPlan): SparkPlan = {
    // This to-top-n optimization assumes exchange operators were already placed in input plan.
    plan.transformUp {
      case p @ LimitTransformer(SortExecTransformer(sortOrder, _, child, _), 0, count) =>
        val global = child.outputPartitioning.satisfies(AllTuples)
        val topN = TopNTransformer(count, sortOrder, global, child)
        if (topN.doValidate().isValid) {
          topN
        } else {
          p
        }
      case other => other
    }
  }
}
