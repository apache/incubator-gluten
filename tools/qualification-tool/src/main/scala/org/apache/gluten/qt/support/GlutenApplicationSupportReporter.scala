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
package org.apache.gluten.qt.support

import org.apache.gluten.qt.{writer, AppStatusStore, QualificationToolConfiguration}
import org.apache.gluten.qt.execution.{Executor, RunState}
import org.apache.gluten.qt.graph.{MetricInternal, SparkPlanGraphClusterInternal, SparkPlanGraphEdgeInternal, SparkPlanGraphInternal, SparkPlanGraphNodeInternal, SqlPlanMetricInternal}
import org.apache.gluten.qt.writer.{ApplicationReport, ImpactReport}

import org.apache.spark.sql.execution.ui.{SparkPlanGraph, SparkPlanGraphCluster, SparkPlanGraphNode, SQLAppStatusStore}

import ResultVisitor.UnsupportedImpact

import scala.collection.JavaConverters._

/**
 * Generates a detailed support report for a Spark application by analyzing its SQL execution plans.
 * This includes identifying unsupported operators, calculating supported and unsupported SQL time,
 * and providing an overall application report. <p> Major functionalities include: <ul>
 * <li>Retrieving and analyzing execution plans for SQL queries via {@link SQLAppStatusStore} .</li>
 * <li>Computing task durations for SQL and non-SQL operations.</li> <li>Determining supported and
 * unsupported SQL time using {@link GlutenExecutionSupportReporter} .</li> <li>Aggregating operator
 * impact reports for unsupported features.</li> <li>Compiling a summary {@link ApplicationReport} ,
 * including task time breakdown and SQL support percentages.</li> </ul> <p>
 *
 * @param conf
 *   the configuration object providing project and path details
 * @param sqlStore
 *   the SQL status store for accessing execution details
 * @param appStore
 *   the application status store for task and application metadata
 */
case class GlutenApplicationSupportReporter(
    conf: QualificationToolConfiguration,
    sqlStore: SQLAppStatusStore,
    appStore: AppStatusStore) {
  private lazy val sqlJobIds = sqlStore
    .executionsList()
    .map(_.executionId)
    .flatMap(sqlStore.execution)
    .flatMap(_.jobs)
    .map(_._1)
    .toSet
  private lazy val (sqlJobToDurationMap, jobToDurationMap) =
    appStore.getJobIdToTaskDurationMap.partition(x => sqlJobIds.contains(x._1))
  private lazy val sqlTaskTime = sqlJobToDurationMap.map(_._2).sum
  private lazy val otherTaskTime = jobToDurationMap.map(_._2).sum
  private lazy val totalTaskTime = sqlTaskTime + otherTaskTime
  private lazy val applicationId = appStore.getApplicationId
  private lazy val applicationName = appStore.getApplicationName
  private lazy val batchUuid = appStore.getBatchUuid

  private lazy val executionIds = sqlStore.executionsList().map(_.executionId)
  Executor.updateState(RunState("PROCESSING", executionIds.size))
  private lazy val executionResult = executionIds
    .map {
      executionId =>
        Executor.incrementState()
        val graph = sqlStore.planGraph(executionId)
        val metrics = sqlStore.executionMetrics(executionId)
        val graphDescription = new ExecutionDescription(
          GlutenApplicationSupportReporter.toSparkPlanGraphInternal(graph),
          GlutenApplicationSupportReporter.toMetricsInternal(metrics)
        )
        val executionSupport = new GlutenExecutionSupportReporter(graphDescription)
        SupportReport(
          executionId,
          executionSupport.getSupportedSqlTime,
          executionSupport.getTotalSqlTime,
          executionSupport.getOperatorToImplement.asScala.toMap
        )
    }
  private lazy val operatorToImplement = executionResult
    .flatMap(e => e.operatorsToImplement)
    .groupBy(_._1)
    .mapValues(_.map(_._2).reduceOption(_.add(_)).getOrElse(new UnsupportedImpact()))
    .toSeq
    .map(r => writer.ImpactReport(r._1, r._2))
  private lazy val supportedSqlTime = executionResult.map(e => e.supportedSqlTime.toMillis).sum
  private lazy val totalSqlTime = executionResult.map(e => e.totalSqlTime.toMillis).sum

  def getOperatorsToImplement: Seq[ImpactReport] = operatorToImplement

  def getReport: ApplicationReport = {
    val rddPercentage = if (totalTaskTime == 0) 0.0 else otherTaskTime.toDouble / totalTaskTime
    val unsupportedSqlPercentage =
      if (totalSqlTime == 0) 0.0
      else ((totalSqlTime - supportedSqlTime).toDouble / totalSqlTime) * (1 - rddPercentage)
    val supportedSqlPercentage =
      if (totalSqlTime == 0) 0.0
      else (supportedSqlTime.toDouble / totalSqlTime) * (1 - rddPercentage)
    ApplicationReport(
      applicationId,
      applicationName,
      batchUuid,
      totalTaskTime,
      rddPercentage,
      unsupportedSqlPercentage,
      supportedSqlPercentage
    )
  }
}

object GlutenApplicationSupportReporter {
  def toSparkPlanGraphInternal(graph: SparkPlanGraph): SparkPlanGraphInternal = {
    new SparkPlanGraphInternal(
      toSparkPlanGraphNodesInternal(graph.nodes),
      graph.edges.map(e => new SparkPlanGraphEdgeInternal(e.fromId, e.toId)).asJava,
      toSparkPlanGraphNodesInternal(graph.allNodes)
    )
  }

  def toSparkPlanGraphNodesInternal(
      nodes: Seq[SparkPlanGraphNode]): java.util.List[SparkPlanGraphNodeInternal] = {
    nodes.map {
      node =>
        val metric = node.metrics.map(toSqlPlanMetricInternal).asJava
        node match {
          case cluster: SparkPlanGraphCluster =>
            new SparkPlanGraphClusterInternal(
              cluster.id,
              cluster.name,
              cluster.desc,
              toSparkPlanGraphNodesInternal(cluster.nodes),
              metric
            )
          case node =>
            new SparkPlanGraphNodeInternal(node.id, node.name, node.desc, metric)
        }
    }.asJava
  }

  def toSqlPlanMetricInternal(
      metric: org.apache.spark.sql.execution.ui.SQLPlanMetric): SqlPlanMetricInternal = {
    new SqlPlanMetricInternal(metric.name, metric.accumulatorId, metric.metricType)
  }

  def toMetricsInternal(metrics: Map[Long, String]): java.util.List[MetricInternal] = {
    metrics.map(m => new MetricInternal(m._1, m._2)).toList.asJava
  }
}
