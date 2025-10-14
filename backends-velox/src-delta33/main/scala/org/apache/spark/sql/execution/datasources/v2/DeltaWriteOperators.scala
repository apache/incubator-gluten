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
package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.delta.{GlutenOptimisticTransaction, OptimisticTransaction, TransactionExecutionObserver}
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.execution.metric.SQLMetric

case class GlutenDeltaLeafV2CommandExec(delegate: LeafV2CommandExec) extends LeafV2CommandExec {

  override def metrics: Map[String, SQLMetric] = delegate.metrics

  override protected def run(): Seq[InternalRow] = {
    TransactionExecutionObserver.withObserver(
      DeltaV2WriteOperators.UseColumnarDeltaTransactionLog) {
      delegate.executeCollect()
    }
  }

  override def output: Seq[Attribute] = {
    delegate.output
  }

  override def nodeName: String = "GlutenDelta " + delegate.nodeName
}

case class GlutenDeltaLeafRunnableCommand(delegate: LeafRunnableCommand)
  extends LeafRunnableCommand {
  override lazy val metrics: Map[String, SQLMetric] = delegate.metrics

  override def output: Seq[Attribute] = {
    delegate.output
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    TransactionExecutionObserver.withObserver(
      DeltaV2WriteOperators.UseColumnarDeltaTransactionLog) {
      delegate.run(sparkSession)
    }
  }

  override def nodeName: String = "GlutenDelta " + delegate.nodeName
}

object DeltaV2WriteOperators {
  object UseColumnarDeltaTransactionLog extends TransactionExecutionObserver {
    override def startingTransaction(f: => OptimisticTransaction): OptimisticTransaction = {
      val delegate = f
      new GlutenOptimisticTransaction(delegate)
    }

    override def preparingCommit[T](f: => T): T = f

    override def beginDoCommit(): Unit = ()

    override def beginBackfill(): Unit = ()

    override def beginPostCommit(): Unit = ()

    override def transactionCommitted(): Unit = ()

    override def transactionAborted(): Unit = ()

    override def createChild(): TransactionExecutionObserver = {
      TransactionExecutionObserver.getObserver
    }
  }
}
