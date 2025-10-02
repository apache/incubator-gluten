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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.delta.{GlutenOptimisticTransaction, OptimisticTransaction, TransactionExecutionObserver}

case class GlutenDeltaV2CreateTableAsSelectExec(delegate: V2CreateTableAsSelectBaseExec)
  extends LeafV2CommandExec {
  import GlutenDeltaV2CreateTableAsSelectExec._

  override protected def run(): Seq[InternalRow] = {
    TransactionExecutionObserver.withObserver(UseColumnarDeltaTransactionLog) {
      delegate.executeCollect()
    }
  }

  override def output: Seq[Attribute] = {
    delegate.output
  }
}

object GlutenDeltaV2CreateTableAsSelectExec {
  private object UseColumnarDeltaTransactionLog extends TransactionExecutionObserver {
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
