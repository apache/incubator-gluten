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
package io.glutenproject.plan

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.exchange.BroadcastExchangeLike
import org.apache.spark.sql.gluten.errors.GlutenError
import org.apache.spark.sql.vectorized.ColumnarBatch

import java.util.concurrent.Future

/**
 * A [[GlutenBroadcastExchangeExec]] collects, transforms and finally broadcasts the result of a
 * transformed [[GlutenPlan]] SparkPlan in columnar format.
 */
case class GlutenBroadcastExchangeExec(mode: BroadcastMode, child: GlutenPlan)
  extends BroadcastExchangeLike
  with GlutenPlan {

  protected def withNewChildInternal(newChild: SparkPlan): GlutenBroadcastExchangeExec = {
    require(newChild.isInstanceOf[GlutenPlan])
    copy(child = newChild.asInstanceOf[GlutenPlan])
  }

  override def relationFuture: Future[Broadcast[Any]] =
    throw new UnsupportedOperationException()

  override protected def completionFuture: scala.concurrent.Future[Broadcast[Any]] =
    throw new UnsupportedOperationException()
  override def runtimeStatistics: Statistics = throw new UnsupportedOperationException()

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] =
    throw GlutenError.executeCodePathUnsupportedError(nodeName)
}
