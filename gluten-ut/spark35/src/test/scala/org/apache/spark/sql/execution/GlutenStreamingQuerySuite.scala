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
package org.apache.spark.sql.execution;

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{GlutenSQLTestsTrait, _}
import org.apache.spark.sql.delta.implicits.intEncoder
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.streaming.util.StreamManualClock
import org.apache.spark.sql.types.StructType
import org.scalactic.TolerantNumerics
import org.scalatest.BeforeAndAfter

class GlutenStreamingQuerySuite extends StreamTest with BeforeAndAfter with Logging with GlutenSQLTestsTrait {

  // To make === between double tolerate inexact values
  implicit val doubleEquality = TolerantNumerics.tolerantDoubleEquality(0.01)

  after {
    sqlContext.streams.active.foreach(_.stop())
  }

  test("input row calculation with same V1 source used twice in self-join") {
    val streamingTriggerDF = spark.createDataset(1 to 10).toDF
    val streamingInputDF = createSingleTriggerStreamingDF(streamingTriggerDF).toDF("value")
    val selfJoin = streamingInputDF.join(streamingInputDF, "value")
    val progress = getStreamingQuery(selfJoin).recentProgress.head
    assert(progress.numInputRows === 20)
  }


  test("input row calculation with same V1 source used twice in self-union") {
    val streamingTriggerDF = spark.createDataset(1 to 10).toDF
    val streamingInputDF = createSingleTriggerStreamingDF(streamingTriggerDF).toDF("value")
    val progress = getStreamingQuery(streamingInputDF.union(streamingInputDF)).recentProgress.head
    assert(progress.numInputRows === 20)
  }

  /** Create a streaming DF that only execute one batch in which it returns the given static DF */
  private def createSingleTriggerStreamingDF(triggerDF: DataFrame): DataFrame = {
    require(!triggerDF.isStreaming)
    // A streaming Source that generate only on trigger and returns the given Dataframe as batch
    val source = new Source() {
      override def schema: StructType = triggerDF.schema
      override def getOffset: Option[Offset] = Some(LongOffset(0))
      override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
        sqlContext.internalCreateDataFrame(
          triggerDF.queryExecution.toRdd, triggerDF.schema, isStreaming = true)
      }
      override def stop(): Unit = {}
    }
    StreamingExecutionRelation(source, spark)
  }

  /** Returns the query at the end of the first trigger of streaming DF */
  private def getStreamingQuery(streamingDF: DataFrame): StreamingQuery = {
    try {
      val q = streamingDF.writeStream.format("memory").queryName("test").start()
      q.processAllAvailable()
      q
    } finally {
      spark.streams.active.foreach(_.stop())
    }
  }

  /** Returns the last query progress from query.recentProgress where numInputRows is positive */
  def getLastProgressWithData(q: StreamingQuery): Option[StreamingQueryProgress] = {
    q.recentProgress.filter(_.numInputRows > 0).lastOption
  }


  object TestAwaitTermination {
  }
}

object StreamingQuerySuite {
  // Singleton reference to clock that does not get serialized in task closures
  var clock: StreamManualClock = null
}
