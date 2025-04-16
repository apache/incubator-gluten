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
package org.apache.spark.sql.execution.datasources

import org.apache.gluten.backendsapi.BackendsApiManager

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.vectorized.ColumnarBatch

import java.io.{IOException, ObjectInputStream, ObjectOutputStream}

class FakeRowEnhancement(@transient private val _batch: ColumnarBatch)
  extends FakeRow(_batch)
  with Serializable {

  override def copy(): InternalRow = {
    val copied = BackendsApiManager.getSparkPlanExecApiInstance.copyColumnarBatch(batch)
    new FakeRowEnhancement(copied)
  }

  @throws(classOf[IOException])
  private def writeObject(output: ObjectOutputStream): Unit = {
    BackendsApiManager.getSparkPlanExecApiInstance.serializeColumnarBatch(output, batch)
  }

  @throws(classOf[IOException])
  private def readObject(input: ObjectInputStream): Unit = {
    batch = BackendsApiManager.getSparkPlanExecApiInstance.deserializeColumnarBatch(input)
  }
}
