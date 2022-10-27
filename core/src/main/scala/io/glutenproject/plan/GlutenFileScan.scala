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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{FileSourceScanExec, LeafExecNode}
import org.apache.spark.sql.vectorized.ColumnarBatch

import io.substrait.relation.LocalFiles

case class GlutenFileScan(fileScan: FileSourceScanExec)
  extends LeafExecNode
  with GlutenPlan
  with SubstraitSupport[LocalFiles] {

  override def output: Seq[Attribute] = fileScan.output

  override def convert: LocalFiles = {
    Substrait.localFiles(output)(
      throw new UnsupportedOperationException(s"$nodeName.convert() fails")
    )
  }

  override def inputColumnarRDDs: Seq[RDD[ColumnarBatch]] = Nil
}
