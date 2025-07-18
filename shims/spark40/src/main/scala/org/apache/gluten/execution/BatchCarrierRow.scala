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
package org.apache.gluten.execution

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.VariantVal

sealed abstract class BatchCarrierRow extends BatchCarrierRowBase {

  override def getVariant(ordinal: Int): VariantVal = {
    throw new UnsupportedOperationException(
      "Underlying columnar data is inaccessible from BatchCarrierRow")
  }
}

object BatchCarrierRow {
  def unwrap(row: InternalRow): Option[ColumnarBatch] = row match {
    case _: PlaceholderRow => None
    case t: TerminalRow => Some(t.batch())
    case _ =>
      throw new UnsupportedOperationException(
        s"Row $row is not a ${classOf[BatchCarrierRow].getSimpleName}")
  }
}

/**
 * A [[BatchCarrierRow]] implementation that is backed by a
 * [[org.apache.spark.sql.vectorized.ColumnarBatch]].
 *
 * Serialization code originated since https://github.com/apache/incubator-gluten/issues/9270.
 */
abstract class TerminalRow extends BatchCarrierRow {
  def batch(): ColumnarBatch
  def withNewBatch(batch: ColumnarBatch): TerminalRow
}

/**
 * A [[BatchCarrierRow]] implementation with no data. The only function of this row implementation
 * is to provide row metadata to the receiver and to support correct row-counting.
 */
class PlaceholderRow extends BatchCarrierRow {
  override def copy(): InternalRow = new PlaceholderRow()
}
