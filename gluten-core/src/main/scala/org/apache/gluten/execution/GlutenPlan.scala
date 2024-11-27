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

import org.apache.gluten.exception.GlutenException
import org.apache.gluten.extension.columnar.transition.{Convention, ConventionReq}

import org.apache.spark.sql.execution.SparkPlan

trait GlutenPlan
  extends SparkPlan
  with Convention.KnownBatchType
  with Convention.KnownRowTypeForSpark33AndLater
  with GlutenPlan.SupportsRowBasedCompatible
  with ConventionReq.KnownChildConvention {

  final override val supportsColumnar: Boolean = {
    batchType() != Convention.BatchType.None
  }

  override def batchType(): Convention.BatchType

  final override val supportsRowBased: Boolean = {
    rowType() != Convention.RowType.None
  }

  override def rowType0(): Convention.RowType

  override def requiredChildConvention(): Seq[ConventionReq] = {
    // In the normal case, children's convention should follow parent node's convention.
    val childReq = Convention.of(rowType(), batchType()).asReq()
    Seq.tabulate(children.size)(
      _ => {
        childReq
      })
  }
}

object GlutenPlan {
  // To be compatible with Spark (version < 3.3)
  trait SupportsRowBasedCompatible {
    def supportsRowBased(): Boolean = {
      throw new GlutenException("Illegal state: The method is not expected to be called")
    }
  }
}
