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
package org.apache.gluten.extension.columnar.cost

import org.apache.gluten.exception.GlutenException

import org.apache.spark.sql.execution.SparkPlan

import scala.collection.mutable

/**
 * A [[LongCostModel]] implementation that consists of a set of sub-costers.
 *
 * The costers will apply in the same order they were registered or added.
 */
private class LongCosterChain private (costers: Seq[LongCoster]) extends LongCostModel {
  override def selfLongCostOf(node: SparkPlan): Long = {
    // Applies the costers respectively, returns when a coster gives a meaningful non-none number.
    // If all costers give none, throw an error.
    costers
      .foldLeft[Option[Long]](None) {
        case (None, coster) =>
          coster.selfCostOf(node)
        case (c @ Some(_), _) =>
          c
      }
      .getOrElse(throw new GlutenException(s"Cost not found for node: $node"))
  }
}

object LongCosterChain {
  def builder(): Builder = new Builder()

  class Builder private[LongCosterChain] {
    private val costers = mutable.ListBuffer[LongCoster]()
    private var out: Option[LongCosterChain] = None

    def register(coster: LongCoster): Builder = synchronized {
      costers += coster
      out = None
      this
    }

    private[cost] def build(): LongCosterChain = synchronized {
      if (out.isEmpty) {
        out = Some(new LongCosterChain(costers.toSeq))
      }
      return out.get
    }
  }
}
