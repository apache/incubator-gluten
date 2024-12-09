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
package org.apache.gluten.extension.columnar.enumerated.planner.metadata

import org.apache.gluten.extension.columnar.enumerated.planner.metadata.GlutenMetadataModel.Verifier

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Attribute

case class Schema(output: Seq[Attribute]) {
  private val hash = output.map(_.semanticHash()).hashCode()

  override def hashCode(): Int = {
    hash
  }

  override def equals(obj: Any): Boolean = obj match {
    case other: Schema =>
      semanticEquals(other)
    case _ =>
      false
  }

  private def semanticEquals(other: Schema): Boolean = {
    if (output.size != other.output.size) {
      return false
    }
    output.zip(other.output).forall {
      case (left, right) =>
        left.semanticEquals(right)
    }
  }

  override def toString: String = {
    output.toString()
  }
}

object Schema {
  implicit val verifier: Verifier[Schema] = new Verifier[Schema] with Logging {
    override def verify(one: Schema, other: Schema): Unit = {
      if (one != other) {
        // We apply loose restriction on schema. Since Gluten still have some customized
        // logics causing schema of an operator to change after being transformed.
        // For example: https://github.com/apache/incubator-gluten/pull/5171
        logWarning(s"Warning: Schema mismatch: one: $one, other: $other")
      }
    }
  }
}
