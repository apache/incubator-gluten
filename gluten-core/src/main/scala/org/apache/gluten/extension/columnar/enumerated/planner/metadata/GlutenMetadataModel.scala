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

import org.apache.gluten.extension.columnar.enumerated.planner.plan.GroupLeafExec
import org.apache.gluten.ras.{GroupLeafBuilder, Metadata, MetadataModel}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.SparkPlan

object GlutenMetadataModel extends Logging {
  def apply(): MetadataModel[SparkPlan] = {
    MetadataModelImpl
  }

  private object MetadataModelImpl extends MetadataModel[SparkPlan] {
    override def metadataOf(node: SparkPlan): Metadata = node match {
      case g: GroupLeafExec => throw new UnsupportedOperationException()
      case other =>
        GlutenMetadata(
          Schema(other.output),
          other.logicalLink.map(LogicalLink(_)).getOrElse(LogicalLink.notFound))
    }

    override def dummy(): Metadata = GlutenMetadata(Schema(List()), LogicalLink.notFound)
    override def verify(one: Metadata, other: Metadata): Unit = (one, other) match {
      case (left: GlutenMetadata, right: GlutenMetadata) =>
        implicitly[Verifier[Schema]].verify(left.schema(), right.schema())
        implicitly[Verifier[LogicalLink]].verify(left.logicalLink(), right.logicalLink())
      case _ => throw new IllegalStateException(s"Metadata mismatch: one: $one, other $other")
    }

    override def assignToGroup(group: GroupLeafBuilder[SparkPlan], meta: Metadata): Unit =
      (group, meta) match {
        case (builder: GroupLeafExec.Builder, metadata: GlutenMetadata) =>
          builder.withMetadata(metadata)
      }
  }

  trait Verifier[T <: Any] {
    def verify(one: T, other: T): Unit
  }
}
