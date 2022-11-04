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
package io.substrait.spark.debug

import io.substrait.relation
import io.substrait.relation.{Aggregate, NamedScan, Project}

import scala.collection.mutable

class SubstraitVerboseStringConverter(addSuffix: Boolean)
  extends relation.AbstractRelVisitor[String, RuntimeException] {

  private def stringBuilder(rel: relation.Rel, remapLength: Int): mutable.StringBuilder = {
    val nodeName = rel.getClass.getSimpleName.replaceAll("Immutable", "")
    val builder: mutable.StringBuilder = new mutable.StringBuilder(s"$nodeName[")
    rel.getRemap.ifPresent(remap => builder.append("remap=").append(remap))
    if (builder.length > remapLength) builder.append(", ")
    builder
  }

  private def withBuilder(rel: relation.Rel, remapLength: Int)(
      f: mutable.StringBuilder => Unit): String = {
    val builder = stringBuilder(rel, remapLength)
    f(builder)
    builder.append("]").toString
  }

  def apply(rel: relation.Rel, maxFields: Int): String = {
    rel.accept(this)
  }

  override def visitFallback(rel: relation.Rel): String =
    throw new UnsupportedOperationException(
      s"Type ${rel.getClass.getCanonicalName}" +
        s" not handled by visitor type ${getClass.getCanonicalName}.")

  override def visit(namedScan: NamedScan): String = {
    withBuilder(namedScan, 10)(
      builder => {
        builder.append("initialSchema=").append(namedScan.getInitialSchema)
        namedScan.getFilter.ifPresent(
          filter => {
            builder.append(", ")
            builder.append("filter=").append(filter)
          })

        namedScan.getGeneralExtension.ifPresent(
          generalExtension => {
            builder.append(", ")
            builder.append("generalExtension=").append(generalExtension)
          })
        builder.append(", ")
        builder.append("names=").append(namedScan.getNames)

        namedScan.getExtension.ifPresent(
          extension => {
            builder.append(", ")
            builder.append("extension=").append(extension)
          })
      })
  }

  override def visit(project: Project): String = {
    withBuilder(project, 8)(
      builder => {
        builder
          .append("expressions=")
          .append(project.getExpressions)
      })
  }

  override def visit(aggregate: Aggregate): String = {
    withBuilder(aggregate, 10)(
      builder => {
        builder
          .append("groupings=")
          .append(aggregate.getGroupings)
          .append(", ")
          .append("measures=")
          .append(aggregate.getMeasures)
      })
  }
}

object SubstraitVerboseStringConverter {
  val verboseStringWithSuffix = new SubstraitVerboseStringConverter(true)
  val verboseString = new SubstraitVerboseStringConverter(false)
}
