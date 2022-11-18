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
package io.substrait.debug

import org.apache.spark.sql.catalyst.util.StringUtils.PlanStringConcat
import org.apache.spark.sql.internal.SQLConf

import RelToVerboseString.{verboseString, verboseStringWithSuffix}
import io.substrait.relation
import io.substrait.relation.Rel

import scala.collection.JavaConverters.asScalaBufferConverter

trait TreePrinter[T] {
  def tree(t: T): String
}

object TreePrinter {

  implicit object SubstraitRel extends TreePrinter[relation.Rel] {
    override def tree(t: Rel): String = TreePrinter.tree(t)
  }

  final def tree(rel: relation.Rel): String = treeString(rel, verbose = true)

  final def treeString(
      rel: relation.Rel,
      verbose: Boolean,
      addSuffix: Boolean = false,
      maxFields: Int = SQLConf.get.maxToStringFields,
      printOperatorId: Boolean = false): String = {
    val concat = new PlanStringConcat()
    treeString(rel, concat.append, verbose, addSuffix, maxFields, printOperatorId)
    concat.toString
  }

  def treeString(
      rel: relation.Rel,
      append: String => Unit,
      verbose: Boolean,
      addSuffix: Boolean,
      maxFields: Int,
      printOperatorId: Boolean): Unit = {
    generateTreeString(rel, 0, Nil, append, verbose, "", addSuffix, maxFields, printOperatorId)
  }

  /**
   * Appends the string representation of this node and its children to the given Writer.
   *
   * The `i`-th element in `lastChildren` indicates whether the ancestor of the current node at
   * depth `i + 1` is the last child of its own parent node. The depth of the root node is 0, and
   * `lastChildren` for the root node should be empty.
   */
  def generateTreeString(
      rel: relation.Rel,
      depth: Int,
      lastChildren: Seq[Boolean],
      append: String => Unit,
      verbose: Boolean,
      prefix: String = "",
      addSuffix: Boolean = false,
      maxFields: Int,
      printNodeId: Boolean,
      indent: Int = 0): Unit = {

    append("   " * indent)
    if (depth > 0) {
      lastChildren.init.foreach(isLast => append(if (isLast) "   " else ":  "))
      append(if (lastChildren.last) "+- " else ":- ")
    }

    val str = if (verbose) {
      if (addSuffix) verboseStringWithSuffix(rel, maxFields) else verboseString(rel, maxFields)
    } else {
      ""
    }
    append(prefix)
    append(str)
    append("\n")

    val children = rel.getInputs.asScala
    if (children.nonEmpty) {
      children.init.foreach(
        generateTreeString(
          _,
          depth + 1,
          lastChildren :+ false,
          append,
          verbose,
          prefix,
          addSuffix,
          maxFields,
          printNodeId = printNodeId,
          indent = indent))

      generateTreeString(
        children.last,
        depth + 1,
        lastChildren :+ true,
        append,
        verbose,
        prefix,
        addSuffix,
        maxFields,
        printNodeId = printNodeId,
        indent = indent)
    }
  }
}
