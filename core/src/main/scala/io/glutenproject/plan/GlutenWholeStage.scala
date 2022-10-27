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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.gluten.errors.GlutenError
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.substrait.TypeConverter

import io.substrait.plan.{ImmutablePlan, ImmutableRoot, Plan}
import io.substrait.relation.{LocalFiles, Rel}

/** An trait for those physical operators that support native execution. */
trait GlutenPlan extends SparkPlan {

  override def supportsColumnar: Boolean = true

  override def doExecute(): RDD[InternalRow] =
    throw GlutenError.executeCodePathUnsupportedError(nodeName)
}

/** An trait for [[GlutenPlan]]s that can convert to substrait [[Rel]] */
trait SubstraitSupport[T <: Rel] {
  self: GlutenPlan =>

  /**
   * Convert current [[GlutenPlan]] to substrait [[Rel]]
   *
   * @return
   *   substrait [[Rel]]
   */
  def convert: T

  /**
   * Returns all the RDDs of ColumnarBatch which generates the input rows.
   *
   * @note
   *   Right now we support up to <B>two</B> RDDs
   */
  def inputColumnarRDDs: Seq[RDD[ColumnarBatch]]
}

object Substrait {
  def localFiles(output: Seq[Attribute])(onError: => LocalFiles): LocalFiles = {
    TypeConverter
      .toNamedStruct(output)
      .map(schema => LocalFiles.builder().initialSchema(schema).build())
      .getOrElse(onError)
  }
}

trait SingleRel[T <: Rel] extends UnaryExecNode with GlutenPlan with SubstraitSupport[T] {

  def substraitChild: SubstraitSupport[_ <: Rel] = child.asInstanceOf[SubstraitSupport[_ <: Rel]]

  override def inputColumnarRDDs: Seq[RDD[ColumnarBatch]] =
    substraitChild.inputColumnarRDDs

}

/** [[PlaceHolder]] for [[SparkPlan]] which don't support native execution. */
case class PlaceHolder(jvmPlan: SparkPlan, children: Seq[SparkPlan]) extends GlutenPlan {
  override def output: Seq[Attribute] = jvmPlan.output
  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[SparkPlan]): PlaceHolder = {
    copy(children = newChildren)
  }
}

/**
 * [[GlutenInputAdapter]] is used to hide a [[SubstraitSupport]] GlutenPlan from a subtree that
 * supports native execution.
 *
 * This is the leaf node of a tree with GlutenWholeStage that is used to generate substrait plan
 * that consumes an RDD iterator of [[ColumnarBatch]]
 */
case class GlutenInputAdapter(child: GlutenPlan)
  extends UnaryExecNode
  with GlutenPlan
  with SubstraitSupport[LocalFiles] {

  override protected def withNewChildInternal(newChild: SparkPlan): GlutenInputAdapter =
    copy(child = newChild.asInstanceOf[GlutenPlan])

  override def output: Seq[Attribute] = child.output

  override def convert: LocalFiles = {
    Substrait.localFiles(output)(
      throw new UnsupportedOperationException(s"$nodeName.convert() fails")
    )
  }

  override def inputColumnarRDDs: Seq[RDD[ColumnarBatch]] = child.executeColumnar() :: Nil
}

/**
 * [[GlutenWholeStage]] converts a subtree of [[SubstraitSupport]] GlutenPlans that support native
 * execution pipeline together into single Native RDD.
 *
 * Here is the call graph of to generate Substrait plan (plan A supports Substrait, but plan B does
 * not):
 *
 * <pre> [[GlutenWholeStage]] Plan A [[GlutenInputAdapter]] Plan B
 * \=========================================================================
 *
 * -> execute() \| doExecuteColumnar() -> inputColumnarRDDs -> inputColumnarRDDs -> executeColumnar
 * \| +-----------------> convert() -------> convert() </pre>
 *
 * SparkPlan A should inherit from [[SubstraitSupport]] and implement convert
 */
case class GlutenWholeStage(child: SparkPlan)(val transformStageId: Int)
  extends UnaryExecNode
  with GlutenPlan {

  override protected def withNewChildInternal(newChild: SparkPlan): GlutenWholeStage =
    copy(child = newChild)(transformStageId)

  override def output: Seq[Attribute] = child.output

  def doSubstraitGen(): Plan = {
    ImmutablePlan
      .builder()
      .addRoots(
        ImmutableRoot
          .builder()
          .input(child.asInstanceOf[SubstraitSupport[_ <: Rel]].convert)
          .build())
      .build()
  }
}
