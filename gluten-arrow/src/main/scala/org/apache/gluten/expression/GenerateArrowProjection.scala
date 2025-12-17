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
package org.apache.gluten.expression

import org.apache.gluten.vectorized.ArrowColumnarRow

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, GenericInternalRow}
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReferences
import org.apache.spark.sql.catalyst.expressions.aggregate.NoOp
import org.apache.spark.sql.catalyst.expressions.codegen._

// ArrowProjection is not accessible in Java
abstract class BaseArrowProjection extends ArrowProjection

/**
 * Generates byte code that produces a [[InternalRow]] object that can update itself based on a new
 * input [[InternalRow]] for a fixed set of [[Expression Expressions]]. It exposes a `target`
 * method, which is used to set the row that will be updated. The internal [[InternalRow]] object
 * created internally is used only when `target` is not used.
 */
object GenerateArrowProjection extends CodeGenerator[Seq[Expression], ArrowProjection] {

  protected def canonicalize(in: Seq[Expression]): Seq[Expression] =
    in.map(ExpressionCanonicalizer.execute)

  protected def bind(in: Seq[Expression], inputSchema: Seq[Attribute]): Seq[Expression] =
    bindReferences(in, inputSchema)

  def generate(
      expressions: Seq[Expression],
      inputSchema: Seq[Attribute],
      useSubexprElimination: Boolean): ArrowProjection = {
    create(canonicalize(bind(expressions, inputSchema)), useSubexprElimination)
  }

  def generate(expressions: Seq[Expression], useSubexprElimination: Boolean): ArrowProjection = {
    create(canonicalize(expressions), useSubexprElimination)
  }

  protected def create(expressions: Seq[Expression]): ArrowProjection = {
    create(expressions, false)
  }

  private def create(
      expressions: Seq[Expression],
      useSubexprElimination: Boolean): ArrowProjection = {
    val ctx = newCodeGenContext()
    val validExpr = expressions.zipWithIndex.filter {
      case (NoOp, _) => false
      case _ => true
    }
    val exprVals = ctx.generateExpressions(validExpr.map(_._1), useSubexprElimination)

    // 4-tuples: (code for projection, isNull variable name, value variable name, column index)
    val projectionCodes: Seq[(String, String)] = validExpr.zip(exprVals).map {
      case ((e, i), ev) =>
        val value = JavaCode
          .global(ctx.addMutableState(CodeGenerator.javaType(e.dataType), "value"), e.dataType)
        val (code, isNull) = if (e.nullable) {
          val isNull = ctx.addMutableState(CodeGenerator.JAVA_BOOLEAN, "isNull")
          (
            s"""
               |${ev.code}
               |$isNull = ${ev.isNull};
               |$value = ${ev.value};
            """.stripMargin,
            JavaCode.isNullGlobal(isNull))
        } else {
          (
            s"""
               |${ev.code}
               |$value = ${ev.value};
            """.stripMargin,
            FalseLiteral)
        }
        // update value into intermediate
        val update =
          s"""
             |if (!$isNull) {
             |  values[$i] = $value;
             |} else {
             |  values[$i] = null;
             |}
             |""".stripMargin
        (code, update)
    }

    // Evaluate all the subexpressions.
    val evalSubexpr = ctx.subexprFunctionsCode

    val allProjections = ctx.splitExpressionsWithCurrentInputs(projectionCodes.map(_._1))
    val allUpdates = ctx.splitExpressionsWithCurrentInputs(projectionCodes.map(_._2))

    val codeBody = s"""
      public java.lang.Object generate(Object[] references) {
        return new SpecificArrowProjection(references);
      }

      class SpecificArrowProjection extends ${classOf[BaseArrowProjection].getName} {

        private Object[] references;
        private ${classOf[ArrowColumnarRow].getName} mutableRow;
        private Object[] values;
        private ${classOf[GenericInternalRow].getName} intermediate;
        ${ctx.declareMutableStates()}

        public SpecificArrowProjection(Object[] references) {
          this.references = references;
          mutableRow = null;
          values = new Object[${expressions.size}];
          intermediate = new ${classOf[GenericInternalRow].getName}(values);
          ${ctx.initMutableStates()}
        }

        public void initialize(int partitionIndex) {
          ${ctx.initPartition()}
        }

        public ${classOf[BaseArrowProjection].getName} target(
            ${classOf[ArrowColumnarRow].getName} row) {
          mutableRow = row;
          return this;
        }

        /* Provide immutable access to the last projected row. */
        public ${classOf[ArrowColumnarRow].getName} currentValue() {
          return (${classOf[ArrowColumnarRow].getName}) mutableRow;
        }

        public java.lang.Object apply(java.lang.Object _i) {
          InternalRow ${ctx.INPUT_ROW} = (InternalRow) _i;
          $evalSubexpr
          $allProjections
          // copy all the results into intermediate
          $allUpdates
          // write intermediate to mutableRow
          mutableRow.writeRow(intermediate);
          return mutableRow;
        }

        ${ctx.declareAddedFunctions()}
      }
    """

    val code = CodeFormatter.stripOverlappingComments(
      new CodeAndComment(codeBody, ctx.getPlaceHolderToComments()))
    logDebug(s"code for ${expressions.mkString(",")}:\n${CodeFormatter.format(code)}")

    val (clazz, _) = CodeGenerator.compile(code)
    clazz.generate(ctx.references.toArray).asInstanceOf[ArrowProjection]
  }
}
