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
package org.apache.spark.substrait

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.types._

import io.substrait.`type`.{NamedStruct, Type, TypeVisitor}
import io.substrait.function.TypeExpression
import io.substrait.utils.Util

import scala.collection.JavaConverters
import scala.collection.JavaConverters.asScalaBufferConverter

private class ToSparkType
  extends TypeVisitor.TypeThrowsVisitor[DataType, RuntimeException]("Unknown expression type.") {

  override def visit(expr: Type.I32): DataType = IntegerType
  override def visit(expr: Type.I64): DataType = LongType

  override def visit(expr: Type.FP32): DataType = FloatType
  override def visit(expr: Type.FP64): DataType = DoubleType

  override def visit(expr: Type.Decimal): DataType =
    DecimalType(expr.precision(), expr.scale())

  override def visit(expr: Type.Date): DataType = DateType

  override def visit(expr: Type.Str): DataType = StringType
}
class ToSubstraitType {

  def convert(typeExpression: TypeExpression): DataType = {
    typeExpression.accept(new ToSparkType)
  }

  def convert(dataType: DataType, nullable: Boolean): Option[Type] = {
    convert(dataType, Seq.empty, nullable)
  }

  def apply(dataType: DataType, nullable: Boolean): Type = {
    convert(dataType, Seq.empty, nullable)
      .getOrElse(
        throw new UnsupportedOperationException(s"Unable to convert the type ${dataType.typeName}"))
  }

  protected def convert(dataType: DataType, names: Seq[String], nullable: Boolean): Option[Type] = {
    val creator = Type.withNullability(nullable)
    dataType match {
      case BooleanType => Some(creator.BOOLEAN)
      case ByteType => Some(creator.I8)
      case ShortType => Some(creator.I16)
      case IntegerType => Some(creator.I32)
      case LongType => Some(creator.I64)
      case FloatType => Some(creator.FP32)
      case DoubleType => Some(creator.FP64)
      case decimal: DecimalType if decimal.precision <= 38 =>
        Some(creator.decimal(decimal.precision, decimal.scale))
      case charType: CharType => Some(creator.fixedChar(charType.length))
      case varcharType: VarcharType => Some(creator.varChar(varcharType.length))
      case StringType => Some(creator.STRING)
      case DateType => Some(creator.DATE)
      case TimestampType => Some(creator.TIMESTAMP)
      case TimestampNTZType => Some(creator.TIMESTAMP_TZ)
      case BinaryType => Some(creator.BINARY)
      case ArrayType(elementType, containsNull) =>
        convert(elementType, Seq.empty, containsNull).map(creator.list)
      case MapType(keyType, valueType, valueContainsNull) =>
        convert(keyType, Seq.empty, nullable = false)
          .flatMap(
            keyT =>
              convert(valueType, Seq.empty, valueContainsNull)
                .map(valueT => creator.map(keyT, valueT)))
      case _ =>
        None
    }
  }
  def toNamedStruct(output: Seq[Attribute]): Option[NamedStruct] = {
    val names = JavaConverters.seqAsJavaList(output.map(_.name))
    val creator = Type.withNullability(false)
    Util
      .seqToOption(output.map(a => convert(a.dataType, a.nullable)))
      .map(l => creator.struct(JavaConverters.asJavaIterable(l)))
      .map(NamedStruct.of(names, _))
  }
  def toNamedStruct(schema: StructType): NamedStruct = {
    val creator = Type.withNullability(false)
    val names = new java.util.ArrayList[String]
    val children = new java.util.ArrayList[Type]
    schema.fields.foreach(
      field => {
        names.add(field.name)
        children.add(apply(field.dataType, field.nullable))
      })
    val struct = creator.struct(children)
    NamedStruct.of(names, struct)
  }

  def toAttribute(namedStruct: NamedStruct): Seq[Attribute] = {
    namedStruct
      .struct()
      .fields()
      .asScala
      .map(t => (t, convert(t)))
      .zip(namedStruct.names().asScala)
      .map { case ((t, d), name) => StructField(name, d, t.nullable()) }
      .map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)())
      .toSeq()
  }
}

object ToSubstraitType extends ToSubstraitType
