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

package io.glutenproject.expression

import io.glutenproject.execution.{BasicScanExecTransformer, BatchScanExecTransformer, FileSourceScanExecTransformer}
import io.glutenproject.substrait.`type`._
import io.glutenproject.substrait.rel.LocalFilesNode.ReadFileFormat
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.optimizer._
import org.apache.spark.sql.catalyst.plans.{FullOuter, Inner, JoinType, LeftAnti, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

object ConverterUtils extends Logging {

  def getAttrFromExpr(fieldExpr: Expression, skipAlias: Boolean = false): AttributeReference = {
    fieldExpr match {
      case a: Cast =>
        getAttrFromExpr(a.child)
      case a: AggregateExpression =>
        getAttrFromExpr(a.aggregateFunction.children.head)
      case a: AttributeReference =>
        a
      case a: Alias =>
        if (skipAlias && a.child.isInstanceOf[AttributeReference]) {
          getAttrFromExpr(a.child)
        } else {
          a.toAttribute.asInstanceOf[AttributeReference]
        }
      case a: KnownFloatingPointNormalized =>
        logInfo(s"$a")
        getAttrFromExpr(a.child)
      case a: NormalizeNaNAndZero =>
        getAttrFromExpr(a.child)
      case c: Coalesce =>
        getAttrFromExpr(c.children.head)
      case i: IsNull =>
        getAttrFromExpr(i.child)
      case a: Add =>
        getAttrFromExpr(a.left)
      case s: Subtract =>
        getAttrFromExpr(s.left)
      case m: Multiply =>
        getAttrFromExpr(m.left)
      case d: Divide =>
        getAttrFromExpr(d.left)
      case u: Upper =>
        getAttrFromExpr(u.child)
      case ss: Substring =>
        getAttrFromExpr(ss.children.head)
      case other =>
        throw new UnsupportedOperationException(
          s"makeStructField is unable to parse from $other (${other.getClass}).")
    }
  }

  def getShortAttributeName(attr: Attribute): String = {
    val subIndex = attr.name.indexOf("(")
    if (subIndex != -1) {
      attr.name.substring(0, subIndex)
    } else {
      attr.name
    }
  }

  def genColumnNameWithExprId(attr: Attribute): String = {
    ConverterUtils.getShortAttributeName(attr) + "#" + attr.exprId.id
  }

  def getResultAttrFromExpr(fieldExpr: Expression,
                            name: String = "None",
                            dataType: Option[DataType] = None): AttributeReference = {
    fieldExpr match {
      case a: Cast =>
        val c = getResultAttrFromExpr(a.child, name, Some(a.dataType))
        AttributeReference(c.name, a.dataType, c.nullable, c.metadata)(c.exprId, c.qualifier)
      case a: AttributeReference =>
        if (name != "None") {
          new AttributeReference(name, a.dataType, a.nullable)()
        } else {
          a
        }
      case a: Alias =>
        if (name != "None") {
          a.toAttribute.asInstanceOf[AttributeReference].withName(name)
        } else {
          a.toAttribute.asInstanceOf[AttributeReference]
        }
      case d: DivideTransformer =>
        new AttributeReference(name, DoubleType, d.nullable)()
      case m: MultiplyTransformer =>
        new AttributeReference(name, m.dataType, m.nullable)()
      case other =>
        val a = if (name != "None") {
          new Alias(other, name)()
        } else {
          new Alias(other, "res")()
        }
        val tmpAttr = a.toAttribute.asInstanceOf[AttributeReference]
        if (dataType.isDefined) {
          new AttributeReference(tmpAttr.name, dataType.getOrElse(null), tmpAttr.nullable)()
        } else {
          tmpAttr
        }
    }
  }

  def getTypeNode(datatye: DataType, nullable: Boolean): TypeNode = {
    datatye match {
      case BooleanType =>
        TypeBuilder.makeBoolean(nullable)
      case DoubleType =>
        TypeBuilder.makeFP64(nullable)
      case StringType =>
        TypeBuilder.makeString(nullable)
      case LongType =>
        TypeBuilder.makeI64(nullable)
      case IntegerType =>
        TypeBuilder.makeI32(nullable)
      case DateType =>
        TypeBuilder.makeDate(nullable)
      case DecimalType() =>
        val decimalType = datatye.asInstanceOf[DecimalType]
        val precision = decimalType.precision
        val scale = decimalType.scale
        TypeBuilder.makeDecimal(nullable, precision, scale)
      case TimestampType =>
        TypeBuilder.makeTimestamp(nullable)
      case unknown =>
        throw new UnsupportedOperationException(s"Type $unknown not supported.")
    }
  }

  def getTypeNodeFromAttributes(attributes: Seq[Attribute]): java.util.ArrayList[TypeNode] = {
    val typeNodes = new java.util.ArrayList[TypeNode]()
    for (attr <- attributes) {
      typeNodes.add(getTypeNode(attr.dataType, attr.nullable))
    }
    typeNodes
  }

  def printBatch(cb: ColumnarBatch): Unit = {
    var batch = ""
    for (rowId <- 0 until cb.numRows()) {
      var row = ""
      for (colId <- 0 until cb.numCols()) {
        row += (cb.column(colId).getUTF8String(rowId) + " ")
      }
      batch += (row + "\n")
    }
    logWarning(s"batch:\n$batch")
  }

  def ifEquals(left: Seq[AttributeReference], right: Seq[NamedExpression]): Boolean = {
    if (left.size != right.size) return false
    for (i <- left.indices) {
      if (left(i).exprId != right(i).exprId) return false
    }
    true
  }

  override def toString: String = {
    s"ConverterUtils"
  }

  def powerOfTen(pow: Int): (String, Int, Int) = {
    val POWERS_OF_10: Array[(String, Int, Int)] = Array(
      ("1", 1, 0),
      ("10", 2, 0),
      ("100", 3, 0),
      ("1000", 4, 0),
      ("10000", 5, 0),
      ("100000", 6, 0),
      ("1000000", 7, 0),
      ("10000000", 8, 0),
      ("100000000", 9, 0),
      ("1000000000", 10, 0),
      ("10000000000", 11, 0),
      ("100000000000", 12, 0),
      ("1000000000000", 13, 0),
      ("10000000000000", 14, 0),
      ("100000000000000", 15, 0),
      ("1000000000000000", 16, 0),
      ("10000000000000000", 17, 0),
      ("100000000000000000", 18, 0),
      ("1000000000000000000", 19, 0),
      ("10000000000000000000", 20, 0),
      ("100000000000000000000", 21, 0),
      ("1000000000000000000000", 22, 0),
      ("10000000000000000000000", 23, 0),
      ("100000000000000000000000", 24, 0),
      ("1000000000000000000000000", 25, 0),
      ("10000000000000000000000000", 26, 0),
      ("100000000000000000000000000", 27, 0),
      ("1000000000000000000000000000", 28, 0),
      ("10000000000000000000000000000", 29, 0),
      ("100000000000000000000000000000", 30, 0),
      ("1000000000000000000000000000000", 31, 0),
      ("10000000000000000000000000000000", 32, 0),
      ("100000000000000000000000000000000", 33, 0),
      ("1000000000000000000000000000000000", 34, 0),
      ("10000000000000000000000000000000000", 35, 0),
      ("100000000000000000000000000000000000", 36, 0),
      ("1000000000000000000000000000000000000", 37, 0))
    POWERS_OF_10(pow)
  }

  // This enum is used to specify the function arg.
  object FunctionConfig extends Enumeration {
    type Config = Value
    val REQ, OPT, NON = Value
  }

  // This method is used to create a function name with input types.
  // The format would be aligned with that specified in Substrait.
  // The function name Format:
  // <function name>:<short_arg_type0>_<short_arg_type1>_..._<short_arg_typeN>
  def makeFuncName(funcName: String, datatypes: Seq[DataType],
                   config: FunctionConfig.Config = FunctionConfig.NON): String = {
    var typedFuncName = config match {
      case FunctionConfig.REQ =>
        funcName.concat(":req_")
      case FunctionConfig.OPT =>
        funcName.concat(":opt_")
      case FunctionConfig.NON =>
        funcName.concat(":")
      case other =>
        throw new UnsupportedOperationException(s"$other is not supported.")
    }
    for (idx <- datatypes.indices) {
      val datatype = datatypes(idx)
      typedFuncName = datatype match {
        case BooleanType =>
          // TODO: Not in Substrait yet.
          typedFuncName.concat("bool")
        case IntegerType =>
          typedFuncName.concat("i32")
        case LongType =>
          typedFuncName.concat("i64")
        case DoubleType =>
          typedFuncName.concat("fp64")
        case DateType =>
          typedFuncName.concat("date")
        case StringType =>
          typedFuncName.concat("str")
        case DecimalType() =>
          typedFuncName.concat("dec")
        case other =>
          throw new UnsupportedOperationException(s"Type $other not supported.")
      }
      // For the last item, do not need to add _.
      if (idx < (datatypes.size - 1)) {
        typedFuncName = typedFuncName.concat("_")
      }
    }
    typedFuncName
  }

  def convertJoinType(joinType: JoinType): String = {
    joinType match {
      case Inner =>
        "Inner"
      case FullOuter =>
        "Outer"
      case LeftOuter | RightOuter =>
        "Left"
      case LeftSemi =>
        "Semi"
      case LeftAnti =>
        "Anti"
    }
  }

  def getFileFormat(scan: BasicScanExecTransformer): ReadFileFormat = {
    scan match {
      case f: BatchScanExecTransformer =>
        f.scan.getClass.getSimpleName match {
          case "OrcScan" => ReadFileFormat.OrcReadFormat
          case "ParquetScan" => ReadFileFormat.ParquetReadFormat
          case "DwrfScan" => ReadFileFormat.DwrfReadFormat
          case _ => ReadFileFormat.UnknownFormat
        }
      case f: FileSourceScanExecTransformer =>
        f.relation.fileFormat.getClass.getSimpleName match {
          case "OrcFileFormat" => ReadFileFormat.OrcReadFormat
          case "ParquetFileFormat" => ReadFileFormat.ParquetReadFormat
          case "DwrfFileFormat" => ReadFileFormat.DwrfReadFormat
          case _ => ReadFileFormat.UnknownFormat
        }
      case other =>
        throw new UnsupportedOperationException(s"$other not supported.")
    }
  }

  // A prefix used in the iterator path.
  final val ITERATOR_PREFIX = "iterator:"

  // Function names used by Substrait plan.
  final val SUM = "sum"
  final val AVG = "avg"
  final val COUNT = "count"
  final val MIN = "min"
  final val MAX = "max"
  final val ADD = "add"
  final val SUBTRACT = "subtract"
  final val MULTIPLY = "multiply"
  final val DIVIDE = "divide"
  final val AND = "and"
  final val OR = "or"
  final val COALESCE = "coalesce"
  final val LIKE = "like"
  final val EQUAL = "equal"
  final val LESS_THAN = "lt"
  final val LESS_THAN_OR_EQUAL = "lte"
  final val GREATER_THAN = "gt"
  final val GREATER_THAN_OR_EQUAL = "gte"
  final val ALIAS = "alias"
  final val IS_NOT_NULL = "is_not_null"
  final val IS_NULL = "is_null"
  final val NOT = "not"

  // String
  final val ASCII = "ascii"
  final val EXTRACT = "extract"
  final val ENDS_WITH = "ends_with"
  final val CONCAT = "concat"
  final val CONTAINS = "contains"
  final val INSTR = "strpos" // instr
  final val LENGTH = "char_length" // length
  final val LOWER = "lower"
  final val UPPER = "upper"
  final val REPLACE = "replace"
  final val SPLIT = "split"
  final val STARTS_WITH = "starts_with"
  final val SUBSTRING = "substring"

  // Other
  final val ROW_CONSTRUCTOR = "row_constructor"
}
