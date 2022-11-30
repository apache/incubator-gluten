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

import com.google.common.collect.Lists
import io.glutenproject.expression.ConverterUtils.FunctionConfig
import io.glutenproject.substrait.`type`.TypeBuilder
import io.glutenproject.substrait.expression.{ExpressionBuilder, ExpressionNode}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

class IsNotNullTransformer(child: Expression, original: Expression)
  extends IsNotNull(child: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val child_node: ExpressionNode =
      child.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!child_node.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"not supported yet.")
    }
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ConverterUtils.IS_NOT_NULL, Seq(child.dataType)))
    val expressionNodes = Lists.newArrayList(child_node.asInstanceOf[ExpressionNode])
    val typeNode = TypeBuilder.makeBoolean(original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class IsNullTransformer(child: Expression, original: Expression)
  extends IsNotNull(child: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val child_node: ExpressionNode =
      child.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!child_node.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"not supported yet.")
    }
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ConverterUtils.IS_NULL, Seq(child.dataType)))
    val expressionNodes = Lists.newArrayList(child_node.asInstanceOf[ExpressionNode])
    val typeNode = TypeBuilder.makeBoolean(original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class NotTransformer(child: Expression, original: Expression)
  extends Not(child: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode = child.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!childNode.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"not supported yet.")
    }

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionName = ConverterUtils.makeFuncName(
      ConverterUtils.NOT, Seq(child.dataType), FunctionConfig.OPT)
    val functionId = ExpressionBuilder.newScalarFunction(functionMap, functionName)
    val expressNodes = Lists.newArrayList(childNode.asInstanceOf[ExpressionNode])
    val typeNode = TypeBuilder.makeBoolean(original.nullable)

    ExpressionBuilder.makeScalarFunction(functionId, expressNodes, typeNode)
  }
}

class AbsTransformer(child: Expression, original: Expression)
  extends Abs(child: Expression)
    with ExpressionTransformer
    with Logging {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode = child.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!childNode.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"Not supported yet.")
    }
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ConverterUtils.ABS, Seq(child.dataType), FunctionConfig.OPT))
    val expressionNodes = Lists.newArrayList(childNode.asInstanceOf[ExpressionNode])
    val typeNode = ConverterUtils.getTypeNode(child.dataType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class CeilTransformer(child: Expression, original: Expression)
  extends Ceil(child: Expression)
    with ExpressionTransformer
    with Logging {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode = child.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!childNode.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"Not supported yet.")
    }
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ConverterUtils.CEIL, Seq(child.dataType), FunctionConfig.OPT))
    val expressionNodes = Lists.newArrayList(childNode.asInstanceOf[ExpressionNode])
    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class FloorTransformer(child: Expression, original: Expression)
  extends Floor(child: Expression)
    with ExpressionTransformer
    with Logging {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode = child.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!childNode.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"Not supported yet.")
    }
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ConverterUtils.FLOOR, Seq(child.dataType), FunctionConfig.OPT))
    val expressionNodes = Lists.newArrayList(childNode.asInstanceOf[ExpressionNode])
    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class AcosTransformer(child: Expression, original: Expression)
  extends Acos(child: Expression)
    with ExpressionTransformer
    with Logging {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode = child.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!childNode.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"Not supported yet.")
    }
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ConverterUtils.ACOS, Seq(child.dataType), FunctionConfig.OPT))
    val expressionNodes = Lists.newArrayList(childNode.asInstanceOf[ExpressionNode])
    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class AsinTransformer(child: Expression, original: Expression)
  extends Asin(child: Expression)
    with ExpressionTransformer
    with Logging {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode = child.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!childNode.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"Not supported yet.")
    }
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ConverterUtils.ASIN, Seq(child.dataType), FunctionConfig.OPT))
    val expressionNodes = Lists.newArrayList(childNode.asInstanceOf[ExpressionNode])
    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class AtanTransformer(child: Expression, original: Expression)
  extends Atan(child: Expression)
    with ExpressionTransformer
    with Logging {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode = child.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!childNode.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"Not supported yet.")
    }
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ConverterUtils.ATAN, Seq(child.dataType), FunctionConfig.OPT))
    val expressionNodes = Lists.newArrayList(childNode.asInstanceOf[ExpressionNode])
    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class CosTransformer(child: Expression, original: Expression)
  extends Cos(child: Expression)
    with ExpressionTransformer
    with Logging {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode = child.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!childNode.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"Not supported yet.")
    }
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ConverterUtils.COS, Seq(child.dataType), FunctionConfig.OPT))
    val expressionNodes = Lists.newArrayList(childNode.asInstanceOf[ExpressionNode])
    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class CoshTransformer(child: Expression, original: Expression)
  extends Cosh(child: Expression)
    with ExpressionTransformer
    with Logging {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode = child.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!childNode.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"Not supported yet.")
    }
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ConverterUtils.COSH, Seq(child.dataType), FunctionConfig.OPT))
    val expressionNodes = Lists.newArrayList(childNode.asInstanceOf[ExpressionNode])
    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class DegreesTransformer(child: Expression, original: Expression)
  extends ToDegrees(child: Expression)
    with ExpressionTransformer
    with Logging {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode = child.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!childNode.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"Not supported yet.")
    }
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ConverterUtils.DEGREES, Seq(child.dataType), FunctionConfig.OPT))
    val expressionNodes = Lists.newArrayList(childNode.asInstanceOf[ExpressionNode])
    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class Log10Transformer(child: Expression, original: Expression)
  extends Log10(child: Expression)
    with ExpressionTransformer
    with Logging {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode = child.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!childNode.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"Not supported yet.")
    }
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ConverterUtils.LOG10, Seq(child.dataType), FunctionConfig.OPT))
    val expressionNodes = Lists.newArrayList(childNode.asInstanceOf[ExpressionNode])
    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class ExpTransformer(child: Expression, original: Expression)
  extends Exp(child: Expression)
    with ExpressionTransformer
    with Logging {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode = child.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!childNode.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"Not supported yet.")
    }
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ConverterUtils.EXP, Seq(child.dataType), FunctionConfig.OPT))
    val expressionNodes = Lists.newArrayList(childNode.asInstanceOf[ExpressionNode])
    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class ChrTransformer(child: Expression, original: Expression)
  extends Chr(child: Expression)
    with ExpressionTransformer
    with Logging {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode = child.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!childNode.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"Not supported yet.")
    }
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ConverterUtils.CHR, Seq(child.dataType), FunctionConfig.OPT))
    val expressionNodes = Lists.newArrayList(childNode.asInstanceOf[ExpressionNode])
    val typeNode = TypeBuilder.makeString(original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class MD5Transformer(child: Expression, original: Expression)
  extends Md5(child: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode = child.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!childNode.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"Not supported yet.")
    }

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ConverterUtils.MD5,
        Seq(child.dataType), FunctionConfig.OPT))
    val expressionNodes = Lists.newArrayList(childNode)

    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class LengthOfJsonArrayTransformer(child: Expression, original: Expression)
  extends LengthOfJsonArray(child: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode = child.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!childNode.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"Not supported yet.")
    }

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ConverterUtils.JSON_ARRAY_LENGTH,
        Seq(child.dataType), FunctionConfig.OPT))
    val expressionNodes = Lists.newArrayList(childNode)
    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class AsciiTransformer(child: Expression, original: Expression)
  extends Ascii(child: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode = child.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!childNode.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"Not supported yet.")
    }

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ConverterUtils.ASCII, Seq(child.dataType), FunctionConfig.OPT))
    val expressionNodes = Lists.newArrayList(childNode)
    val typeNode = TypeBuilder.makeI32(original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class SinTransformer(child: Expression, original: Expression)
  extends Sin(child: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode = child.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!childNode.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"Not supported yet.")
    }

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ConverterUtils.SIN, Seq(child.dataType), FunctionConfig.OPT))
    val expressionNodes = Lists.newArrayList(childNode)
    val typeNode = TypeBuilder.makeFP64(original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class SinhTransformer(child: Expression, original: Expression)
  extends Sinh(child: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode = child.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!childNode.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"Not supported yet.")
    }

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ConverterUtils.SINH, Seq(child.dataType), FunctionConfig.OPT))
    val expressionNodes = Lists.newArrayList(childNode)
    val typeNode = TypeBuilder.makeFP64(original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class TanTransformer(child: Expression, original: Expression)
  extends Tan(child: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode = child.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!childNode.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"Not supported yet.")
    }

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ConverterUtils.TAN, Seq(child.dataType), FunctionConfig.OPT))
    val expressionNodes = Lists.newArrayList(childNode)
    val typeNode = TypeBuilder.makeFP64(original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class TanhTransformer(child: Expression, original: Expression)
  extends Tanh(child: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode = child.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!childNode.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"Not supported yet.")
    }

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ConverterUtils.TANH, Seq(child.dataType), FunctionConfig.OPT))
    val expressionNodes = Lists.newArrayList(childNode)
    val typeNode = TypeBuilder.makeFP64(original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class BitwiseNotTransformer(child: Expression, original: Expression)
  extends BitwiseNot(child: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode = child.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!childNode.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"Not supported yet.")
    }

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ConverterUtils.BITWISE_NOT,
      Seq(child.dataType), FunctionConfig.OPT))
    val expressionNodes = Lists.newArrayList(childNode)
    val typeNode = TypeBuilder.makeI32(original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class LengthTransformer(child: Expression, original: Expression)
  extends Length(child: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode = child.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!childNode.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"Not supported yet.")
    }

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ConverterUtils.CHAR_LENGTH, Seq(child.dataType),
      FunctionConfig.OPT))
    val expressionNodes = Lists.newArrayList(childNode)
    val typeNode = TypeBuilder.makeI32(original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class LowerTransformer(child: Expression, original: Expression)
  extends Lower(child: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode = child.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!childNode.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"Not supported yet.")
    }

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ConverterUtils.LOWER, Seq(child.dataType), FunctionConfig.OPT))
    val expressionNodes = Lists.newArrayList(childNode)
    val typeNode = TypeBuilder.makeString(original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class UpperTransformer(child: Expression, original: Expression)
  extends Upper(child: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode = child.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!childNode.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"Not supported yet.")
    }

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ConverterUtils.UPPER, Seq(child.dataType), FunctionConfig.OPT))
    val expressionNodes = Lists.newArrayList(childNode)
    val typeNode = TypeBuilder.makeString(original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class ReverseTransformer(child: Expression, original: Expression)
  extends Reverse(child: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode = child.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!childNode.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"Not supported yet.")
    }

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ConverterUtils.REVERSE, Seq(child.dataType), FunctionConfig.OPT))
    val expressionNodes = Lists.newArrayList(childNode)
    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class KnownFloatingPointNormalizedTransformer(
                                               child: Expression,
                                               original: KnownFloatingPointNormalized)
  extends KnownFloatingPointNormalized(child: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    child.asInstanceOf[ExpressionTransformer].doTransform(args)
  }
}

class CheckOverflowTransformer(child: Expression, original: CheckOverflow)
  extends CheckOverflow(
    child: Expression,
    original.dataType: DecimalType,
    original.nullOnOverflow: Boolean)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    throw new UnsupportedOperationException("Not supported: CheckOverflow.")
  }
}

class CastTransformer(child: Expression,
                      datatype: DataType,
                      timeZoneId: Option[String],
                      original: Expression)
  extends Cast(child: Expression, datatype: DataType, timeZoneId: Option[String])
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode = child.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!childNode.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"not supported yet.")
    }

    val typeNode = ConverterUtils.getTypeNode(dataType, original.nullable)
    ExpressionBuilder.makeCast(typeNode, childNode.asInstanceOf[ExpressionNode],
      SQLConf.get.ansiEnabled)
  }
}

class UnscaledValueTransformer(child: Expression, original: Expression)
  extends UnscaledValue(child: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    throw new UnsupportedOperationException("Not supported: UnscaledValue.")
  }
}

class MakeDecimalTransformer(
                              child: Expression,
                              precision: Int,
                              scale: Int,
                              nullOnOverflow: Boolean,
                              original: Expression)
  extends MakeDecimal(child: Expression, precision: Int, scale: Int, nullOnOverflow: Boolean)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    throw new UnsupportedOperationException("Not supported: MakeDecimal.")
  }
}

class NormalizeNaNAndZeroTransformer(child: Expression, original: NormalizeNaNAndZero)
  extends NormalizeNaNAndZero(child: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    child.asInstanceOf[ExpressionTransformer].doTransform(args)
  }
}

class PromotePrecisionTransformer(child: Expression, original: PromotePrecision)
  extends PromotePrecision(child: Expression) with ExpressionTransformer with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    throw new UnsupportedOperationException("Not supported: PromotePrecision.")
  }
}

class SqrtTransformer(child: Expression, original: Expression)
  extends Sqrt(child: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode = child.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!childNode.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"Not supported yet.")
    }

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ConverterUtils.SQRT, Seq(child.dataType), FunctionConfig.OPT))
    val expressionNodes = Lists.newArrayList(childNode)
    val typeNode = TypeBuilder.makeFP64(original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class CbrtTransformer(child: Expression, original: Expression)
  extends Cbrt(child: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode = child.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!childNode.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"Not supported yet.")
    }

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ConverterUtils.CBRT, Seq(child.dataType), FunctionConfig.OPT))
    val expressionNodes = Lists.newArrayList(childNode)
    val typeNode = TypeBuilder.makeFP64(original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class ToDegreesTransformer(child: Expression, original: Expression)
  extends ToDegrees(child: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode = child.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!childNode.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"Not supported yet.")
    }

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ConverterUtils.DEGREES, Seq(child.dataType), FunctionConfig.OPT))
    val expressionNodes = Lists.newArrayList(childNode)
    val typeNode = TypeBuilder.makeFP64(original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class HexTransformer(child: Expression, original: Expression)
  extends Hex(child: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode = child.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!childNode.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"Not supported yet.")
    }

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ConverterUtils.HEX, Seq(child.dataType), FunctionConfig.OPT))
    val expressionNodes = Lists.newArrayList(childNode)
    val typeNode = TypeBuilder.makeString(original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class UnhexTransformer(child: Expression, original: Expression)
  extends Unhex(child: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode = child.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!childNode.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"Not supported yet.")
    }

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ConverterUtils.UNHEX, Seq(child.dataType), FunctionConfig.OPT))
    val expressionNodes = Lists.newArrayList(childNode)
    val typeNode = TypeBuilder.makeString(original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class SignumTransformer(child: Expression, original: Expression)
  extends Signum(child: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode = child.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!childNode.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"Not supported yet.")
    }

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ConverterUtils.SIGN, Seq(child.dataType), FunctionConfig.OPT))
    val expressionNodes = Lists.newArrayList(childNode)
    val typeNode = TypeBuilder.makeFP64(original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class Log1pTransformer(child: Expression, original: Expression)
  extends Log1p(child: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode = child.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!childNode.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"Not supported yet.")
    }

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ConverterUtils.LOG1P, Seq(child.dataType), FunctionConfig.OPT))
    val expressionNodes = Lists.newArrayList(childNode)
    val typeNode = TypeBuilder.makeFP64(original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class Log2Transformer(child: Expression, original: Expression)
  extends Log2(child: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode = child.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!childNode.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"Not supported yet.")
    }

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ConverterUtils.LOG2, Seq(child.dataType), FunctionConfig.OPT))
    val expressionNodes = Lists.newArrayList(childNode)
    val typeNode = TypeBuilder.makeFP64(original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class LogTransformer(child: Expression, original: Expression)
  extends Log(child: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode = child.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!childNode.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"Not supported yet.")
    }

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ConverterUtils.LOG, Seq(child.dataType), FunctionConfig.OPT))
    val expressionNodes = Lists.newArrayList(childNode)
    val typeNode = TypeBuilder.makeFP64(original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class ToRadiansTransformer(child: Expression, original: Expression)
  extends ToRadians(child: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode = child.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!childNode.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"Not supported yet.")
    }

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ConverterUtils.RADIANS, Seq(child.dataType), FunctionConfig.OPT))
    val expressionNodes = Lists.newArrayList(childNode)
    val typeNode = TypeBuilder.makeFP64(original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class RintTransformer(child: Expression, original: Expression)
  extends Rint(child: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode = child.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!childNode.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"Not supported yet.")
    }

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ConverterUtils.ROUND, Seq(child.dataType),
      ConverterUtils.FunctionConfig.OPT))
    val expressionNodes = Lists.newArrayList(childNode.asInstanceOf[ExpressionNode])
    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

object UnaryOperatorTransformer {

  def create(child: Expression, original: Expression): Expression = original match {
    case in: IsNull =>
      new IsNullTransformer(child, in)
    case i: IsNotNull =>
      new IsNotNullTransformer(child, i)
    case y: Year =>
      new YearTransformer(child)
    case q: Quarter =>
      new QuarterTransformer(child)
    case m: Month =>
      new MonthTransformer(child)
    case s: Second =>
      new SecondTransformer(child)
    case d: DayOfMonth =>
      new DayOfMonthTransformer(child)
    case doy: DayOfYear =>
      new DayOfYearTransformer(child)
    case dow: DayOfWeek =>
      new DayOfWeekTransformer(child)
    case wd: WeekDay =>
      new WeekDayTransformer(child)
    case wof: WeekOfYear =>
      new WeekOfYearTransformer(child)
    case n: Not =>
      new NotTransformer(child, n)
    case md5: Md5 =>
      new MD5Transformer(child, md5)
    case json: LengthOfJsonArray =>
      new LengthOfJsonArrayTransformer(child, json)
    case a: Abs =>
      new AbsTransformer(child, a)
    case c: Ceil =>
      new CeilTransformer(child, c)
    case f: Floor =>
      new FloorTransformer(child, f)
    case c: Acos =>
      new AcosTransformer(child, c)
    case s: Asin =>
      new AsinTransformer(child, s)
    case t: Atan =>
      new AtanTransformer(child, t)
    case t: Cos =>
      new CosTransformer(child, t)
    case t: Cosh =>
      new CoshTransformer(child, t)
    case t: ToDegrees =>
      new ToDegreesTransformer(child, t)
    case t: Log10 =>
      new Log10Transformer(child, t)
    case e: Exp =>
      new ExpTransformer(child, e)
    case s: Sin =>
      new SinTransformer(child, s)
    case s: Sinh =>
      new SinhTransformer(child, s)
    case t: Tan =>
      new TanTransformer(child, t)
    case t: Tanh =>
      new TanhTransformer(child, t)
    case s: Sqrt =>
      new SqrtTransformer(child, s)
    case c: Cbrt =>
      new CbrtTransformer(child, c)
    case h: Hex =>
      new HexTransformer(child, h)
    case u: Unhex =>
      new UnhexTransformer(child, u)
    case s: Signum =>
      new SignumTransformer(child, s)
    case l: Log1p =>
      new Log1pTransformer(child, l)
    case l: Log2 =>
      new Log2Transformer(child, l)
    case l: Log =>
      new LogTransformer(child, l)
    case t: ToRadians =>
      new ToRadiansTransformer(child, t)
    case r: Rint =>
      new RintTransformer(child, r)
//    case q: Quarter =>
//      new QuarterTransformer(child, q)
    case ascii: Ascii =>
      new AsciiTransformer(child, ascii)
    case chr: Chr =>
      new ChrTransformer(child, chr)
    case len: Length =>
      new LengthTransformer(child, len)
    case lower: Lower =>
      new LowerTransformer(child, lower)
    case upper: Upper =>
      new UpperTransformer(child, upper)
    case reverse: Reverse =>
      new ReverseTransformer(child, reverse)
    case c: Cast =>
      new CastTransformer(child, c.dataType, c.timeZoneId, c)
    case u: UnscaledValue =>
      new UnscaledValueTransformer(child, u)
    case u: MakeDecimal =>
      new MakeDecimalTransformer(child, u.precision, u.scale, u.nullOnOverflow, u)
    case n: BitwiseNot =>
      new BitwiseNotTransformer(child, n)
    case k: KnownFloatingPointNormalized =>
      new KnownFloatingPointNormalizedTransformer(child, k)
    case n: NormalizeNaNAndZero =>
      new NormalizeNaNAndZeroTransformer(child, n)
    case p: PromotePrecision =>
      new PromotePrecisionTransformer(child, p)
    case a: CheckOverflow =>
      new CheckOverflowTransformer(child, a)
    case a: UnixDate =>
      new UnixDateTransformer(child)
    case a: UnixSeconds =>
      new UnixSecondsTransformer(child)
    case a: UnixMillis =>
      new UnixMillisTransformer(child)
    case a: UnixMicros =>
      new UnixMicrosTransformer(child)
    case a: SecondsToTimestamp =>
      new SecondsToTimestampTransformer(child)
    case a: MillisToTimestamp =>
      new MillisToTimestampTransformer(child)
    case a: MicrosToTimestamp =>
      new MicrosToTimestampTransformer(child)
    case other =>
      throw new UnsupportedOperationException(s"not currently supported: $other.")
  }
}
