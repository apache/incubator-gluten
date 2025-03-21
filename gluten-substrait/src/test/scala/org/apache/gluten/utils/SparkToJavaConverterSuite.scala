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
package org.apache.gluten.utils

import org.apache.gluten.substrait.`type`._

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.types.Decimal

import org.scalatest.funsuite.AnyFunSuite

import java.math.{BigDecimal, BigInteger, MathContext}
import java.util

class SparkToJavaConverterSuite extends AnyFunSuite {
  test("Convert decimal") {
    val decimal =
      SparkToJavaConverter.toJava(new Decimal().set(11, 5, 3), new DecimalTypeNode(false, 5, 3))
    val result = new BigDecimal(BigInteger.valueOf(11), 3, new MathContext(5))
    assert(result.equals(decimal))
  }

  test("Convert binary") {
    val binary =
      SparkToJavaConverter
        .toJava(Array[Byte](5, 6, 7), new BinaryTypeNode(false))
        .asInstanceOf[Array[Byte]]
    val result = Array[Byte](5, 6, 7)
    assert(result.sameElements(binary))
  }

  test("Convert row") {
    val doubleArray = Array[Any](6.66, 2.22)
    val strArray = Array[Any]("str1", "str2")
    val list = new GenericArrayData(doubleArray)
    val strList = new GenericArrayData(strArray)
    val array = new Array[Any](5)
    array(0) = true
    array(1) = 777777
    array(2) = list
    array(3) = new ArrayBasedMapData(strList, list)
    array(4) = 12356L
    val row = new GenericInternalRow(array)
    val fieldTypes = new util.ArrayList[TypeNode]()
    fieldTypes.add(new BooleanTypeNode(false))
    fieldTypes.add(new DateTypeNode(false))
    fieldTypes.add(new ListNode(false, new FP64TypeNode(false)))
    fieldTypes.add(new MapNode(false, new StringTypeNode(false), new FP64TypeNode(false)))
    fieldTypes.add(new TimestampTypeNode(false))
    val javaRow =
      SparkToJavaConverter.toJava(row, new StructNode(false, fieldTypes))
    val result = new util.ArrayList[Object]()
    result.add(java.lang.Boolean.TRUE)
    result.add(java.lang.Integer.valueOf(777777))
    val javaDoubleList = new util.ArrayList[Double]()
    javaDoubleList.add(6.66)
    javaDoubleList.add(2.22)
    result.add(javaDoubleList)
    val javaMap = new util.HashMap[String, Double]()
    javaMap.put("str1", 6.66)
    javaMap.put("str2", 2.22)
    result.add(javaMap)
    result.add(java.lang.Long.valueOf(12356))
    assert(javaRow == result)
  }
}
