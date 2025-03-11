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
package org.apache.spark.sql.catalyst.expressions

import org.apache.gluten.execution.VeloxWholeStageTransformerSuite

import org.apache.spark.sql.types._

class VeloxCastSuite extends VeloxWholeStageTransformerSuite with ExpressionEvalHelper {
  def cast(v: Any, targetType: DataType, timeZoneId: Option[String] = None): Cast = {
    v match {
      case lit: Expression =>
        Cast(lit, targetType, timeZoneId)
      case _ =>
        val lit = Literal(v)
        Cast(lit, targetType, timeZoneId)
    }
  }

  test("cast binary to string type") {

    val testCases = Seq(
      ("Hello, World!".getBytes, "Hello, World!"),
      ("12345".getBytes, "12345"),
      ("".getBytes, ""),
      ("Some special characters: !@#$%^&*()".getBytes, "Some special characters: !@#$%^&*()"),
      ("Line\nbreak".getBytes, "Line\nbreak")
    )

    for ((binaryValue, expectedString) <- testCases) {
      checkEvaluation(cast(cast(binaryValue, BinaryType), StringType), expectedString)
    }
  }

  override protected val resourcePath: String = "N/A"
  override protected val fileFormat: String = "N/A"
}
