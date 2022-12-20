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

package org.apache.spark.sql

import java.io.File
import scala.collection.mutable.ArrayBuffer
import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.execution.ProjectExecTransformer
import io.glutenproject.test.TestStats
import io.glutenproject.utils.SystemParameters
import org.apache.commons.io.FileUtils
import org.scalactic.source.Position
import org.scalatest.{Args, Status, Tag}
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.analysis.ResolveTimeZone
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.optimizer.{ConstantFolding, ConvertToLocalRelation, NullPropagation}
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

trait GlutenTestsCommonTrait
  extends SparkFunSuite with ExpressionEvalHelper with GlutenTestsBaseTrait {

  override def runTest(testName: String, args: Args): Status = {
    TestStats.suiteTestNumber += 1
    val status = super.runTest(testName, args)
    if (TestStats.offloadGluten) {
      TestStats.offloadGlutenTestNumber += 1
      print("'" + testName + "'" + " offload to gluten\n")
    } else {
      // you can find the keyword 'Validation failed for' in function doValidate() in log
      // to get the fallback reason
      print("'" + testName + "'" + " NOT use gluten\n")
    }
    status
  }

  override protected def test(testName: String,
                              testTags: Tag*)(testFun: => Any)(implicit pos: Position): Unit = {
    if (shouldRun(testName)) {
      super.test(testName, testTags: _*)(testFun)
    }
  }
}
