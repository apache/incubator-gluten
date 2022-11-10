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

import io.glutenproject.utils.NotSupport

trait GlutenTestsBaseTrait {

  protected val rootPath: String = getClass.getResource("/").getPath
  protected val basePath: String = rootPath + "unit-tests-working-home"

  protected val warehouse: String = basePath + "/spark-warehouse"
  protected val metaStorePathAbsolute: String = basePath + "/meta"

  def whiteTestNameList: Seq[String] = Seq.empty

  // prefer to use blackTestNameList
  def blackTestNameList: Seq[String] =
    NotSupport.NotYetSupportCase(getClass.getSuperclass.getSimpleName)


  def whiteBlackCheck(testName: String): Boolean = {
    if (testName.startsWith(GlutenTestConstants.GLUTEN_TEST)) {
      true
    } else if (blackTestNameList.isEmpty && whiteTestNameList.isEmpty) {
      true
    } else if (blackTestNameList.nonEmpty &&
               blackTestNameList.head.equalsIgnoreCase(GlutenTestConstants.IGNORE_ALL)) {
      false
    } else if (blackTestNameList.nonEmpty) {
      val exactContain = blackTestNameList.contains(testName)
      if (exactContain) return false

      // some test cases' names start with SPARK-ISSUE_ID:
      // we allow simply put SPARK-ISSUE_ID in blacklist for short
      var fuzzyContain = false
      if (testName.startsWith("SPARK-")) {
        import scala.util.matching.Regex
        val issueIDPattern = "SPARK-[0-9]+".r
        val issueID = issueIDPattern.findFirstIn(testName) match {
          case Some(x: String) => x
        }
        fuzzyContain = blackTestNameList.exists(_.startsWith(issueID))
      }
      !fuzzyContain
    } else if (whiteTestNameList.nonEmpty) {
      whiteTestNameList.contains(testName)
    } else {
      false
    }
  }
}
