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

trait GlutenTestsBaseTrait {

  protected val rootPath: String = getClass.getResource("/").getPath
  protected val basePath: String = rootPath + "unit-tests-working-home"

  protected val warehouse: String = basePath + "/spark-warehouse"
  protected val metaStorePathAbsolute: String = basePath + "/meta"

  def whiteTestNameList: Seq[String] = Seq.empty

  // prefer to use blackTestNameList
  def blackTestNameList: Seq[String] = Seq.empty

  def whiteBlackCheck(testName: String): Boolean = {
    if (testName.startsWith(GlutenTestConstants.GLUTEN_TEST)) {
      true
    } else if (blackTestNameList.isEmpty && whiteTestNameList.isEmpty) {
      true
    } else if (blackTestNameList.nonEmpty &&
      blackTestNameList(0).equalsIgnoreCase(GlutenTestConstants.IGNORE_ALL)) {
      false
    } else if (blackTestNameList.nonEmpty) {
      !blackTestNameList.contains(testName)
    } else if (whiteTestNameList.nonEmpty) {
      whiteTestNameList.contains(testName)
    } else {
      false
    }
  }
}
