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
package io.glutenproject.backendsapi

import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.execution.datasources.FileFormat

trait BackendSettings {
  def supportedFileFormats(): Set[Class[_ <: FileFormat]] = Set()
  def supportExpandExec(): Boolean = false
  def supportSortExec(): Boolean = false
  def supportHashBuildJoinTypeOnLeft: JoinType => Boolean = {
    case _: InnerLike | RightOuter | FullOuter => true
    case _ => false
  }
  def supportHashBuildJoinTypeOnRight: JoinType => Boolean = {
    case _: InnerLike | LeftOuter | FullOuter | LeftSemi | LeftAnti | _: ExistenceJoin => true
    case _ => false
  }
  def recreateJoinExecOnFallback(): Boolean = false
  def removeHashColumnFromColumnarShuffleExchangeExec(): Boolean = false
  def utilizeShuffledHashJoinHint(): Boolean = false
  def excludeScanExecFromCollapsedStage(): Boolean = false
  def avoidOverwritingFilterTransformer(): Boolean = false
  def fallbackFilterWithoutConjunctiveScan(): Boolean = false
}
