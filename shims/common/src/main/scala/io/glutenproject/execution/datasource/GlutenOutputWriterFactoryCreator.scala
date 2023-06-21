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
package io.glutenproject.execution.datasource

import org.apache.spark.sql.execution.datasources.OutputWriterFactory

trait GlutenOutputWriterFactoryCreator {
  def createFactory(
      fileFormat: String,
      options: Map[String, String] = Map.empty[String, String]): OutputWriterFactory
}

object GlutenOutputWriterFactoryCreator {
  private var INSTANCE: GlutenOutputWriterFactoryCreator = _

  def setInstance(instance: GlutenOutputWriterFactoryCreator): Unit = {
    if (INSTANCE == null) {
      INSTANCE = instance
    }
  }

  def getInstance(): GlutenOutputWriterFactoryCreator = {
    if (INSTANCE == null) {
      throw new IllegalStateException("Please set a valid GlutenOutputWriterFactoryCreator first.")
    }
    INSTANCE
  }
}
