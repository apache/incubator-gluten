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
package org.apache.spark.sql.catalyst.expressions.objects

import org.apache.spark.sql.catalyst.expressions.Expression

/**
 * Extractors for Invoke expressions to ensure compatibility across different Spark versions.
 *
 * For Spark 3.3, StructsToJson is not replaced with Invoke expressions,
 * so this extractor returns None to maintain API compatibility with other versions.
 */
object StructsToJsonInvoke {
  def unapply(expr: Expression): Option[(Map[String, String], Expression, Option[String])] = {
    None
  }
}

