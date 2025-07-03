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
package org.apache.gluten.execution

/**
 * GlutenExpression is a marker trait for expressions that are specific to Gluten execution. It can
 * be used to identify expressions that should only be evaluated in the context of a Spark task.
 */
trait GlutenExpression {

  def onlyInSparkTask(): Boolean = false

}

trait GlutenTaskOnlyExpression extends GlutenExpression {

  override def onlyInSparkTask(): Boolean = true

}
