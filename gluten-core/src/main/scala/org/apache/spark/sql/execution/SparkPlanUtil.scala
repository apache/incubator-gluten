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
package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.trees.TreeNode

import java.lang.reflect.Method

/**
 * This is a hack to access withNewChildrenInternal method of TreeNode, which is a protected method
 */
object SparkPlanUtil {
  def withNewChildrenInternal(plan: SparkPlan, children: IndexedSeq[SparkPlan]): SparkPlan = {
    // 1. Get the Method object for the protected method
    val method: Method =
      classOf[TreeNode[_]].getDeclaredMethod("withNewChildrenInternal", classOf[IndexedSeq[_]])

    // 2. Make it accessible, bypassing the 'protected' modifier
    method.setAccessible(true)

    // 3. Invoke the method on the specific instance of the SparkPlan
    //    and cast the result to the expected type SparkPlan.
    method.invoke(plan, children).asInstanceOf[SparkPlan]
  }
}
