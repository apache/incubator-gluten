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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.NamedExpression

import scala.util.control.Breaks.{break, breakable}

object CHAggUtil extends Logging {
  def distinctIgnoreQualifier(expressions: Seq[NamedExpression]): Seq[NamedExpression] = {
    var dist = List[NamedExpression]()
    for (i <- expressions.indices) {
      var k = -1
      breakable {
        for (j <- 0 to i - i)
          if (
            j != i &&
            expressions(i).name == expressions(j).name &&
            expressions(i).exprId == expressions(j).exprId &&
            expressions(i).dataType == expressions(j).dataType &&
            expressions(i).nullable == expressions(j).nullable
          ) {
            k = j
            break
          }
      }
      if (k < 0) dist = dist :+ expressions(i)
    }
    dist
  }
}
