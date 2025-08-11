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

import org.apache.gluten.expression.SpecializedGettersGetVariantCompatible

import org.apache.spark.sql.catalyst.InternalRow

/** An internal-row base implementation that is compatible with both Spark 3.x and 4.x. */
abstract class InternalRowGetVariantCompatible
  extends InternalRow
  with SpecializedGettersGetVariantCompatible {
  override def getVariant(ordinal: Int): Nothing = throw new UnsupportedOperationException()
}
