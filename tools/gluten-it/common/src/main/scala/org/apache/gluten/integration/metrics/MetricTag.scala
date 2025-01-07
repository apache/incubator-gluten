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

package org.apache.gluten.integration.metrics

import scala.reflect.{ClassTag, classTag}

trait MetricTag[T] {
  import MetricTag._
  final def name(): String = nameOf(ClassTag(this.getClass))
  def value(): T
}

object MetricTag {
  def nameOf[T <: MetricTag[_]: ClassTag]: String = {
    val clazz = classTag[T].runtimeClass
    assert(classOf[MetricTag[_]].isAssignableFrom(clazz))
    clazz.getSimpleName
  }
  case class IsSelfTime() extends MetricTag[Nothing] {
    override def value(): Nothing = {
      throw new UnsupportedOperationException()
    }
  }
}
