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
package org.apache.iceberg.transforms

import org.apache.gluten.proto.{IcebergPartitionField, TransformType}

import org.apache.iceberg.PartitionField

object IcebergTransformUtil {

  def convertPartitionField(field: PartitionField): IcebergPartitionField = {
    val transform = field.transform()
    // TODO: if the field is in nest column, concat it.
    var builder = IcebergPartitionField.newBuilder().setName(field.name())
    builder = transform match {
      case _: Identity[_] => builder.setTransform(TransformType.IDENTITY)
      case _: Years[_] => builder.setTransform(TransformType.YEAR)
      case _: Months[_] => builder.setTransform(TransformType.MONTH)
      case _: Days[_] => builder.setTransform(TransformType.DAY)
      case _: Hours[_] => builder.setTransform(TransformType.HOUR)
      case b: Bucket[_] => builder.setTransform(TransformType.BUCKET).setParameter(b.numBuckets())
      case t: Truncate[_] => builder.setTransform(TransformType.TRUNCATE).setParameter(t.width)
    }
    builder.build()
  }
}
