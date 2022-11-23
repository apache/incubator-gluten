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

package io.glutenproject.integration.tpc

import org.apache.spark.sql.types.{DataType, DecimalType, StructField, StructType}

trait DataGen {
  def gen(): Unit
}

abstract class TypeModifier(val from: DataType, val to: DataType) extends Serializable {
  def modValue(value: Any): Any
}

class NoopModifier(t: DataType) extends TypeModifier(t, t) {
  override def modValue(value: Any): Any = value
}

object DataGen {
  def getRowModifier(schema: StructType, typeModifiers: List[TypeModifier]): Int => TypeModifier = {
    val typeMapping: java.util.Map[DataType, TypeModifier] = new java.util.HashMap()
    typeModifiers.foreach { m =>
      if (typeMapping.containsKey(m.from)) {
        throw new IllegalStateException()
      }
      typeMapping.put(m.from, m)
    }
    val modifiers = new java.util.ArrayList[TypeModifier]()
    schema.fields.foreach { f =>
      if (typeMapping.containsKey(f.dataType)) {
        modifiers.add(typeMapping.get(f.dataType))
      } else if (f.dataType.isInstanceOf[DecimalType]) {
        modifiers.add(typeMapping.get(DecimalType.SYSTEM_DEFAULT))
      } else {
        modifiers.add(new NoopModifier(f.dataType))
      }
    }
    i => modifiers.get(i)
  }

  def modifySchema(schema: StructType, rowModifier: Int => TypeModifier): StructType = {
    val modifiedSchema = new StructType(
      schema.fields.zipWithIndex.map { case (f, i) =>
        val modifier = rowModifier.apply(i)
        StructField(f.name, modifier.to, f.nullable, f.metadata)
      })
    modifiedSchema
  }
}