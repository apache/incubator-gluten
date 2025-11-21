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
package org.apache.spark.sql.execution.unsafe

import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.serializer.{JavaSerializer, KryoSerializer}
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.physical.IdentityBroadcastMode
import org.apache.spark.sql.execution.joins.HashedRelationBroadcastMode
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.Platform

import java.util

class UnsafeColumnarBuildSideRelationTest extends SharedSparkSession {
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.memory.offHeap.size", "200M")
      .set("spark.memory.offHeap.enabled", "true")
  }

  var unsafeRelWithIdentityMode: UnsafeColumnarBuildSideRelation = _
  var unsafeRelWithHashMode: UnsafeColumnarBuildSideRelation = _
  var sampleBytes: Array[Array[Byte]] = _

  private def toUnsafeByteArray(bytes: Array[Byte]): UnsafeByteArray = {
    val buf = ArrowBufferAllocators.globalInstance().buffer(bytes.length)
    buf.setBytes(0, bytes, 0, bytes.length);
    new UnsafeByteArray(buf, bytes.length.toLong)
  }

  private def toByteArray(unsafeByteArray: UnsafeByteArray): Array[Byte] = {
    val byteArray = new Array[Byte](Math.toIntExact(unsafeByteArray.size()))
    Platform.copyMemory(
      null,
      unsafeByteArray.address(),
      byteArray,
      Platform.BYTE_ARRAY_OFFSET,
      byteArray.length)
    byteArray
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    val a = AttributeReference("a", StringType, nullable = false, null)()
    val output = Seq(a)
    sampleBytes = Array("12345".getBytes(), "7890".getBytes)
    unsafeRelWithIdentityMode = UnsafeColumnarBuildSideRelation(
      output,
      sampleBytes.map(a => toUnsafeByteArray(a)),
      IdentityBroadcastMode
    )
    unsafeRelWithHashMode = UnsafeColumnarBuildSideRelation(
      output,
      sampleBytes.map(a => toUnsafeByteArray(a)),
      HashedRelationBroadcastMode(output, isNullAware = false)
    )
  }

  test("Java default serialization") {
    val javaSerialization = new JavaSerializer(SparkEnv.get.conf)
    val serializerInstance = javaSerialization.newInstance()

    // test unsafeRelWithIdentityMode
    val buffer = serializerInstance.serialize(unsafeRelWithIdentityMode)
    val obj = serializerInstance.deserialize[UnsafeColumnarBuildSideRelation](buffer)
    assert(obj != null)
    assert(obj.mode == IdentityBroadcastMode)
    assert(
      util.Arrays.deepEquals(
        obj.getBatches().map(toByteArray).toArray[AnyRef],
        sampleBytes.asInstanceOf[Array[AnyRef]]))

    // test unsafeRelWithHashMode
    val buffer2 = serializerInstance.serialize(unsafeRelWithHashMode)
    val obj2 = serializerInstance.deserialize[UnsafeColumnarBuildSideRelation](buffer2)
    assert(obj2 != null)
    assert(obj2.mode.isInstanceOf[HashedRelationBroadcastMode])
    assert(
      util.Arrays.deepEquals(
        obj2.getBatches().map(toByteArray).toArray[AnyRef],
        sampleBytes.asInstanceOf[Array[AnyRef]]))
  }

  test("Kryo serialization") {
    val kryo = new KryoSerializer(SparkEnv.get.conf)
    val serializerInstance = kryo.newInstance()

    // test unsafeRelWithIdentityMode
    val buffer = serializerInstance.serialize(unsafeRelWithIdentityMode)
    val obj = serializerInstance.deserialize[UnsafeColumnarBuildSideRelation](buffer)
    assert(obj != null)
    assert(obj.mode == IdentityBroadcastMode)
    assert(
      util.Arrays.deepEquals(
        obj.getBatches().map(toByteArray).toArray[AnyRef],
        sampleBytes.asInstanceOf[Array[AnyRef]]))

    // test unsafeRelWithHashMode
    val buffer2 = serializerInstance.serialize(unsafeRelWithHashMode)
    val obj2 = serializerInstance.deserialize[UnsafeColumnarBuildSideRelation](buffer2)
    assert(obj2 != null)
    assert(obj2.mode.isInstanceOf[HashedRelationBroadcastMode])
    assert(
      util.Arrays.deepEquals(
        obj2.getBatches().map(toByteArray).toArray[AnyRef],
        sampleBytes.asInstanceOf[Array[AnyRef]]))
  }

}
