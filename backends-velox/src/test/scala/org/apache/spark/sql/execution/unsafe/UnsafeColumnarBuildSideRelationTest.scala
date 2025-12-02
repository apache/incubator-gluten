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
import org.apache.gluten.memory.memtarget.ThrowOnOomMemoryTarget.OutOfMemoryException

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.memory.GlobalOffHeapMemory
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.serializer.{JavaSerializer, KryoSerializer}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.physical.IdentityBroadcastMode
import org.apache.spark.sql.execution.joins.HashedRelationBroadcastMode
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.Platform

import java.util

import scala.collection.mutable
import scala.util.Random

class UnsafeColumnarBuildSideRelationTest extends SharedSparkSession {
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.memory.offHeap.size", "200M")
      .set("spark.memory.offHeap.enabled", "true")
  }

  var unsafeRelWithIdentityMode: UnsafeColumnarBuildSideRelation = _
  var unsafeRelWithHashMode: UnsafeColumnarBuildSideRelation = _
  var output: Seq[Attribute] = _
  var sampleBytes: Array[Array[Byte]] = _
  var initialGlobalBytes: Long = _

  private def toUnsafeByteArray(bytes: Array[Byte]): UnsafeByteArray = {
    val buf = ArrowBufferAllocators.globalInstance().buffer(bytes.length)
    buf.setBytes(0, bytes, 0, bytes.length)
    try {
      new UnsafeByteArray(buf, bytes.length.toLong)
    } finally {
      buf.close()
    }
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
    initialGlobalBytes = GlobalOffHeapMemory.currentBytes()
    output = Seq(AttributeReference("a", StringType, nullable = false, null)())
    sampleBytes = Array(randomBytes(10), randomBytes(100))
    unsafeRelWithIdentityMode = newUnsafeRelationWithIdentityMode(sampleBytes: _*)
    unsafeRelWithHashMode = newUnsafeRelationWithHashMode(sampleBytes: _*)
  }

  override protected def afterAll(): Unit = {
    // Makes sure all the underlying UnsafeByteArray instances become GC non-reachable and
    // be released after a full-GC.
    unsafeRelWithIdentityMode = null
    unsafeRelWithHashMode = null
    System.gc()
    Thread.sleep(500)
    // FIXME: This should be zero. We had to assert with the initial bytes because
    //  there were some allocations from the previous run suites.
    assert(GlobalOffHeapMemory.currentBytes() == initialGlobalBytes)
  }

  private def randomBytes(size: Int): Array[Byte] = {
    val array = new Array[Byte](size)
    val random = new Random()
    random.nextBytes(array)
    array
  }

  private def newUnsafeRelationWithIdentityMode(
      bytes: Array[Byte]*): UnsafeColumnarBuildSideRelation = {
    require(bytes.nonEmpty)
    UnsafeColumnarBuildSideRelation(
      output,
      bytes.map(a => toUnsafeByteArray(a)),
      IdentityBroadcastMode
    )
  }

  private def newUnsafeRelationWithHashMode(
      bytes: Array[Byte]*): UnsafeColumnarBuildSideRelation = {
    require(bytes.nonEmpty)
    UnsafeColumnarBuildSideRelation(
      output,
      bytes.map(a => toUnsafeByteArray(a)),
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

  test("Should throw OOM when off-heap memory is running out") {
    // 500 MiB > 200 MiB so OOM should be thrown.
    val relations = mutable.ListBuffer[UnsafeColumnarBuildSideRelation]()
    assertThrows[OutOfMemoryException] {
      for (i <- 0 until 10) {
        relations += newUnsafeRelationWithHashMode(randomBytes(ByteUnit.MiB.toBytes(50).toInt))
      }
    }
    relations.clear()
  }
}
