package org.apache.spark.broadcast

import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.SparkConf
import org.apache.spark.sql.test.SharedSparkSession

import scala.reflect.ClassTag

class BroadcastFactoryInjectionSuite extends SharedSparkSession {
  import BroadcastFactoryInjectionSuite._
  test("Get") {
    val factory = BroadcastFactoryInjection.get()
    assert(factory.isInstanceOf[TorrentBroadcastFactory])
  }

  test("Inject") {
    BroadcastFactoryInjection.get().stop()
    val factory = new DummyBroadcastFactory()
    assert(!factory.initialized)
    BroadcastFactoryInjection.inject(factory)
    assert(BroadcastFactoryInjection.get() eq factory)
    assert(factory.initialized)
    val text: String = "DUMMY"
    val b = SparkShimLoader.getSparkShims.broadcastInternal(sparkContext, text)
    assert(b.isInstanceOf[DummyBroadcast[String]])
  }
}

object BroadcastFactoryInjectionSuite {
  private class DummyBroadcast[T: ClassTag](id: Long, value: T) extends Broadcast[T](id) {
    override protected def getValue(): T = value
    override protected def doUnpersist(blocking: Boolean): Unit = {
      // No-op.
    }
    override protected def doDestroy(blocking: Boolean): Unit = {
      // No-op.
    }
  }

  private class DummyBroadcastFactory() extends CompatibleBroadcastFactory {
    var initialized = false

    override def initialize(isDriver: Boolean, conf: SparkConf): Unit = {
      initialized = true
    }
    override def newBroadcast[T: ClassTag](value: T, isLocal: Boolean, id: Long): Broadcast[T] = {
      newBroadcast(value, isLocal, id, serializedOnly = true)
    }
    override def newBroadcast[T: ClassTag](
        value: T,
        isLocal: Boolean,
        id: Long,
        serializedOnly: Boolean): Broadcast[T] = {
      new DummyBroadcast[T](id, value)
    }
    override def unbroadcast(id: Long, removeFromDriver: Boolean, blocking: Boolean): Unit = {
      // No-op
    }
    override def stop(): Unit = {
      // No-op.
    }
  }
}
