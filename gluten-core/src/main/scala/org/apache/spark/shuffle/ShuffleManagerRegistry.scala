package org.apache.spark.shuffle

import org.apache.spark.SparkConf
import org.apache.spark.util.Utils

import scala.collection.mutable

class ShuffleManagerRegistry private[ShuffleManagerRegistry] {
  import ShuffleManagerRegistry._
  private val all: mutable.Buffer[(LookupKey, String)] = mutable.Buffer()
  private val routerBuilders: mutable.Buffer[RouterBuilder] = mutable.Buffer()

  def register(lookupKey: LookupKey, shuffleManagerClass: String): Unit = {
    val clazz = Utils.classForName(shuffleManagerClass)
    assert(
      classOf[ShuffleManager].isAssignableFrom(clazz),
      s"Shuffle manager class to register is not an implementation of Spark ShuffleManager")
    this.synchronized {
      all += lookupKey -> shuffleManagerClass
      // Invalidate all shuffle managers cached in each alive router builder instances.
      // Then, once the router builder is accessed, a new router will be forced to create.
      routerBuilders.foreach(_.invalidateCache())
    }
  }

  def newRouterBuilder(conf: SparkConf, isDriver: Boolean): RouterBuilder = this.synchronized {
    val out = new RouterBuilder(this, conf, isDriver)
    routerBuilders += out
    out
  }
}

object ShuffleManagerRegistry {
  private val instance = new ShuffleManagerRegistry()

  def get(): ShuffleManagerRegistry = instance

  class RouterBuilder(registry: ShuffleManagerRegistry, conf: SparkConf, isDriver: Boolean) {
    private var router: Option[ShuffleManagerRouter] = None

    private[ShuffleManagerRegistry] def invalidateCache(): Unit = synchronized {
      router = None
    }

    private[shuffle] def getOrBuild(): ShuffleManagerRouter = synchronized {
      if (router.isEmpty) {
        val instances = registry.all.map(key => key._1 -> instantiate(key._2, conf, isDriver))
        router = Some(new ShuffleManagerRouter(new ShuffleManagerLookup(instances)))
      }
      router.get
    }

    private def instantiate(clazz: String, conf: SparkConf, isDriver: Boolean): ShuffleManager = {
      Utils
        .instantiateSerializerOrShuffleManager[ShuffleManager](clazz, conf, isDriver)
    }
  }
}
