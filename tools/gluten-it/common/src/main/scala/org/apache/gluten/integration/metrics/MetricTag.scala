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
