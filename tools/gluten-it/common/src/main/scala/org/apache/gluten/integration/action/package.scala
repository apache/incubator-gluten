package org.apache.gluten.integration

package object action {
  implicit class DualOptionsOps[T](value: (Option[T], Option[T])) {
    def onBothProvided[R](func: (T, T) => R): Option[R] = {
      if (value._1.isEmpty || value._2.isEmpty) {
        return None
      }
      Some(func(value._1.get, value._2.get))
    }
  }

  implicit class DualMetricsOps(value: (Map[String, Long], Map[String, Long])) {
    def sumUp: Map[String, Long] = {
      assert(value._1.keySet == value._2.keySet)
      value._1.map { case (k, v) => k -> (v + value._2(k)) }
    }
  }
}
