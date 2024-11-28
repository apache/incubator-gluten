package org.apache.spark.shuffle

import org.apache.spark.ShuffleDependency

trait LookupKey {
  def accepts[K, V, C](dependency: ShuffleDependency[K, V, C]): Boolean
}