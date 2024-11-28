package org.apache.spark.shuffle

import org.apache.spark.ShuffleDependency

/**
 * Required during shuffle manager registration to determine whether the shuffle manager should be
 * used for the particular shuffle dependency.
 */
trait LookupKey {
  def accepts[K, V, C](dependency: ShuffleDependency[K, V, C]): Boolean
}
