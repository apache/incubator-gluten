package org.apache.spark.shuffle

import org.apache.spark.ShuffleDependency

private class ShuffleManagerLookup(all: Seq[(LookupKey, ShuffleManager)]) {
  private val allReversed = all.reverse

  def findShuffleManager[K, V, C](dependency: ShuffleDependency[K, V, C]): ShuffleManager = {
    this.synchronized {
      // The latest shuffle manager registered will be looked up earlier.
      allReversed.find(_._1.accepts(dependency)).map(_._2).getOrElse {
        throw new IllegalStateException(s"No ShuffleManager found for $dependency")
      }
    }
  }

  def all(): Seq[ShuffleManager] = {
    this.synchronized {
      all.map(_._2)
    }
  }
}
