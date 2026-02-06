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
package org.apache.gluten.ras.path

import org.apache.gluten.ras._
import org.apache.gluten.ras.memo.MemoStore

trait RasPath[T <: AnyRef] {
  def ras(): Ras[T]
  def keys(): PathKeySet
  def height(): Int
  def node(): RasPath.PathNode[T]
  def plan(): T
}

object RasPath {
  val INF_DEPTH: Int = Int.MaxValue

  trait PathNode[T <: AnyRef] {
    def self(): RasNode[T]
    def children(): Seq[PathNode[T]]
  }

  object PathNode {
    def apply[T <: AnyRef](node: RasNode[T], children: Seq[PathNode[T]]): PathNode[T] = {
      new PathNodeImpl(node, children)
    }
  }

  implicit class PathNodeImplicits[T <: AnyRef](pNode: RasPath.PathNode[T]) {
    def zipChildrenWithGroupIds(): Seq[(RasPath.PathNode[T], Int)] = {
      pNode
        .children()
        .zip(pNode.self().asCanonical().getChildrenGroupIds())
    }

    def zipChildrenWithGroups(
        allGroups: Int => RasGroup[T]): Seq[(RasPath.PathNode[T], RasGroup[T])] = {
      pNode
        .children()
        .zip(pNode.self().asCanonical().getChildrenGroups(allGroups).map(_.group(allGroups)))
    }
  }

  private def apply[T <: AnyRef](
      ras: Ras[T],
      keys: PathKeySet,
      height: Int,
      node: RasPath.PathNode[T]): RasPath[T] = {
    new RasPathImpl(ras, keys, height, node)
  }

  // Returns none if children doesn't share at least one path key.
  def apply[T <: AnyRef](
      ras: Ras[T],
      node: RasNode[T],
      children: Seq[RasPath[T]]): Option[RasPath[T]] = {
    assert(children.forall(_.ras() eq ras))

    val newKeysUnsafe = children.map(_.keys().keys()).reduce[Set[PathKey]] {
      case (one, other) =>
        one.intersect(other)
    }
    if (newKeysUnsafe.isEmpty) {
      return None
    }
    val newKeys = PathKeySet(newKeysUnsafe)
    Some(
      RasPath(
        ras,
        newKeys,
        1 + children.map(_.height()).reduceOption(_ max _).getOrElse(0),
        PathNode(node, children.map(_.node()))))
  }

  def zero[T <: AnyRef](ras: Ras[T], keys: PathKeySet, group: GroupNode[T]): RasPath[T] = {
    RasPath(ras, keys, 0, PathNode(group, List.empty))
  }

  def one[T <: AnyRef](
      ras: Ras[T],
      keys: PathKeySet,
      allGroups: Int => RasGroup[T],
      canonical: CanonicalNode[T]): RasPath[T] = {
    RasPath(
      ras,
      keys,
      1,
      PathNode(canonical, canonical.getChildrenGroups(allGroups).map(g => PathNode(g, List.empty))))
  }

  def cartesianProduct[T <: AnyRef](
      ras: Ras[T],
      canonical: CanonicalNode[T],
      children: Seq[Iterable[RasPath[T]]]): Iterable[RasPath[T]] = {
    // Apply cartesian product across all children to get an enumeration
    // of all possible choices of parent and children.
    //
    // Example:
    //
    // Root:
    //   n0(group1, group2)
    // Children Input:
    //   (group1, group2)
    // = ([n1 || n2], [n3 || n4 || n5])
    // = ([p1 || p2.1 || p2.2], [p3 || p4 || p5]) (expanded)
    // Children Output:
    //   [(p1, p3), (p1, p4), (p1, p5), (p2.1, p3), (p2.1, p4), (p2.1, p5),
    //    (p2.2, p3), (p2.2, p4), (p2.2, p5))] (choices)
    // Path enumerated:
    //   [p0.1(p1, p3), p0.2(p1, p4), p0.3(p1, p5), p0.4(p2.1, p3), p0.5(p2.1, p4),
    //    p0.6(p2.1, p5), p0.7(p2.2, p3), p0.8(p2.2, p4), p0.9(p2.2, p5)] (output)
    //
    // TODO: Make inner builder list mutable to reduce memory usage
    val choicesBuilder: Iterable[Seq[RasPath[T]]] = List(List.empty)
    val choices: Iterable[Seq[RasPath[T]]] = children
      .foldLeft(choicesBuilder) {
        (choicesBuilder: Iterable[Seq[RasPath[T]]], child: Iterable[RasPath[T]]) =>
          for (left <- choicesBuilder; right <- child) yield left :+ right
      }

    choices.flatMap { children: Seq[RasPath[T]] => RasPath(ras, canonical, children) }
  }

  implicit class RasPathImplicits[T <: AnyRef](path: RasPath[T]) {
    def dive(memoStore: MemoStore[T], extraDepth: Int): Iterable[RasPath[T]] = {
      val accumulatedDepth = extraDepth match {
        case RasPath.INF_DEPTH => RasPath.INF_DEPTH
        case _ =>
          Math.addExact(path.height(), extraDepth)
      }

      val finder = PathFinder
        .builder(path.ras(), memoStore)
        .depth(accumulatedDepth)
        .build()
      finder.find(path)
    }
  }

  private class PathNodeImpl[T <: AnyRef](
      override val self: RasNode[T],
      override val children: Seq[PathNode[T]])
    extends PathNode[T]

  private class RasPathImpl[T <: AnyRef](
      override val ras: Ras[T],
      override val keys: PathKeySet,
      override val height: Int,
      override val node: RasPath.PathNode[T])
    extends RasPath[T] {
    assert(height >= 0)
    private lazy val built: T = {
      def dfs(node: RasPath.PathNode[T]): T = {
        ras.withNewChildren(node.self().self(), node.children().map(c => dfs(c)))
      }
      dfs(node)
    }

    override def plan(): T = built

    override def toString: String = s"RasPathImpl(${ras.explain.describeNode(built)})"
  }
}

trait InClusterPath[T <: AnyRef] {
  def cluster(): RasClusterKey
  def path(): RasPath[T]
}

object InClusterPath {
  def apply[T <: AnyRef](cluster: RasClusterKey, path: RasPath[T]): InClusterPath[T] = {
    new InClusterPathImpl(cluster, path)
  }

  private class InClusterPathImpl[T <: AnyRef](
      override val cluster: RasClusterKey,
      override val path: RasPath[T])
    extends InClusterPath[T]
}
