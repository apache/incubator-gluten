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
package io.substrait.utils

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

object Util {

  /**
   * Compute the cartesian product for n lists.
   *
   * <p>Based on <a
   * href="https://thomas.preissler.me/blog/2020/12/29/permutations-using-java-streams">Soln by
   * Thomas Preissler</a></a>
   */
  def crossProduct[T](lists: Seq[Seq[T]]): Seq[Seq[T]] = {

    /** list [a, b], element 1 =>  list + element => [a, b, 1] */
    val appendElementToList: (Seq[T], T) => Seq[T] =
      (list, element) => list :+ element

    /** ([a, b], [1, 2]) ==> [a, b, 1], [a, b, 2] */
    val appendAndGen: (Seq[T], Seq[T]) => Seq[Seq[T]] =
      (list, elemsToAppend) => elemsToAppend.map(e => appendElementToList(list, e))

    val firstListToJoin = lists.head
    val startProduct = appendAndGen(new ArrayBuffer[T], firstListToJoin)

    /** ([ [a, b], [c, d] ], [1, 2]) -> [a, b, 1], [a, b, 2], [c, d, 1], [c, d, 2] */
    val appendAndGenLists: (Seq[Seq[T]], Seq[T]) => Seq[Seq[T]] =
      (products, toJoin) => products.flatMap(product => appendAndGen(product, toJoin))
    lists.tail.foldLeft(startProduct)(appendAndGenLists)
  }

  def seqToOption[T](s: Seq[Option[T]]): Option[Seq[T]] = {
    @tailrec
    def seqToOptionHelper(s: Seq[Option[T]], accum: Seq[T] = Seq[T]()): Option[Seq[T]] = {
      s match {
        case Some(head) :: Nil =>
          Option(accum :+ head)
        case Some(head) :: tail =>
          seqToOptionHelper(tail, accum :+ head)
        case _ => None
      }
    }
    seqToOptionHelper(s)
  }

}
