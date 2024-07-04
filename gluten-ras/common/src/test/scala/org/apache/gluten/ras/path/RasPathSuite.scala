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

import org.apache.gluten.ras.{CanonicalNode, Ras}
import org.apache.gluten.ras.RasSuiteBase._
import org.apache.gluten.ras.mock.MockRasPath
import org.apache.gluten.ras.rule.RasRule

import org.scalatest.funsuite.AnyFunSuite

class RasPathSuite extends AnyFunSuite {
  import RasPathSuite._

  test("Cartesian product - empty") {
    val ras =
      Ras[TestNode](
        PlanModelImpl,
        CostModelImpl,
        MetadataModelImpl,
        PropertyModelImpl,
        ExplainImpl,
        RasRule.Factory.reuse(List.empty))
    assert(
      RasPath.cartesianProduct(
        ras,
        CanonicalNode(ras, Binary("b", ras.dummyGroupLeaf(), ras.dummyGroupLeaf())),
        List(
          List.empty,
          List(
            MockRasPath.mock(
              ras,
              Leaf("l", 1),
              PathKeySet(Set(DummyPathKey(3)))
            )))
      ) == List.empty)
  }
  test("Cartesian product") {
    val ras =
      Ras[TestNode](
        PlanModelImpl,
        CostModelImpl,
        MetadataModelImpl,
        PropertyModelImpl,
        ExplainImpl,
        RasRule.Factory.reuse(List.empty))

    val n1 = "n1"
    val n2 = "n2"
    val n3 = "n3"
    val n4 = "n4"
    val n5 = "n5"
    val n6 = "n6"

    val path1 = MockRasPath.mock(
      ras,
      Unary(n5, Leaf(n6, 1)),
      PathKeySet(Set(DummyPathKey(1), DummyPathKey(3)))
    )
    val path2 = MockRasPath.mock(
      ras,
      Unary(n1, Unary(n2, Leaf(n3, 1))),
      PathKeySet(Set(DummyPathKey(1)))
    )
    val path3 = MockRasPath.mock(
      ras,
      Leaf(n6, 1),
      PathKeySet(Set(DummyPathKey(1)))
    )
    val path4 = MockRasPath.mock(
      ras,
      Leaf(n3, 1),
      PathKeySet(Set(DummyPathKey(3)))
    )

    val path5 = MockRasPath.mock(
      ras,
      Unary(n2, Leaf(n3, 1)),
      PathKeySet(Set(DummyPathKey(4)))
    )

    val product = RasPath.cartesianProduct(
      ras,
      CanonicalNode(ras, Binary(n4, ras.dummyGroupLeaf(), ras.dummyGroupLeaf())),
      List(
        List(path1, path2),
        List(path3, path4, path5)
      ))

    val out = product.toList
    assert(out.size == 3)

    assert(
      out.map(_.plan()) == List(
        Binary(n4, Unary(n5, Leaf(n6, 1)), Leaf(n6, 1)),
        Binary(n4, Unary(n5, Leaf(n6, 1)), Leaf(n3, 1)),
        Binary(n4, Unary(n1, Unary(n2, Leaf(n3, 1))), Leaf(n6, 1))))
  }
}

object RasPathSuite {
  case class Leaf(name: String, override val selfCost: Long) extends LeafLike {
    override def makeCopy(): LeafLike = this
  }

  case class Unary(name: String, child: TestNode) extends UnaryLike {
    override def selfCost(): Long = 1
    override def withNewChildren(child: TestNode): UnaryLike = copy(child = child)
  }

  case class Binary(name: String, left: TestNode, right: TestNode) extends BinaryLike {
    override def selfCost(): Long = 1
    override def withNewChildren(left: TestNode, right: TestNode): BinaryLike =
      copy(left = left, right = right)
  }

  case class DummyPathKey(value: Int) extends PathKey
}
