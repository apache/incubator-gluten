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
package org.apache.gluten.ras.rule

import org.apache.gluten.ras.Ras
import org.apache.gluten.ras.RasSuiteBase._
import org.apache.gluten.ras.mock.MockRasPath
import org.apache.gluten.ras.path.{Pattern, RasPath}

import org.scalatest.funsuite.AnyFunSuite

class PatternSuite extends AnyFunSuite {
  import PatternSuite._
  test("Match any") {
    val ras =
      Ras[TestNode](
        PlanModelImpl,
        CostModelImpl,
        MetadataModelImpl,
        PropertyModelImpl,
        ExplainImpl,
        RasRule.Factory.none())

    val pattern = Pattern.ignore[TestNode].build()
    val path = MockRasPath.mock(ras, Leaf("n1", 1))
    assert(path.height() == 1)

    assert(pattern.matches(path, 1))
  }

  test("Match ignore") {
    val ras =
      Ras[TestNode](
        PlanModelImpl,
        CostModelImpl,
        MetadataModelImpl,
        PropertyModelImpl,
        ExplainImpl,
        RasRule.Factory.none())

    val pattern = Pattern.ignore[TestNode].build()
    val path = MockRasPath.mock(ras, Leaf("n1", 1))
    assert(path.height() == 1)

    assert(pattern.matches(path, 1))
  }

  test("Match unary") {
    val ras =
      Ras[TestNode](
        PlanModelImpl,
        CostModelImpl,
        MetadataModelImpl,
        PropertyModelImpl,
        ExplainImpl,
        RasRule.Factory.none())

    val path = MockRasPath.mock(ras, Unary("n1", Leaf("n2", 1)))
    assert(path.height() == 2)

    val pattern1 = Pattern.branch[TestNode](n => n.isInstanceOf[Unary], Pattern.ignore).build()
    assert(pattern1.matches(path, 1))
    assert(pattern1.matches(path, 2))

    val pattern2 =
      Pattern.branch[TestNode](n => n.asInstanceOf[Unary].name == "foo", Pattern.ignore).build()
    assert(!pattern2.matches(path, 1))
    assert(!pattern2.matches(path, 2))
  }

  test("Match binary") {
    val ras =
      Ras[TestNode](
        PlanModelImpl,
        CostModelImpl,
        MetadataModelImpl,
        PropertyModelImpl,
        ExplainImpl,
        RasRule.Factory.none())

    val path = MockRasPath.mock(
      ras,
      Binary("n7", Unary("n1", Unary("n2", Leaf("n3", 1))), Unary("n5", Leaf("n6", 1))))
    assert(path.height() == 4)

    val pattern = Pattern
      .branch[TestNode](
        n => n.isInstanceOf[Binary],
        Pattern.branch(
          n => n.isInstanceOf[Unary],
          Pattern.branch(
            n => n.isInstanceOf[Unary],
            Pattern.ignore
          )
        ),
        Pattern.ignore)
      .build()
    assert(pattern.matches(path, 1))
    assert(pattern.matches(path, 2))
    assert(pattern.matches(path, 3))
    assert(pattern.matches(path, 4))
  }

  test("Matches above a certain depth") {
    val ras =
      Ras[TestNode](
        PlanModelImpl,
        CostModelImpl,
        MetadataModelImpl,
        PropertyModelImpl,
        ExplainImpl,
        RasRule.Factory.none())

    val path = MockRasPath.mock(
      ras,
      Binary("n7", Unary("n1", Unary("n2", Leaf("n3", 1))), Unary("n5", Leaf("n6", 1))))
    assert(path.height() == 4)

    val pattern1 = Pattern
      .branch[TestNode](
        n => n.isInstanceOf[Binary],
        Pattern.branch(
          n => n.isInstanceOf[Unary],
          Pattern.branch(
            n => n.isInstanceOf[Unary],
            Pattern.leaf(
              _.asInstanceOf[Leaf].name == "foo"
            )
          )
        ),
        Pattern.ignore
      )
      .build()

    assert(pattern1.matches(path, 1))
    assert(pattern1.matches(path, 2))
    assert(pattern1.matches(path, 3))
    assert(!pattern1.matches(path, 4))

    val pattern2 = Pattern
      .branch[TestNode](
        n => n.isInstanceOf[Binary],
        Pattern.branch(
          n => n.isInstanceOf[Unary],
          Pattern.branch(
            n => n.isInstanceOf[Unary],
            Pattern.branch(
              n => n.isInstanceOf[Unary],
              Pattern.ignore
            )
          )
        ),
        Pattern.ignore
      )
      .build()

    assert(pattern2.matches(path, 1))
    assert(pattern2.matches(path, 2))
    assert(pattern2.matches(path, 3))
    assert(!pattern2.matches(path, 4))
  }

  test("Match class") {
    val ras =
      Ras[TestNode](
        PlanModelImpl,
        CostModelImpl,
        MetadataModelImpl,
        PropertyModelImpl,
        ExplainImpl,
        RasRule.Factory.none())

    val path = MockRasPath.mock(ras, Unary("n1", Leaf("n2", 1)))
    assert(path.height() == 2)

    val pattern1 = Pattern
      .branch[TestNode](
        Pattern.Matchers.clazz(classOf[Unary]),
        Pattern.branch(Pattern.Matchers.clazz(classOf[Leaf])))
      .build()
    assert(pattern1.matches(path, 1))
    assert(pattern1.matches(path, 2))

    val pattern2 = Pattern
      .leaf[TestNode](Pattern.Matchers.clazz(classOf[Leaf]))
      .build()
    assert(!pattern2.matches(path, 1))
    assert(!pattern2.matches(path, 2))

    val pattern3 = Pattern
      .branch[TestNode](
        Pattern.Matchers
          .or(Pattern.Matchers.clazz(classOf[Unary]), Pattern.Matchers.clazz(classOf[Leaf])),
        Pattern.branch(Pattern.Matchers.clazz(classOf[Leaf]))
      )
      .build()
    assert(pattern3.matches(path, 1))
    assert(pattern3.matches(path, 2))

    val pattern4 = Pattern
      .branch[TestNode](
        Pattern.Matchers
          .or(Pattern.Matchers.clazz(classOf[Unary]), Pattern.Matchers.clazz(classOf[Leaf])),
        Pattern.branch(Pattern.Matchers
          .or(Pattern.Matchers.clazz(classOf[Unary]), Pattern.Matchers.clazz(classOf[Unary])))
      )
      .build()
    assert(pattern4.matches(path, 1))
    assert(!pattern4.matches(path, 2))
  }
}

object PatternSuite {
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

  case class DummyGroup() extends LeafLike {
    override def makeCopy(): LeafLike = throw new UnsupportedOperationException()
    override def selfCost(): Long = throw new UnsupportedOperationException()
  }

  implicit class PatternImplicits[T <: AnyRef](pattern: Pattern[T]) {
    def matchesAll(paths: Seq[RasPath[T]], depth: Int): Boolean = {
      paths.forall(pattern.matches(_, depth))
    }
  }
}
