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
package org.apache.gluten.ras.mock

import org.apache.gluten.ras.{CanonicalNode, Ras}
import org.apache.gluten.ras.memo.Memo
import org.apache.gluten.ras.path.{PathKeySet, RasPath}

object MockRasPath {
  def mock[T <: AnyRef](ras: Ras[T], node: T): RasPath[T] = {
    mock(ras, node, PathKeySet.trivial)
  }

  def mock[T <: AnyRef](ras: Ras[T], node: T, keys: PathKeySet): RasPath[T] = {
    val memo = Memo(ras)
    val g = memo.memorize(node, ras.userConstraintSet())
    val state = memo.newState()
    val groupSupplier = state.asGroupSupplier()
    assert(g.nodes(state).size == 1)
    val can = g.nodes(state).head

    def dfs(n: CanonicalNode[T]): RasPath[T] = {
      if (n.isLeaf()) {
        return RasPath.one(ras, keys, groupSupplier, n)
      }
      RasPath(
        ras,
        n,
        n.getChildrenGroups(groupSupplier).map(_.group(groupSupplier)).map {
          cg =>
            assert(cg.nodes(state).size == 1)
            dfs(cg.nodes(state).head)
        }).get
    }
    dfs(can)
  }
}
