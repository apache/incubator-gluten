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

import org.apache.gluten.ras.{CanonicalNode, GroupNode}
import org.apache.gluten.ras.path.FilterWizard.{FilterAction, FilterAdvanceAction}
import org.apache.gluten.ras.path.OutputWizard.{AdvanceAction, OutputAction}
import org.apache.gluten.ras.util.CycleDetector

trait FilterWizard[T <: AnyRef] {
  import FilterWizard._
  def omit(can: CanonicalNode[T]): FilterAction[T]
  def omit(group: GroupNode[T]): FilterAction[T]
  def advance(offset: Int, count: Int): FilterAdvanceAction[T]
}

object FilterWizard {
  sealed trait FilterAction[T <: AnyRef]
  object FilterAction {
    case class Omit[T <: AnyRef] private () extends FilterAction[T]
    object Omit {
      val INSTANCE: Omit[Null] = Omit[Null]()
      // Enclose default constructor.
      private def apply[T <: AnyRef](): Omit[T] = new Omit()
    }
    def omit[T <: AnyRef]: Omit[T] = Omit.INSTANCE.asInstanceOf[Omit[T]]

    case class Continue[T <: AnyRef](newWizard: FilterWizard[T]) extends FilterAction[T]
  }

  sealed trait FilterAdvanceAction[T <: AnyRef]
  object FilterAdvanceAction {
    case class Continue[T <: AnyRef](newWizard: FilterWizard[T]) extends FilterAdvanceAction[T]
  }
}

object FilterWizards {
  def omitNodeCycles[T <: AnyRef](): FilterWizard[T] = {
    // Compares against group ID to identify cycles.
    OmitCycles.onNodes(CycleDetector((one, other) => one.toHashKey == other.toHashKey))
  }

  def omitGroupCycles[T <: AnyRef](): FilterWizard[T] = {
    // Compares against group ID to identify cycles.
    OmitCycles.onGroups(CycleDetector((one, other) => one.groupId() == other.groupId()))
  }

  def none[T <: AnyRef](): FilterWizard[T] = {
    None[T]()
  }

  private class None[T <: AnyRef] private () extends FilterWizard[T] {
    override def omit(can: CanonicalNode[T]): FilterAction[T] = FilterAction.Continue(this)
    override def omit(group: GroupNode[T]): FilterAction[T] = FilterAction.Continue(this)
    override def advance(offset: Int, count: Int): FilterAdvanceAction[T] =
      FilterAdvanceAction.Continue(this)
  }

  private object None {
    def apply[T <: AnyRef](): None[T] = new None[T]()
  }

  // Cycle detection starts from the first visited group in the input path.
  private class OmitCycles[T <: AnyRef] private (
      detectorOnNodes: CycleDetector[CanonicalNode[T]],
      detectorOnGroups: CycleDetector[GroupNode[T]])
    extends FilterWizard[T] {
    override def omit(can: CanonicalNode[T]): FilterAction[T] = {
      if (detectorOnNodes.contains(can)) {
        return FilterAction.omit
      }
      FilterAction.Continue(new OmitCycles(detectorOnNodes.append(can), detectorOnGroups))
    }

    override def omit(group: GroupNode[T]): FilterAction[T] = {
      if (detectorOnGroups.contains(group)) {
        return FilterAction.omit
      }
      FilterAction.Continue(new OmitCycles(detectorOnNodes, detectorOnGroups.append(group)))
    }

    override def advance(offset: Int, count: Int): FilterAdvanceAction[T] =
      FilterAdvanceAction.Continue(this)
  }

  private object OmitCycles {
    def onNodes[T <: AnyRef](detector: CycleDetector[CanonicalNode[T]]): OmitCycles[T] = {
      new OmitCycles(detector, CycleDetector.noop())
    }

    def onGroups[T <: AnyRef](detector: CycleDetector[GroupNode[T]]): OmitCycles[T] = {
      new OmitCycles(CycleDetector.noop(), detector)
    }
  }
}

object OutputFilter {
  def apply[T <: AnyRef](
      outputWizard: OutputWizard[T],
      filterWizard: FilterWizard[T]): OutputWizard[T] = {
    new OutputFilterImpl[T](outputWizard, filterWizard)
  }

  // Composite wizard works within "and" basis, to filter out
  // the unwanted emitted paths from a certain specified output wizard
  // by another filter wizard.
  private class OutputFilterImpl[T <: AnyRef](
      outputWizard: OutputWizard[T],
      filterWizard: FilterWizard[T])
    extends OutputWizard[T] {

    override def visit(can: CanonicalNode[T]): OutputAction[T] = {
      filterWizard.omit(can) match {
        case FilterAction.Omit() => OutputAction.stop
        case FilterAction.Continue(newFilterWizard) =>
          outputWizard.visit(can) match {
            case stop @ OutputAction.Stop(_) =>
              stop
            case OutputAction.Continue(drain, newOutputWizard) =>
              OutputAction.Continue(drain, new OutputFilterImpl(newOutputWizard, newFilterWizard))
          }
      }
    }

    override def visit(group: GroupNode[T]): OutputAction[T] = {
      filterWizard.omit(group: GroupNode[T]) match {
        case FilterAction.Omit() => OutputAction.stop
        case FilterAction.Continue(newFilterWizard) =>
          outputWizard.visit(group) match {
            case stop @ OutputAction.Stop(_) => stop
            case OutputAction.Continue(drain, newOutputWizard) =>
              OutputAction.Continue(drain, new OutputFilterImpl(newOutputWizard, newFilterWizard))
          }
      }
    }

    override def advance(offset: Int, count: Int): OutputWizard.AdvanceAction[T] = {
      val newOutputWizard = outputWizard.advance(offset, count) match {
        case AdvanceAction.Continue(newWizard) => newWizard
      }
      val newFilterWizard = filterWizard.advance(offset, count) match {
        case FilterAdvanceAction.Continue(newWizard) => newWizard
      }
      AdvanceAction.Continue(new OutputFilterImpl(newOutputWizard, newFilterWizard))
    }

    override def withPathKey(newKey: PathKey): OutputWizard[T] =
      new OutputFilterImpl[T](outputWizard.withPathKey(newKey), filterWizard)
  }
}
