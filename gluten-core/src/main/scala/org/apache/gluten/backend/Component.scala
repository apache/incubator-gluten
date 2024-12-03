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

package org.apache.gluten.backend

import org.apache.gluten.extension.columnar.transition.ConventionFunc
import org.apache.gluten.extension.injector.Injector

import org.apache.spark.SparkContext
import org.apache.spark.annotation.Experimental
import org.apache.spark.api.plugin.PluginContext

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable
import scala.reflect.ClassTag

/**
 * The base API to inject user-defined logic to Gluten. To register a component, its implementations
 * should be placed to Gluten's classpath with a Java service file. Gluten will discover all the
 * component implementations then register them at the booting time.
 *
 * Experimental: This is not expected to be used in production yet. Use [[Backend]] instead.
 */
@Experimental
trait Component {
  import Component._

  private val uid = nextUid.getAndIncrement()

  graph.add(this)

  final protected[this] def require[T <: Component: ClassTag](): Unit = {
    graph.declareRequirement(
      this,
      implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[_ <: Component]])
  }

  /** Base information. */
  def name(): String
  def buildInfo(): BuildInfo

  /** Spark listeners. */
  def onDriverStart(sc: SparkContext, pc: PluginContext): Unit = {}
  def onDriverShutdown(): Unit = {}
  def onExecutorStart(pc: PluginContext): Unit = {}
  def onExecutorShutdown(): Unit = {}

  /**
   * Overrides [[org.apache.gluten.extension.columnar.transition.ConventionFunc]] Gluten is using to
   * determine the convention (its row-based processing / columnar-batch processing support) of a
   * plan with a user-defined function that accepts a plan then returns convention type it outputs,
   * and input conventions it requires.
   */
  def convFuncOverride(): ConventionFunc.Override = ConventionFunc.Override.Empty

  /** Query planner rules. */
  def injectRules(injector: Injector): Unit
}

object Component {
  private val nextUid = new AtomicInteger()

  val graph = new Graph()

  private class Registry {
    private val lookupByUid: mutable.Map[Int, Component] = mutable.Map()
    private val lookupByClass: mutable.Map[Class[_ <: Component], Component] = mutable.Map()

    def register(comp: Component): Unit = synchronized {
      val uid = comp.uid
      val clazz = comp.getClass
      require(!lookupByUid.contains(uid))
      require(!lookupByClass.contains(clazz))
      lookupByUid += uid -> comp
      lookupByClass += clazz -> comp
    }

    def isRegistered(comp: Component): Boolean = synchronized {
      val out = lookupByUid.contains(comp.uid)
      assert(out == lookupByClass.contains(comp.getClass))
      out
    }

    def findByClass(clazz: Class[_ <: Component]): Component = synchronized {
      require(lookupByClass.contains(clazz))
      lookupByClass(clazz)
    }

    def findById(id: Int): Component = synchronized {
      require(lookupByUid.contains(id))
      lookupByUid(id)
    }
  }

  class Graph private[Component] {
    import Graph._
    private val registry: Registry = new Registry()
    private val nodes: mutable.Buffer[Node] = mutable.Buffer()
    private val lookup: mutable.Map[Int, Node] = mutable.Map()

    private[Component] def add(comp: Component): Unit = synchronized {
      require(!registry.isRegistered(comp))
      registry.register(comp)
      val uid = comp.uid
      require(!lookup.contains(uid))
      val n = new Node(uid)
      lookup += uid -> n
      nodes += n
    }

    private[Component] def declareRequirement(
        comp: Component,
        requiredCompClass: Class[_ <: Component]): Unit =
      synchronized {
        val uid = comp.uid
        val requiredUid = registry.findByClass(requiredCompClass).uid
        require(uid != requiredUid)
        require(lookup.contains(uid))
        require(lookup.contains(requiredUid))
        val n = lookup(uid)
        val r = lookup(requiredUid)
        require(!n.parent.contains(r.uid))
        require(!r.children.contains(n.uid))
        n.parent += r.uid -> r
        r.children += n.uid -> n
      }

    // format: off
    /**
     * Run topology sort on all registered components in graph to get an ordered list of components.
     * The root nodes will be on the head side of the list, while leaf nodes will be on the tail
     * side of the list.
     *
     * Say if component-A requires component-B while component-C requires nothing, then the output
     * order will be one of the following:
     *
     *   1. [component-B, component-A, component-C]
     *   2. [component-C, component-B, component-A]
     *   3. [component-B, component-C, component-A]
     *
     * By all means component B will be placed before component A because of the declared
     * requirement from component A to component B.
     */
    // format: on
    def sort(): Seq[Component] = {
      ???
    }
  }

  private object Graph {
    class Node(val uid: Int) {
      val parent: mutable.Map[Int, Node] = mutable.Map()
      val children: mutable.Map[Int, Node] = mutable.Map()
    }
  }

  case class BuildInfo(name: String, branch: String, revision: String, revisionTime: String)
}
