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
package org.apache.gluten.qt.execution

import org.apache.gluten.qt.QualificationToolConfiguration
import org.apache.gluten.qt.writer.WriteUtils

import Executor.hasUpdate

import java.io.{FileWriter, PrintWriter}
import java.util.concurrent.{ConcurrentHashMap, PriorityBlockingQueue, ThreadFactory, ThreadPoolExecutor, TimeUnit}
import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConverters._

/**
 * Provides a thread-based execution mechanism using a pool of worker threads to process tasks with
 * assigned priorities. The entry point is the {@code Executor} class, which manages a
 * {@link ThreadPoolExecutor} along with counters for submitted, finished, and failed tasks. Tasks
 * are submitted as {@link PriorityTask} instances, each containing a {@link TaskContext} that
 * includes its priority and task type. <p> The {@code Executor} displays progress information in a
 * loop until all threads become {@code IDLE} or the pool is shut down. It also handles error
 * logging and can be run in verbose mode to provide real-time updates. <p> The key components are:
 * <ul> <li>{@code Executor} - Creates and manages a thread pool to execute {@link PriorityTask}
 * s.</li> <li>{@code PriorityTask} - Wraps a runnable task with a priority, allowing
 * higher-priority tasks to be processed first.</li> <li>{@code TaskContext} - Contains metadata for
 * a task (priority, name, and type).</li> <li>{@code RunState} - Represents the state of a thread,
 * such as "STARTED" or "IDLE," and can also track progress.</li> <li>{@code Progress} - Holds the
 * current and end values for task progress monitoring.</li> </ul>
 */
case class Executor(conf: QualificationToolConfiguration) extends ThreadFactory {
  private val corePoolSize = conf.threads
  private val maxPoolSize = conf.threads
  private val keepAliveTime = 1L
  private val queue = new PriorityBlockingQueue[Runnable]()

  private val executor =
    new ThreadPoolExecutor(corePoolSize, maxPoolSize, keepAliveTime, TimeUnit.SECONDS, queue, this)

  private val submittedCounter = new AtomicLong(0L)
  private val finishedCounter = new AtomicLong(0L)
  private val failedCounter = new AtomicLong(0L)

  private val errorPath = conf.outputPath.resolve(s"qt.stderr")
  WriteUtils.deleteFileIfPresent(errorPath.toAbsolutePath.toString)
  private lazy val errorWriter = new PrintWriter(
    new FileWriter(errorPath.toAbsolutePath.toString, true))
  private def addError(ctx: TaskContext, e: Throwable): Unit = errorWriter.synchronized {
    errorWriter.println(ctx.name)
    e.printStackTrace(errorWriter)
    errorWriter.println()
    errorWriter.flush()
  }

  def submitTask(task: PriorityTask): Unit = {
    if (task.ctx.taskType == EVENT_FILE_PROCESS_TASK) {
      submittedCounter.incrementAndGet()
    }
    val taskWithStatus = task.copy(task = () => {
      Executor.updateState(RunState("STARTED"))
      try {
        task.task()
        if (task.ctx.taskType == EVENT_FILE_PROCESS_TASK) {
          finishedCounter.incrementAndGet()
        }
      } catch {
        case e: Throwable =>
          addError(task.ctx, e)
          if (task.ctx.taskType == EVENT_FILE_PROCESS_TASK) {
            failedCounter.incrementAndGet()
          }
      }
      Executor.updateState(RunState("IDLE"))
    })
    executor.execute(taskWithStatus)
  }

  override def newThread(r: Runnable): Thread = {
    val thread = new Thread(r)
    Executor.addThread(thread)
    thread
  }

  def waitAndDisplayStatus(): Unit = {
    var states = Executor.getStates

    def moveCursorUp(n: Int): Unit = {
      printf(s"\u001b[${n}A")
    }

    def clearLine(): Unit = {
      printf("\u001b[2K")
    }

    def displayState(): Unit = {
      val processing = submittedCounter.get() - (finishedCounter.get() + failedCounter.get())
      clearLine()
      println(
        s"Processing $processing Processed $finishedCounter" +
          (if (failedCounter.get() == 0) "" else s" Failed or Skipped $failedCounter")
      )
      states.zipWithIndex.foreach {
        case (s, i) =>
          clearLine()
          println(s"T$i: $s")
      }
    }

    def displayStateAndClean(): Unit = {
      displayState()
      moveCursorUp(states.size + 1)
    }

    while (!states.forall(_ == RunState("IDLE"))) {
      if (conf.verbose && Executor.hasUpdate) {
        displayStateAndClean()
        hasUpdate = false
      }
      Thread.sleep(100)
      states = Executor.getStates
    }
    displayState()
    executor.shutdown()
    errorWriter.close()
  }
}

object Executor {
  private val _stateStore = new ConcurrentHashMap[Thread, RunState]()
  @volatile private var hasUpdate: Boolean = false
  private def stateStore: ConcurrentHashMap[Thread, RunState] = {
    hasUpdate = true
    _stateStore
  }

  def updateState(runState: RunState): Unit = {
    stateStore.compute(Thread.currentThread(), (_, _) => runState)
  }

  def incrementState(): Unit = {
    stateStore.compute(
      Thread.currentThread(),
      (_, s) => {
        s.progress.foreach(_.increment())
        s
      })
  }

  private def addThread(thread: Thread): Unit = {
    stateStore.put(thread, RunState("NEW"))
  }

  private def getStates: Seq[RunState] = _stateStore.values().asScala.toSeq
}

case class PriorityTask(ctx: TaskContext, task: () => Unit)
  extends Runnable
  with Comparable[PriorityTask] {
  override def run(): Unit = task()

  override def compareTo(other: PriorityTask): Int = {
    Integer.compare(
      other.ctx.priority,
      this.ctx.priority
    ) // Higher priority tasks are executed first
  }
}

sealed trait TaskType

object EVENT_FILE_PROCESS_TASK extends TaskType
object FILE_LISTING_TASK extends TaskType

case class TaskContext(priority: Int, name: String, taskType: TaskType)

case class RunState(state: String, progress: Option[Progress] = None) {
  override def toString: String = s"$state ${progress.map(_.toString).getOrElse("")}"
}

object RunState {
  def apply(state: String, end: Long): RunState = {
    RunState(state, Some(Progress(end)))
  }

  def apply(state: String, progress: Progress): RunState = {
    RunState(state, Some(progress))
  }
}

case class Progress(end: Option[Long]) {
  private var current = 0L

  def increment(): Unit = {
    current = current + 1
  }

  def update(newValue: Long): Unit = {
    current = newValue
  }

  override def toString: String = s"$current${end.map("/" + _).getOrElse("")}"
}

object Progress {
  def apply(end: Long): Progress = {
    Progress(Some(end))
  }

  def apply(): Progress = {
    Progress(None)
  }
}
