package org.apache.gluten.utils.iterator

import org.apache.gluten.utils.iterator.Iterators.WrapperBuilder

import org.apache.spark.TaskContext
import org.apache.spark.util.TaskResources

import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable

object IteratorsV2 {
  private type HasNext = Option[Boolean]
  private type Next[A] = Option[A]

  private trait Listener[A] {
    def beforeHasNext(value: HasNext): HasNext
    def afterHasNext(value: Boolean): Unit
    def beforeNext(value: Next[A]): Next[A]
    def afterNext(value: A): Unit
  }

  private class ListenableIterator[A](in: Iterator[A], listeners: Seq[Listener[A]])
    extends Iterator[A] {
    private val listenerArray = listeners.toList
    private val reversedListenerArray = listeners.reverse.toList

    override def hasNext: Boolean = {
      var hasNext: HasNext = None
      // Avoid using foldLeft / foldRight for performance consideration.
      reversedListenerArray.foreach {
        listener =>
          hasNext = listener.beforeHasNext(hasNext)
      }
      val value = hasNext match {
        case Some(value) => value
        case None => in.hasNext
      }
      listenerArray.foreach(listener => listener.afterHasNext(value))
      value
    }

    override def next(): A = {
      var next: Next[A] = None
      // Avoid using foldLeft / foldRight for performance consideration.
      reversedListenerArray.foreach {
        listener =>
          next = listener.beforeNext(next)
      }
      val value = next match {
        case Some(value) => value
        case None => in.next()
      }
      listenerArray.foreach(listener => listener.afterNext(value))
      value
    }
  }

  private class ReadTimeAccumulator[A](onAdded: Long => Unit) extends Listener[A] {
    private var prev: Long = Long.MinValue

    override def beforeHasNext(value: HasNext): HasNext = {
      prev = System.currentTimeMillis()
      value
    }
    override def afterHasNext(value: Boolean): Unit = {
      val duration = System.currentTimeMillis() - prev
      onAdded(duration)
    }
    override def beforeNext(value: Next[A]): Next[A] = {
      prev = System.currentTimeMillis()
      value
    }
    override def afterNext(value: A): Unit = {
      val duration = System.currentTimeMillis() - prev
      onAdded(duration)
    }
  }

  private class IteratorCompleter[A](completionCallback: => Unit) extends Listener[A] {
    private val completed = new AtomicBoolean(false)

    TaskResources.addRecycler("Iterators#IteratorRecycler", 100) {
      tryComplete()
    }

    override def beforeHasNext(value: HasNext): HasNext = value
    override def afterHasNext(value: Boolean): Unit = {
      if (!value) {
        tryComplete()
      }
    }
    override def beforeNext(value: Next[A]): Next[A] = value
    override def afterNext(value: A): Unit = {}

    private def tryComplete(): Unit = {
      if (!completed.compareAndSet(false, true)) {
        return // make sure the iterator is completed once
      }
      completionCallback
    }
  }

  class WrapperBuilderV2[A] private[iterator] (in: Iterator[A]) extends WrapperBuilder[A] {
    private val listeners: mutable.ListBuffer[Listener[A]] = mutable.ListBuffer()

    override def recyclePayload(closeCallback: A => Unit): WrapperBuilder[A] = ???
    override def recycleIterator(completionCallback: => Unit): WrapperBuilder[A] = {
      listeners += new IteratorCompleter[A](completionCallback)
      this
    }

    override def collectLifeMillis(onCollected: Long => Unit): WrapperBuilder[A] = ???
    override def collectReadMillis(onAdded: Long => Unit): WrapperBuilder[A] = {
      listeners += new ReadTimeAccumulator[A](onAdded)
      this
    }
    override def asInterruptible(context: TaskContext): WrapperBuilder[A] = ???
    override def protectInvocationFlow(): WrapperBuilder[A] = ???

    override def create(): Iterator[A] = {
      if (listeners.isEmpty) {
        return in
      }
      new ListenableIterator[A](in, listeners)
    }
  }
}
