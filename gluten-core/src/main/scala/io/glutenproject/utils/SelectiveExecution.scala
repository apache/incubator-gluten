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

package io.glutenproject.utils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.SparkSession
import java.lang.Throwable
import scala.util.Try
import scala.collection.immutable.List

object SelectiveExecution extends Logging{
    var defaultEnabled = true
    val CONF_OPTION = "gluten_enabled"

    private[this] def stackTrace(max: Int = 1000): String = {
        val trim: Int = 6
        new Throwable().fillInStackTrace().getStackTrace().slice(trim, trim + max).mkString("\n")
    }

    private[this] def shouldUseGluten(session: SparkSession, plan: QueryPlan[_]): Boolean = {
        if (log.isDebugEnabled) {
            logDebug(s"=========================\n" +
              s"running shloudUseGluten from:\n${stackTrace()}\n" +
              s"plan:\n${plan.treeString}\n" +
              "=========================")
        }
        val conf: Option[String] = session.conf.getOption(CONF_OPTION)
        val ret = conf.flatMap((x: String) => Try(x.toBoolean).toOption).getOrElse(defaultEnabled)
        logInfo(s"shouldUseGluten: $ret")
        ret
    }

    def maybe[T <: QueryPlan[_]](session: SparkSession, plan: T) (func: => T): T = {
      if (shouldUseGluten(session, plan)) func else plan
    }

    def maybeNil[T <: QueryPlan[_]](session: SparkSession, plan: QueryPlan[_])
      (func: => Seq[T]): Seq[T] = {
      if (shouldUseGluten(session, plan)) func else Nil
    }

}
