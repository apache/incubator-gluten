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
package org.apache.gluten.extension

import org.apache.gluten.GlutenConfig
import org.apache.gluten.backend.Backend
import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.expression.TransformerState
import org.apache.gluten.extension.columnar.transition.Convention
import org.apache.gluten.logging.LogLevelUtil
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.plan.PlanBuilder
import org.apache.gluten.substrait.rel.RelNode
import org.apache.gluten.test.TestStats

import org.apache.spark.sql.execution.SparkPlan

import com.google.common.collect.Lists

sealed trait ValidationResult {
  def ok(): Boolean
  def reason(): String
}

object ValidationResult {
  private case object Succeeded extends ValidationResult {
    override def ok(): Boolean = true
    override def reason(): String = throw new UnsupportedOperationException(
      "Succeeded validation doesn't have failure details")
  }

  private case class Failed(override val reason: String) extends ValidationResult {
    override def ok(): Boolean = false
  }

  def succeeded: ValidationResult = Succeeded
  def failed(reason: String): ValidationResult = Failed(reason)
}

/** Every Gluten Operator should extend this trait. */
trait GlutenPlan extends SparkPlan with Convention.KnownBatchType with LogLevelUtil {
  protected lazy val enableNativeValidation = glutenConf.enableNativeValidation

  protected def glutenConf: GlutenConfig = GlutenConfig.getConf

  /**
   * Validate whether this SparkPlan supports to be transformed into substrait node in Native Code.
   */
  final def doValidate(): ValidationResult = {
    val schemaVaidationResult = BackendsApiManager.getValidatorApiInstance
      .doSchemaValidate(schema)
      .map {
        reason =>
          ValidationResult.failed(s"Found schema check failure for $schema, due to: $reason")
      }
      .getOrElse(ValidationResult.succeeded)
    if (!schemaVaidationResult.ok()) {
      TestStats.addFallBackClassName(this.getClass.toString)
      return schemaVaidationResult
    }
    try {
      TransformerState.enterValidation
      val res = doValidateInternal()
      if (!res.ok()) {
        TestStats.addFallBackClassName(this.getClass.toString)
      }
      res
    } catch {
      case e @ (_: GlutenNotSupportException | _: UnsupportedOperationException) =>
        if (!e.isInstanceOf[GlutenNotSupportException]) {
          logDebug(s"Just a warning. This exception perhaps needs to be fixed.", e)
        }
        // FIXME: Use a validation-specific method to catch validation failures
        TestStats.addFallBackClassName(this.getClass.toString)
        logValidationMessage(
          s"Validation failed with exception for plan: $nodeName, due to: ${e.getMessage}",
          e)
        ValidationResult.failed(e.getMessage)
    } finally {
      TransformerState.finishValidation
    }
  }

  final override def batchType(): Convention.BatchType = {
    if (!supportsColumnar) {
      return Convention.BatchType.None
    }
    val batchType = batchType0()
    assert(batchType != Convention.BatchType.None)
    batchType
  }

  protected def batchType0(): Convention.BatchType = {
    Backend.get().defaultBatchType
  }

  protected def doValidateInternal(): ValidationResult = ValidationResult.succeeded

  protected def doNativeValidation(context: SubstraitContext, node: RelNode): ValidationResult = {
    if (node != null && glutenConf.enableNativeValidation) {
      val planNode = PlanBuilder.makePlan(context, Lists.newArrayList(node))
      BackendsApiManager.getValidatorApiInstance
        .doNativeValidateWithFailureReason(planNode)
    } else {
      ValidationResult.succeeded
    }
  }

  private def logValidationMessage(msg: => String, e: Throwable): Unit = {
    if (glutenConf.printStackOnValidationFailure) {
      logOnLevel(glutenConf.validationLogLevel, msg, e)
    } else {
      logOnLevel(glutenConf.validationLogLevel, msg)
    }
  }
}
