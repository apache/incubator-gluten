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
package io.glutenproject.extension

import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.expression.TransformerState
import io.glutenproject.substrait.SubstraitContext
import io.glutenproject.substrait.plan.PlanBuilder
import io.glutenproject.substrait.rel.RelNode
import io.glutenproject.test.TestStats
import io.glutenproject.utils.LogLevelUtil
import io.glutenproject.validate.NativePlanValidationInfo

import org.apache.spark.sql.execution.SparkPlan

import com.google.common.collect.Lists

import scala.collection.JavaConverters._

case class ValidationResult(isValid: Boolean, reason: Option[String])

object ValidationResult {
  def ok: ValidationResult = ValidationResult(isValid = true, None)
  def notOk(reason: String): ValidationResult = ValidationResult(isValid = false, Option(reason))
  def convertFromValidationInfo(info: NativePlanValidationInfo): ValidationResult = {
    if (info.isSupported) {
      ok
    } else {
      val fallbackInfo = info.getFallbackInfo.asScala
        .mkString("native check failure:", ", ", "")
      notOk(fallbackInfo)
    }
  }

  /**
   * Merge the results of two ValidationResult objects, including combining the reasons message for
   * invalid ValidationResult.
   *   - valid merge valid = valid
   *   - invalid merge valid = invalid
   *   - invalid merge invalid = invalid
   */
  def merge(first: ValidationResult, second: ValidationResult): ValidationResult = {
    if (first.isValid && second.isValid) {
      ok
    } else {
      val reasonStr = first.reason.getOrElse("") + second.reason.getOrElse("")
      notOk(reasonStr)
    }
  }
}

/** Every Gluten Operator should extend this trait. */
trait GlutenPlan extends SparkPlan with LogLevelUtil {

  private lazy val validationLogLevel = glutenConf.validationLogLevel
  private lazy val printStackOnValidationFailure = glutenConf.printStackOnValidationFailure
  protected lazy val enableNativeValidation = glutenConf.enableNativeValidation

  protected def glutenConf: GlutenConfig = GlutenConfig.getConf

  /**
   * Validate whether this SparkPlan supports to be transformed into substrait node in Native Code.
   */
  final def doValidate(): ValidationResult = {
    try {
      TransformerState.enterValidation
      val res = doValidateInternal()
      if (!res.isValid) {
        TestStats.addFallBackClassName(this.getClass.toString)
      }
      res
    } catch {
      case e: Exception =>
        // FIXME: Use a validation-specific method to catch validation failures
        TestStats.addFallBackClassName(this.getClass.toString)
        logValidationMessage(s"Validation failed with exception for plan: $nodeName, due to:", e)
        ValidationResult.notOk(e.getMessage)
    } finally {
      TransformerState.finishValidation
    }
  }

  protected def doValidateInternal(): ValidationResult = ValidationResult.ok

  protected def doNativeValidation(context: SubstraitContext, node: RelNode): ValidationResult = {
    if (node != null && enableNativeValidation) {
      val planNode = PlanBuilder.makePlan(context, Lists.newArrayList(node))
      val info = BackendsApiManager.getValidatorApiInstance
        .doNativeValidateWithFailureReason(planNode)
      ValidationResult.convertFromValidationInfo(info)
    } else {
      ValidationResult.ok
    }
  }

  private def logValidationMessage(msg: => String, e: Throwable): Unit = {
    if (printStackOnValidationFailure) {
      logOnLevel(validationLogLevel, msg, e)
    } else {
      logOnLevel(validationLogLevel, msg)
    }
  }
}
