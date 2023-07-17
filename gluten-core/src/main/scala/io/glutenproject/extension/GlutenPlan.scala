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

import scala.collection.JavaConverters._

import io.glutenproject.GlutenConfig
import io.glutenproject.expression.TransformerState
import io.glutenproject.test.TestStats
import io.glutenproject.utils.LogLevelUtil
import io.glutenproject.validate.NativePlanValidatorInfo
import org.apache.spark.sql.execution.SparkPlan

case class ValidationResult(
    validated: Boolean,
    reason: Option[String])

object ValidationResult {
  def ok: ValidationResult = ValidationResult(validated = true, None)
  def notOk(reason: String): ValidationResult = ValidationResult(validated = false, Some(reason))
}

/**
 * Every Gluten Operator should extend this trait.
 */
trait GlutenPlan extends SparkPlan with LogLevelUtil {

  private lazy val validateFailureLogLevel = glutenConf.validateFailureLogLevel
  private lazy val printStackOnValidateFailure = glutenConf.printStackOnValidateFailure

  protected def glutenConf: GlutenConfig = GlutenConfig.getConf

  /**
   * Validate whether this SparkPlan supports to be transformed into substrait node in Native Code.
   */
  final def doValidate(): ValidationResult = {
    try {
      TransformerState.enterValidation
      val res = doValidateInternal()
      if (!res.validated) {
        TestStats.addFallBackClassName(this.getClass.toString)
        // the reason must be set if failed to validate
        assert(res.reason.isDefined)
      }
      res
    } catch {
      case e: Throwable =>
        TestStats.addFallBackClassName(this.getClass.toString)
        logValidateFailure(s"Validation failed with exception for plan: $nodeName, due to:", e)
        notOk(e.getMessage)
    } finally {
      TransformerState.finishValidation
    }
  }

  protected def doValidateInternal(): ValidationResult = ValidationResult.ok

  protected def logValidateFailure(msg: => String, e: Throwable): Unit = {
    if (printStackOnValidateFailure) {
      logOnLevel(validateFailureLogLevel, msg, e)
    } else {
      logOnLevel(validateFailureLogLevel, msg)
    }
  }

  protected def ok(): ValidationResult = ValidationResult.ok
  protected def notOk(reason: String): ValidationResult = ValidationResult.notOk(reason)
  protected def nativeValidationResult(info: NativePlanValidatorInfo): ValidationResult = {
    if (info.isSupported) {
      ok()
    } else {
      val fallbackInfo = info.getFallbackInfo.asScala
        .mkString("native check failure:", ", ", "")
      notOk(fallbackInfo)
    }
  }
}
