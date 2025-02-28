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

import org.apache.gluten.extension.columnar.FallbackTag
import org.apache.gluten.extension.columnar.FallbackTag.{Appendable, Converter}
import org.apache.gluten.extension.columnar.FallbackTags.add
import org.apache.gluten.extension.columnar.validator.Validator

import org.apache.spark.sql.catalyst.trees.TreeNode

sealed trait ValidationResult {
  def ok(): Boolean
  def reason(): String
}

object ValidationResult {
  implicit object FromValidationResult extends Converter[ValidationResult] {
    override def from(result: ValidationResult): Option[FallbackTag] = {
      if (result.ok()) {
        return None
      }
      Some(Appendable(result.reason()))
    }
  }

  private case object Succeeded extends ValidationResult {
    override def ok(): Boolean = true
    override def reason(): String = throw new UnsupportedOperationException(
      "Succeeded validation doesn't have failure details")
  }

  private case class Failed(override val reason: String) extends ValidationResult {
    override def ok(): Boolean = false
  }

  def succeeded: ValidationResult = Succeeded
  def failed(reason: String, prefix: String = "\n - "): ValidationResult = Failed(prefix + reason)
  def merge(left: ValidationResult, right: ValidationResult): ValidationResult =
    (left.ok(), right.ok()) match {
      case (_, true) =>
        left
      case (true, false) =>
        right
      case (false, false) =>
        failed(left.reason() + right.reason(), prefix = "")
    }

  implicit class EncodeFallbackTagImplicits(result: ValidationResult) {
    def tagOnFallback(plan: TreeNode[_]): Unit = {
      if (result.ok()) {
        return
      }
      add(plan, result)
    }

    def toValidatorOutcome(): Validator.OutCome = {
      if (result.ok()) {
        return Validator.Passed
      }
      Validator.Failed(result.reason())
    }
  }
}
