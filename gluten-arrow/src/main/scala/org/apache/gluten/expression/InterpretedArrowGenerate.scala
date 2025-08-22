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
package org.apache.gluten.expression

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, ExpressionsEvaluator, Generator}
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReferences

/**
 * A [[InterpretedArrowGenerate]] that is calculated by calling `eval` on the specified generate.
 * Because `HiveGenericUDTF` uses `ArrayBuffer` to store results and the actual return type of
 * `eval` method of `HiveGenericUDTF` is `Seq` , so `eval` and `terminate` just returns `Seq`.
 *
 * @param generate
 *   an expression that determine the value of each column of the output row.
 */
case class InterpretedArrowGenerate(generator: Generator)
  extends (InternalRow => Option[Seq[InternalRow]])
  with ExpressionsEvaluator {
  def this(generator: Generator, inputSchema: Seq[Attribute]) =
    this(bindReferences(Seq(generator), inputSchema).head)

  override def apply(input: InternalRow): Option[Seq[InternalRow]] = {
    val resultRows = generator.eval(input)
    if (resultRows.isEmpty) {
      None
    } else {
      Some(resultRows.asInstanceOf[Seq[InternalRow]])
    }
  }

  def terminate(): Option[Seq[InternalRow]] = {
    val resultRows = generator.terminate()
    if (resultRows.isEmpty) {
      None
    } else {
      Some(resultRows.asInstanceOf[Seq[InternalRow]])
    }
  }
}

object InterpretedArrowGenerate {
  def create(generator: Generator): InterpretedArrowGenerate = {
    new InterpretedArrowGenerate(generator)
  }
}
