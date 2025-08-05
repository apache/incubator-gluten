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
package org.apache.gluten.functions

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.ProjectExecTransformer

import org.apache.spark.SparkConf
import org.apache.spark.sql.internal.SQLConf

class ArithmeticAnsiValidateSuiteRasOff extends ArithmeticAnsiValidateSuite {
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(GlutenConfig.RAS_ENABLED.key, "false")
  }
}

class ArithmeticAnsiValidateSuiteRasOn extends ArithmeticAnsiValidateSuite {
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(GlutenConfig.RAS_ENABLED.key, "true")
  }
}

abstract class ArithmeticAnsiValidateSuite extends FunctionsValidateSuite {

  disableFallbackCheck

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(GlutenConfig.GLUTEN_ANSI_FALLBACK_ENABLED.key, "false")
      .set(SQLConf.ANSI_ENABLED.key, "true")
  }

  test("arithmetic addition with ansi mode - integer types") {
    runQueryAndCompare("SELECT int_field1 + 100 FROM datatab WHERE int_field1 IS NOT NULL") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("arithmetic subtraction with ansi mode - integer types") {
    runQueryAndCompare("SELECT int_field1 - 50 FROM datatab WHERE int_field1 IS NOT NULL") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("arithmetic multiplication with ansi mode - integer types") {
    runQueryAndCompare("SELECT int_field1 * 2 FROM datatab WHERE int_field1 IS NOT NULL") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("arithmetic division with ansi mode - integer types") {
    runQueryAndCompare("SELECT int_field1 / 2 FROM datatab WHERE int_field1 IS NOT NULL") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("arithmetic addition with ansi mode - long types") {
    runQueryAndCompare(
      "SELECT CAST(int_field1 AS BIGINT) + 1000L FROM datatab WHERE int_field1 IS NOT NULL") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("arithmetic subtraction with ansi mode - long types") {
    runQueryAndCompare(
      "SELECT CAST(int_field1 AS BIGINT) - 500L FROM datatab WHERE int_field1 IS NOT NULL") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("arithmetic multiplication with ansi mode - long types") {
    runQueryAndCompare(
      "SELECT CAST(int_field1 AS BIGINT) * 10L FROM datatab WHERE int_field1 IS NOT NULL") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("arithmetic division with ansi mode - long types") {
    runQueryAndCompare(
      "SELECT CAST(int_field1 AS BIGINT) / 2L FROM datatab WHERE int_field1 IS NOT NULL " +
        "LIMIT 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("arithmetic addition with ansi mode - short types") {
    runQueryAndCompare(
      "SELECT CAST(int_field1 AS SMALLINT) + CAST(10 AS SMALLINT) FROM datatab " +
        "WHERE int_field1 IS NOT NULL LIMIT 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("arithmetic subtraction with ansi mode - short types") {
    runQueryAndCompare(
      "SELECT CAST(int_field1 AS SMALLINT) - CAST(1 AS SMALLINT) FROM datatab " +
        "WHERE int_field1 IS NOT NULL LIMIT 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("arithmetic multiplication with ansi mode - short types") {
    runQueryAndCompare(
      "SELECT CAST(int_field1 AS SMALLINT) * CAST(2 AS SMALLINT) FROM datatab " +
        "WHERE int_field1 IS NOT NULL LIMIT 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("arithmetic division with ansi mode - short types") {
    runQueryAndCompare(
      "SELECT CAST(int_field1 AS SMALLINT) / CAST(2 AS SMALLINT) FROM datatab " +
        "WHERE int_field1 IS NOT NULL LIMIT 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("arithmetic addition with ansi mode - byte types") {
    runQueryAndCompare(
      "SELECT CAST(int_field1 AS TINYINT) + CAST(5 AS TINYINT) FROM datatab " +
        "WHERE int_field1 IS NOT NULL LIMIT 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("arithmetic subtraction with ansi mode - byte types") {
    runQueryAndCompare(
      "SELECT CAST(int_field1 AS TINYINT) - CAST(1 AS TINYINT) FROM datatab " +
        "WHERE int_field1 IS NOT NULL LIMIT 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("arithmetic multiplication with ansi mode - byte types") {
    runQueryAndCompare(
      "SELECT CAST(int_field1 AS TINYINT) * CAST(2 AS TINYINT) FROM datatab " +
        "WHERE int_field1 IS NOT NULL LIMIT 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("arithmetic division with ansi mode - byte types") {
    runQueryAndCompare(
      "SELECT CAST(int_field1 AS TINYINT) / CAST(2 AS TINYINT) FROM datatab " +
        "WHERE int_field1 IS NOT NULL LIMIT 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("arithmetic addition with ansi mode - double types") {
    runQueryAndCompare(
      "SELECT double_field1 + 1.5 FROM datatab WHERE double_field1 IS NOT NULL LIMIT 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("arithmetic subtraction with ansi mode - double types") {
    runQueryAndCompare(
      "SELECT double_field1 - 0.5 FROM datatab WHERE double_field1 IS NOT NULL LIMIT 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("arithmetic multiplication with ansi mode - double types") {
    runQueryAndCompare(
      "SELECT double_field1 * 2.0 FROM datatab WHERE double_field1 IS NOT NULL LIMIT 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("arithmetic division with ansi mode - double types") {
    runQueryAndCompare(
      "SELECT double_field1 / 2.0 FROM datatab WHERE double_field1 IS NOT NULL LIMIT 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("arithmetic addition with ansi mode - float types") {
    runQueryAndCompare(
      "SELECT CAST(double_field1 AS FLOAT) + CAST(1.5 AS FLOAT) FROM datatab " +
        "WHERE double_field1 IS NOT NULL LIMIT 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("arithmetic subtraction with ansi mode - float types") {
    runQueryAndCompare(
      "SELECT CAST(double_field1 AS FLOAT) - CAST(0.5 AS FLOAT) FROM datatab " +
        "WHERE double_field1 IS NOT NULL LIMIT 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("arithmetic multiplication with ansi mode - float types") {
    runQueryAndCompare(
      "SELECT CAST(double_field1 AS FLOAT) * CAST(2.0 AS FLOAT) FROM datatab " +
        "WHERE double_field1 IS NOT NULL LIMIT 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("arithmetic division with ansi mode - float types") {
    runQueryAndCompare(
      "SELECT CAST(double_field1 AS FLOAT) / CAST(2.0 AS FLOAT) FROM datatab " +
        "WHERE double_field1 IS NOT NULL LIMIT 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("arithmetic mixed operations with ansi mode") {
    runQueryAndCompare(
      "SELECT (int_field1 + 10) * 2 - 5 FROM datatab WHERE int_field1 IS NOT NULL LIMIT 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("complex arithmetic expressions with ansi mode") {
    runQueryAndCompare(
      "SELECT (int_field1 + 5) * (int_field1 - 1) + double_field1 FROM datatab " +
        "WHERE int_field1 IS NOT NULL LIMIT 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }

  test("arithmetic with null values and ansi mode") {
    runQueryAndCompare(
      "SELECT int_field1 + NULL, NULL * int_field1, double_field1 - NULL FROM datatab LIMIT 1") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }
  }
}
