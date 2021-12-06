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

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.spark.sql.internal.SQLConf

object ParquetSQLConf {
  // We default this option value to TRUE. This is because once the code is executed, the compiled
  // arrow-datasource-parquet.jar file is supposed to be placed into Spark's lib folder. Which
  // means it's user's intention to use the replaced ParquetDataSource.
  val OVERWRITE_PARQUET_DATASOURCE_READ =
  SQLConf.buildConf("spark.sql.arrow.overwrite.parquet.read")
    .doc("Overwrite Parquet datasource v1 with reader of Arrow datasource.")
    .booleanConf
    .createWithDefault(true)

  implicit def fromSQLConf(c: SQLConf): ParquetSQLConf = {
    new ParquetSQLConf(c)
  }
}

class ParquetSQLConf(c: SQLConf) {
  def overwriteParquetDataSourceRead: Boolean =
    c.getConf(ParquetSQLConf.OVERWRITE_PARQUET_DATASOURCE_READ)
}
