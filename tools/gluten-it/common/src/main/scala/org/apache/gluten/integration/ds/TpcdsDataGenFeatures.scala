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
package org.apache.gluten.integration.ds

import org.apache.gluten.integration.DataGen

import org.apache.spark.sql.SparkSession

object TpcdsDataGenFeatures {
  object EnableDeltaDeletionVector extends DataGen.Feature {
    override def name(): String = "enable_dv"
    override def run(spark: SparkSession, source: String): Unit = {
      require(
        source == "delta",
        s"${EnableDeltaDeletionVector.getClass} only supports Delta data source")
      spark.sql(
        "ALTER TABLE store_sales SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
      spark.sql(
        "ALTER TABLE store_returns SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
      spark.sql(
        "ALTER TABLE catalog_sales SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
      spark.sql(
        "ALTER TABLE catalog_returns SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
      spark.sql("ALTER TABLE web_sales SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
      spark.sql(
        "ALTER TABLE web_returns SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
    }
  }

  object DeleteTenPercentData extends DataGen.Feature {
    override def name(): String = "delete_10pc"
    override def run(spark: SparkSession, source: String): Unit = {
      require(
        source == "delta",
        s"${DeleteTenPercentData.getClass} only supports Delta data source")
      spark.sql("DELETE FROM store_sales WHERE ss_ticket_number % 10 = 7").show()
      spark.sql("DELETE FROM store_returns WHERE sr_ticket_number % 10 = 7").show()
      spark.sql("DELETE FROM catalog_sales WHERE cs_order_number % 10 = 7").show()
      spark.sql("DELETE FROM catalog_returns WHERE cr_order_number % 10 = 7").show()
      spark.sql("DELETE FROM web_sales WHERE ws_order_number % 10 = 7").show()
      spark.sql("DELETE FROM web_returns WHERE wr_order_number % 10 = 7").show()
    }
  }
}
