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
package org.apache.gluten.sql;

import org.apache.gluten.TestConfUtil;

import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.TestHiveMetastore;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.spark.sql.TestAggregatePushDown;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeAll;

public class TestGlutenAggregatePushDown extends TestAggregatePushDown {

  @BeforeAll
  public static void startMetastoreAndSpark() {
    TestBase.metastore = new TestHiveMetastore();
    metastore.start();
    TestBase.hiveConf = metastore.hiveConf();
    TestBase.spark.close();
    TestBase.spark =
        SparkSession.builder()
            .master("local[2]")
            .config("spark.sql.iceberg.aggregate_pushdown", "true")
            .config(TestConfUtil.GLUTEN_CONF)
            .enableHiveSupport()
            .getOrCreate();
    TestBase.catalog =
        (HiveCatalog)
            CatalogUtil.loadCatalog(
                HiveCatalog.class.getName(), "hive", ImmutableMap.of(), hiveConf);

    try {
      catalog.createNamespace(Namespace.of(new String[] {"default"}));
    } catch (AlreadyExistsException var1) {
    }
  }
}
