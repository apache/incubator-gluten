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

package org.apache.gluten.execution.iceberg;

import java.util.Map;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.TestHiveMetastore;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.spark.source.TestPositionDeletesTable;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith({ParameterizedTestExtension.class})
public class TestPositionDeletesTableGluten extends TestPositionDeletesTable {
  private static final Map<String, String> CATALOG_PROPS =
      ImmutableMap.of("type", "hive", "default-namespace", "default", "cache-enabled", "false");
  private static ClickHouseIcebergHiveTableSupport hiveTableSupport;

  @BeforeAll
  public static void startMetastoreAndSpark() {
    metastore = new TestHiveMetastore();
    metastore.start();
    hiveConf = metastore.hiveConf();
    hiveTableSupport = new ClickHouseIcebergHiveTableSupport();
    hiveTableSupport.initSparkConf(
        hiveConf.get("hive.metastore.uris"), SparkCatalogConfig.HIVE.catalogName(), null);
    hiveTableSupport.initializeSession();
    spark = hiveTableSupport.spark();
    sparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
    catalog =
        (HiveCatalog)
            CatalogUtil.loadCatalog(
                HiveCatalog.class.getName(), "hive", ImmutableMap.of(), hiveConf);

    try {
      catalog.createNamespace(Namespace.of(new String[] {"default"}));
    } catch (AlreadyExistsException ignore) {
    }
  }

  @AfterAll
  public static void stopMetastoreAndSpark() throws Exception {
    catalog = null;
    if (metastore != null) {
      metastore.stop();
      metastore = null;
    }
    hiveTableSupport.clean();
  }

  public TestPositionDeletesTableGluten() {}

  @Parameters(name = "catalogName = {1}, implementation = {2}, config = {3}, fileFormat = {4}")
  public static Object[][] parameters() {
    // ignore ORC and AVRO, ch backend only support PARQUET
    return new Object[][] {
      {
        SparkCatalogConfig.HIVE.catalogName(),
        SparkCatalogConfig.HIVE.implementation(),
        CATALOG_PROPS,
        FileFormat.PARQUET
      }
    };
  }
}
