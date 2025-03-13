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
package org.apache.gluten.spark34.sql;

import org.apache.gluten.spark34.TestConfUtil;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.TestHiveMetastore;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkTestBase;
import org.apache.iceberg.spark.sql.TestAggregatePushDown;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class GlutenTestAggregatePushDown extends TestAggregatePushDown {
    public GlutenTestAggregatePushDown(String catalogName, String implementation, Map<String, String> config) {
        super(catalogName, implementation, config);
    }

    @BeforeClass
    public static void startMetastoreAndSpark() {
        SparkTestBase.metastore = new TestHiveMetastore();
        metastore.start();
        SparkTestBase.hiveConf = metastore.hiveConf();

        SparkTestBase.spark =
                SparkSession.builder()
                        .master("local[2]")
                        .config("spark.sql.iceberg.aggregate_pushdown", "true")
                        .config(TestConfUtil.GLUTEN_CONF)
                        .enableHiveSupport()
                        .getOrCreate();

        SparkTestBase.catalog =
                (HiveCatalog)
                        CatalogUtil.loadCatalog(
                                HiveCatalog.class.getName(), "hive", ImmutableMap.of(), hiveConf);

        try {
            catalog.createNamespace(Namespace.of("default"));
        } catch (AlreadyExistsException ignored) {
            // the default namespace already exists. ignore the create error
        }
    }

    @Test
    public void testAggregatePushDownInDeleteCopyOnWrite() {
        sql("CREATE TABLE %s (id LONG, data INT) USING iceberg", tableName);
        sql(
                "INSERT INTO TABLE %s VALUES (1, 1111), (1, 2222), (2, 3333), (2, 4444), (3, 5555), (3, 6666) ",
                tableName);
        withSQLConf(ImmutableMap.of("spark.gluten.enabled", "false"), () -> {
            sql("DELETE FROM %s WHERE data = 1111", tableName);
            Dataset<Row> df = spark.sql(String.format("DELETE FROM %s WHERE data = 1111", tableName));
            df.collect();
            System.out.println(df.queryExecution().executedPlan().toString());
        });

        Dataset<Row> df = spark.sql(String.format("DELETE FROM %s WHERE data = 1111", tableName));
        df.collect();
        System.out.println("gluten plan" + df.queryExecution().executedPlan().toString());
        String select = "SELECT max(data), min(data), count(data) FROM %s";

        List<Object[]> explain = sql("EXPLAIN " + select, tableName);
        String explainString = explain.get(0)[0].toString().toLowerCase(Locale.ROOT);
        boolean explainContainsPushDownAggregates = false;
        if (explainString.contains("max(data)")
                && explainString.contains("min(data)")
                && explainString.contains("count(data)")) {
            explainContainsPushDownAggregates = true;
        }

        Assert.assertTrue("min/max/count pushed down for deleted", explainContainsPushDownAggregates);

        AtomicReference<List<Object[]>> actual = new AtomicReference<>();
        withSQLConf(ImmutableMap.of("spark.gluten.enabled", "false"), () -> {
            actual.set(sql(select, tableName));
        });
        
        List<Object[]> expected = Lists.newArrayList();
        expected.add(new Object[] {6666, 2222, 5L});
        assertEquals("min/max/count push down", expected, actual.get());
    }
}
