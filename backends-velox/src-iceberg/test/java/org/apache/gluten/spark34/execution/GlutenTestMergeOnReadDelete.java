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
package org.apache.gluten.spark34.execution;

import org.apache.iceberg.PlanningMode;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.extensions.TestMergeOnReadDelete;
import org.apache.iceberg.spark.source.TestSparkCatalog;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.Map;

@Ignore
// Should extend TestMergeOnReadDelete but not because all tests failed by delete operator.
public class GlutenTestMergeOnReadDelete extends TestMergeOnReadDelete {

    public GlutenTestMergeOnReadDelete(String catalogName, String implementation, Map<String, String> config, String fileFormat, Boolean vectorized, String distributionMode, boolean fanoutEnabled, String branch, PlanningMode planningMode) {
        super(catalogName, implementation, config, fileFormat, vectorized, distributionMode, fanoutEnabled, branch, planningMode);
    }

    protected Map<String, String> extraTableProperties() {
        return ImmutableMap.of("format-version", "2", "write.delete.mode", RowLevelOperationMode.MERGE_ON_READ.modeName());
    }

    @Parameterized.AfterParam
    public static void clearTestSparkCatalogCache() {
        TestSparkCatalog.clearTables();
    }

    @BeforeClass
    public static void setupSparkConf() {
        spark.conf().set("spark.gluten.sql.transform.logLevel", "WARN");
        spark.conf().set("spark.gluten.sql.columnar.batchscan", "true");
        spark.conf().set("spark.gluten.sql.columnar.shuffle", "true");
    }

    @Test
    public void testAggregatePushDownInMergeOnReadDelete() {
        createAndInitTable("id LONG, data INT");
        sql(
                "INSERT INTO TABLE %s VALUES (1, 1111), (1, 2222), (2, 3333), (2, 4444), (3, 5555), (3, 6666) ",
                tableName);
        createBranchIfNeeded();

        System.out.println("start delete");
        sql("DELETE FROM %s WHERE data = 1111", commitTarget());
        System.out.println("delete success start select");
    }
}
