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

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.flink.CatalogTestBase;
import org.apache.iceberg.flink.MiniClusterResource;
import org.apache.iceberg.flink.TestHelpers;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

public class TestFlinkUpsert extends CatalogTestBase {

  @Parameter(index = 2)
  private FileFormat format;

  @Parameter(index = 3)
  private boolean isStreamingJob;

  private final Map<String, String> tableUpsertProps = Maps.newHashMap();
  private TableEnvironment tEnv;
  private SparkSession spark;
  private ClickHouseIcebergHiveTableSupport hiveTableSupport;

  @Parameters(name = "catalogName={0}, baseNamespace={1}, format={2}, isStreaming={3}")
  public static List<Object[]> parameters() {
    List<Object[]> parameters = Lists.newArrayList();
    // ignore ORC and AVRO, ch backend only support PARQUET
    for (FileFormat format : new FileFormat[] {FileFormat.PARQUET}) {
      for (Boolean isStreaming : new Boolean[] {true, false}) {
        // Only test with one catalog as this is a file operation concern.
        // FlinkCatalogTestBase requires the catalog name start with testhadoop if using hadoop
        // catalog.
        String catalogName = "testhive";
        Namespace baseNamespace = Namespace.empty();
        parameters.add(new Object[] {catalogName, baseNamespace, format, isStreaming});
      }
    }
    return parameters;
  }

  @Override
  protected TableEnvironment getTableEnv() {
    if (tEnv == null) {
      synchronized (this) {
        EnvironmentSettings.Builder settingsBuilder = EnvironmentSettings.newInstance();
        if (isStreamingJob) {
          settingsBuilder.inStreamingMode();
          StreamExecutionEnvironment env =
              StreamExecutionEnvironment.getExecutionEnvironment(
                  MiniClusterResource.DISABLE_CLASSLOADER_CHECK_CONFIG);
          env.enableCheckpointing(400);
          env.setMaxParallelism(2);
          env.setParallelism(2);
          tEnv = StreamTableEnvironment.create(env, settingsBuilder.build());
        } else {
          settingsBuilder.inBatchMode();
          tEnv = TableEnvironment.create(settingsBuilder.build());
        }
      }
    }
    return tEnv;
  }

  @Override
  @BeforeEach
  public void before() {
    super.before();
    sql("CREATE DATABASE IF NOT EXISTS %s", flinkDatabase);
    sql("USE CATALOG %s", catalogName);
    sql("USE %s", DATABASE);
    tableUpsertProps.put(TableProperties.FORMAT_VERSION, "2");
    tableUpsertProps.put(TableProperties.UPSERT_ENABLED, "true");
    tableUpsertProps.put(TableProperties.DEFAULT_FILE_FORMAT, format.name());

    hiveTableSupport = new ClickHouseIcebergHiveTableSupport();
    hiveTableSupport.initSparkConf(
        hiveConf.get("hive.metastore.uris"),
        catalogName,
        String.format("file://%s", this.warehouseRoot()));
    hiveTableSupport.initializeSession();
    spark = hiveTableSupport.spark();
  }

  @Override
  @AfterEach
  public void clean() {
    sql("DROP DATABASE IF EXISTS %s", flinkDatabase);
    super.clean();

    hiveTableSupport.clean();
  }

  static String toWithClause(Map<String, String> props) {
    StringBuilder builder = new StringBuilder();
    builder.append("(");
    int propCount = 0;
    for (Map.Entry<String, String> entry : props.entrySet()) {
      if (propCount > 0) {
        builder.append(",");
      }
      builder
          .append("'")
          .append(entry.getKey())
          .append("'")
          .append("=")
          .append("'")
          .append(entry.getValue())
          .append("'");
      propCount++;
    }
    builder.append(")");
    return builder.toString();
  }

  @TestTemplate
  public void testUpsertAndQuery() {
    String tableName = "test_upsert_query";
    LocalDate dt20220301 = LocalDate.of(2022, 3, 1);
    LocalDate dt20220302 = LocalDate.of(2022, 3, 2);
    Date dt20220301Spark =
        Date.from(dt20220301.atStartOfDay().atZone(ZoneId.systemDefault()).toInstant());
    Date dt20220302Spark =
        Date.from(dt20220302.atStartOfDay().atZone(ZoneId.systemDefault()).toInstant());

    sql(
        "CREATE TABLE %s(id INT NOT NULL, name STRING NOT NULL, dt DATE, "
            + "PRIMARY KEY(id,dt) NOT ENFORCED) "
            + "PARTITIONED BY (dt) WITH %s",
        tableName, toWithClause(tableUpsertProps));

    try {
      sql(
          "INSERT INTO %s VALUES "
              + "(1, 'Bill', DATE '2022-03-01'),"
              + "(1, 'Jane', DATE '2022-03-01'),"
              + "(2, 'Jane', DATE '2022-03-01')",
          tableName);

      sql(
          "INSERT INTO %s VALUES "
              + "(2, 'Bill', DATE '2022-03-01'),"
              + "(1, 'Jane', DATE '2022-03-02'),"
              + "(2, 'Jane', DATE '2022-03-02')",
          tableName);

      List<Row> rowsOn20220301 =
          Lists.newArrayList(Row.of(1, "Jane", dt20220301), Row.of(2, "Bill", dt20220301));
      TestHelpers.assertRows(
          sql("SELECT * FROM %s WHERE dt < '2022-03-02'", tableName), rowsOn20220301);
      List<Row> rowsOn20220301Spark =
          Lists.newArrayList(
              Row.of(1, "Jane", dt20220301Spark), Row.of(2, "Bill", dt20220301Spark));
      TestHelpers.assertRows(
          convertToFlinkRows(
              spark.sql(
                  String.format(
                      Locale.ROOT,
                      "SELECT * FROM %s.db.%s WHERE dt < '2022-03-02'",
                      catalogName,
                      tableName)),
              3),
          rowsOn20220301Spark);

      List<Row> rowsOn20220302 =
          Lists.newArrayList(Row.of(1, "Jane", dt20220302), Row.of(2, "Jane", dt20220302));
      TestHelpers.assertRows(
          sql("SELECT * FROM %s WHERE dt = '2022-03-02'", tableName), rowsOn20220302);
      List<Row> rowsOn20220302Spark =
          Lists.newArrayList(
              Row.of(1, "Jane", dt20220302Spark), Row.of(2, "Jane", dt20220302Spark));
      TestHelpers.assertRows(
          convertToFlinkRows(
              spark.sql(
                  String.format(
                      Locale.ROOT,
                      "SELECT * FROM %s.db.%s WHERE dt = '2022-03-02'",
                      catalogName,
                      tableName)),
              3),
          rowsOn20220302Spark);

      TestHelpers.assertRows(
          sql("SELECT * FROM %s", tableName),
          Lists.newArrayList(Iterables.concat(rowsOn20220301, rowsOn20220302)));
      TestHelpers.assertRows(
          convertToFlinkRows(
              spark.sql(
                  String.format(Locale.ROOT, "SELECT * FROM %s.db.%s", catalogName, tableName)),
              3),
          Lists.newArrayList(Iterables.concat(rowsOn20220301Spark, rowsOn20220302Spark)));
    } finally {
      sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, tableName);
    }
  }

  private List<Row> convertToFlinkRows(Dataset<org.apache.spark.sql.Row> rows, int columnCount) {
    return rows.collectAsList().stream()
        .map(
            r -> {
              switch (columnCount) {
                case 1:
                  return Row.of(r.get(0));
                case 2:
                  return Row.of(r.get(0), r.get(1));
                case 3:
                  return Row.of(r.get(0), r.get(1), r.get(2));
                default:
                  throw new IllegalArgumentException("Unsupported column count: " + columnCount);
              }
            })
        .collect(Collectors.toList());
  }

  @TestTemplate
  public void testUpsertOptions() {
    String tableName = "test_upsert_options";
    LocalDate dt20220301 = LocalDate.of(2022, 3, 1);
    LocalDate dt20220302 = LocalDate.of(2022, 3, 2);
    Date dt20220301Spark =
        Date.from(dt20220301.atStartOfDay().atZone(ZoneId.systemDefault()).toInstant());
    Date dt20220302Spark =
        Date.from(dt20220302.atStartOfDay().atZone(ZoneId.systemDefault()).toInstant());

    Map<String, String> optionsUpsertProps = Maps.newHashMap(tableUpsertProps);
    optionsUpsertProps.remove(TableProperties.UPSERT_ENABLED);
    sql(
        "CREATE TABLE %s(id INT NOT NULL, name STRING NOT NULL, dt DATE, "
            + "PRIMARY KEY(id,dt) NOT ENFORCED) "
            + "PARTITIONED BY (dt) WITH %s",
        tableName, toWithClause(optionsUpsertProps));

    try {
      sql(
          "INSERT INTO %s /*+ OPTIONS('upsert-enabled'='true')*/  VALUES "
              + "(1, 'Bill', DATE '2022-03-01'),"
              + "(1, 'Jane', DATE '2022-03-01'),"
              + "(2, 'Jane', DATE '2022-03-01')",
          tableName);

      sql(
          "INSERT INTO %s /*+ OPTIONS('upsert-enabled'='true')*/  VALUES "
              + "(2, 'Bill', DATE '2022-03-01'),"
              + "(1, 'Jane', DATE '2022-03-02'),"
              + "(2, 'Jane', DATE '2022-03-02')",
          tableName);

      List<Row> rowsOn20220301 =
          Lists.newArrayList(Row.of(1, "Jane", dt20220301), Row.of(2, "Bill", dt20220301));
      TestHelpers.assertRows(
          sql("SELECT * FROM %s WHERE dt < '2022-03-02'", tableName), rowsOn20220301);
      List<Row> rowsOn20220301Spark =
          Lists.newArrayList(
              Row.of(1, "Jane", dt20220301Spark), Row.of(2, "Bill", dt20220301Spark));
      TestHelpers.assertRows(
          convertToFlinkRows(
              spark.sql(
                  String.format(
                      Locale.ROOT,
                      "SELECT * FROM %s.db.%s WHERE dt < '2022-03-02'",
                      catalogName,
                      tableName)),
              3),
          rowsOn20220301Spark);

      List<Row> rowsOn20220302 =
          Lists.newArrayList(Row.of(1, "Jane", dt20220302), Row.of(2, "Jane", dt20220302));
      TestHelpers.assertRows(
          sql("SELECT * FROM %s WHERE dt = '2022-03-02'", tableName), rowsOn20220302);
      List<Row> rowsOn20220302Spark =
          Lists.newArrayList(
              Row.of(1, "Jane", dt20220302Spark), Row.of(2, "Jane", dt20220302Spark));
      TestHelpers.assertRows(
          convertToFlinkRows(
              spark.sql(
                  String.format(
                      Locale.ROOT,
                      "SELECT * FROM %s.db.%s WHERE dt = '2022-03-02'",
                      catalogName,
                      tableName)),
              3),
          rowsOn20220302Spark);

      TestHelpers.assertRows(
          sql("SELECT * FROM %s", tableName),
          Lists.newArrayList(Iterables.concat(rowsOn20220301, rowsOn20220302)));
      TestHelpers.assertRows(
          convertToFlinkRows(
              spark.sql(
                  String.format(Locale.ROOT, "SELECT * FROM %s.db.%s", catalogName, tableName)),
              3),
          Lists.newArrayList(Iterables.concat(rowsOn20220301Spark, rowsOn20220302Spark)));
    } finally {
      sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, tableName);
    }
  }

  @TestTemplate
  public void testPrimaryKeyEqualToPartitionKey() {
    // This is an SQL based reproduction of TestFlinkIcebergSinkV2#testUpsertOnDataKey
    String tableName = "upsert_on_id_key";
    try {
      sql(
          "CREATE TABLE %s(id INT NOT NULL, name STRING NOT NULL, PRIMARY KEY(id) NOT ENFORCED) "
              + "PARTITIONED BY (id) WITH %s",
          tableName, toWithClause(tableUpsertProps));

      sql("INSERT INTO %s VALUES " + "(1, 'Bill')," + "(1, 'Jane')," + "(2, 'Bill')", tableName);

      List<Row> rows = Lists.newArrayList(Row.of(1, "Jane"), Row.of(2, "Bill"));
      TestHelpers.assertRows(sql("SELECT * FROM %s", tableName), rows);
      TestHelpers.assertRows(
          convertToFlinkRows(
              spark.sql(
                  String.format(Locale.ROOT, "SELECT * FROM %s.db.%s", catalogName, tableName)),
              2),
          rows);

      sql("INSERT INTO %s VALUES " + "(1, 'Bill')," + "(2, 'Jane')", tableName);

      List<Row> rows2 = Lists.newArrayList(Row.of(1, "Bill"), Row.of(2, "Jane"));
      TestHelpers.assertRows(sql("SELECT * FROM %s", tableName), rows2);
      spark.sql(String.format(Locale.ROOT, "REFRESH TABLE %s.db.%s", catalogName, tableName));
      TestHelpers.assertRows(
          convertToFlinkRows(
              spark.sql(
                  String.format(Locale.ROOT, "SELECT * FROM %s.db.%s", catalogName, tableName)),
              2),
          rows2);

      sql("INSERT INTO %s VALUES " + "(3, 'Bill')," + "(4, 'Jane')", tableName);

      List<Row> rows3 =
          Lists.newArrayList(
              Row.of(1, "Bill"), Row.of(2, "Jane"), Row.of(3, "Bill"), Row.of(4, "Jane"));
      TestHelpers.assertRows(sql("SELECT * FROM %s", tableName), rows3);
      spark.sql(String.format(Locale.ROOT, "REFRESH TABLE %s.db.%s", catalogName, tableName));
      TestHelpers.assertRows(
          convertToFlinkRows(
              spark.sql(
                  String.format(Locale.ROOT, "SELECT * FROM %s.db.%s", catalogName, tableName)),
              2),
          rows3);
    } finally {
      sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, tableName);
    }
  }

  @TestTemplate
  public void testPrimaryKeyFieldsAtBeginningOfSchema() {
    String tableName = "upsert_on_pk_at_schema_start";
    LocalDate dt = LocalDate.of(2022, 3, 1);
    Date dtSpark = Date.from(dt.atStartOfDay().atZone(ZoneId.systemDefault()).toInstant());

    try {
      sql(
          "CREATE TABLE %s(id INT, dt DATE NOT NULL, name STRING NOT NULL, "
              + "PRIMARY KEY(id,dt) NOT ENFORCED) "
              + "PARTITIONED BY (dt) WITH %s",
          tableName, toWithClause(tableUpsertProps));

      sql(
          "INSERT INTO %s VALUES "
              + "(1, DATE '2022-03-01', 'Andy'),"
              + "(1, DATE '2022-03-01', 'Bill'),"
              + "(2, DATE '2022-03-01', 'Jane')",
          tableName);

      TestHelpers.assertRows(
          sql("SELECT * FROM %s", tableName),
          Lists.newArrayList(Row.of(1, dt, "Bill"), Row.of(2, dt, "Jane")));
      TestHelpers.assertRows(
          convertToFlinkRows(
              spark.sql(
                  String.format(Locale.ROOT, "SELECT * FROM %s.db.%s", catalogName, tableName)),
              3),
          Lists.newArrayList(Row.of(1, dtSpark, "Bill"), Row.of(2, dtSpark, "Jane")));

      sql(
          "INSERT INTO %s VALUES "
              + "(1, DATE '2022-03-01', 'Jane'),"
              + "(2, DATE '2022-03-01', 'Bill')",
          tableName);

      TestHelpers.assertRows(
          sql("SELECT * FROM %s", tableName),
          Lists.newArrayList(Row.of(1, dt, "Jane"), Row.of(2, dt, "Bill")));
      spark.sql(String.format(Locale.ROOT, "REFRESH TABLE %s.db.%s", catalogName, tableName));
      TestHelpers.assertRows(
          convertToFlinkRows(
              spark.sql(
                  String.format(Locale.ROOT, "SELECT * FROM %s.db.%s", catalogName, tableName)),
              3),
          Lists.newArrayList(Row.of(1, dtSpark, "Jane"), Row.of(2, dtSpark, "Bill")));

      sql(
          "INSERT INTO %s VALUES "
              + "(3, DATE '2022-03-01', 'Duke'),"
              + "(4, DATE '2022-03-01', 'Leon')",
          tableName);

      TestHelpers.assertRows(
          sql("SELECT * FROM %s", tableName),
          Lists.newArrayList(
              Row.of(1, dt, "Jane"),
              Row.of(2, dt, "Bill"),
              Row.of(3, dt, "Duke"),
              Row.of(4, dt, "Leon")));
      spark.sql(String.format(Locale.ROOT, "REFRESH TABLE %s.db.%s", catalogName, tableName));
      TestHelpers.assertRows(
          convertToFlinkRows(
              spark.sql(
                  String.format(Locale.ROOT, "SELECT * FROM %s.db.%s", catalogName, tableName)),
              3),
          Lists.newArrayList(
              Row.of(1, dtSpark, "Jane"),
              Row.of(2, dtSpark, "Bill"),
              Row.of(3, dtSpark, "Duke"),
              Row.of(4, dtSpark, "Leon")));
    } finally {
      sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, tableName);
    }
  }

  @TestTemplate
  public void testPrimaryKeyFieldsAtEndOfTableSchema() {
    // This is the same test case as testPrimaryKeyFieldsAtBeginningOfSchema, but the primary key
    // fields
    // are located at the end of the flink schema.
    String tableName = "upsert_on_pk_at_schema_end";
    LocalDate dt = LocalDate.of(2022, 3, 1);
    Date dtSpark = Date.from(dt.atStartOfDay().atZone(ZoneId.systemDefault()).toInstant());
    try {
      sql(
          "CREATE TABLE %s(name STRING NOT NULL, id INT, dt DATE NOT NULL, "
              + "PRIMARY KEY(id,dt) NOT ENFORCED) "
              + "PARTITIONED BY (dt) WITH %s",
          tableName, toWithClause(tableUpsertProps));

      sql(
          "INSERT INTO %s VALUES "
              + "('Andy', 1, DATE '2022-03-01'),"
              + "('Bill', 1, DATE '2022-03-01'),"
              + "('Jane', 2, DATE '2022-03-01')",
          tableName);

      TestHelpers.assertRows(
          sql("SELECT * FROM %s", tableName),
          Lists.newArrayList(Row.of("Bill", 1, dt), Row.of("Jane", 2, dt)));
      TestHelpers.assertRows(
          convertToFlinkRows(
              spark.sql(
                  String.format(Locale.ROOT, "SELECT * FROM %s.db.%s", catalogName, tableName)),
              3),
          Lists.newArrayList(Row.of("Bill", 1, dtSpark), Row.of("Jane", 2, dtSpark)));

      sql(
          "INSERT INTO %s VALUES "
              + "('Jane', 1, DATE '2022-03-01'),"
              + "('Bill', 2, DATE '2022-03-01')",
          tableName);

      TestHelpers.assertRows(
          sql("SELECT * FROM %s", tableName),
          Lists.newArrayList(Row.of("Jane", 1, dt), Row.of("Bill", 2, dt)));
      spark.sql(String.format(Locale.ROOT, "REFRESH TABLE %s.db.%s", catalogName, tableName));
      TestHelpers.assertRows(
          convertToFlinkRows(
              spark.sql(
                  String.format(Locale.ROOT, "SELECT * FROM %s.db.%s", catalogName, tableName)),
              3),
          Lists.newArrayList(Row.of("Jane", 1, dtSpark), Row.of("Bill", 2, dtSpark)));

      sql(
          "INSERT INTO %s VALUES "
              + "('Duke', 3, DATE '2022-03-01'),"
              + "('Leon', 4, DATE '2022-03-01')",
          tableName);

      TestHelpers.assertRows(
          sql("SELECT * FROM %s", tableName),
          Lists.newArrayList(
              Row.of("Jane", 1, dt),
              Row.of("Bill", 2, dt),
              Row.of("Duke", 3, dt),
              Row.of("Leon", 4, dt)));
      spark.sql(String.format(Locale.ROOT, "REFRESH TABLE %s.db.%s", catalogName, tableName));
      TestHelpers.assertRows(
          convertToFlinkRows(
              spark.sql(
                  String.format(Locale.ROOT, "SELECT * FROM %s.db.%s", catalogName, tableName)),
              3),
          Lists.newArrayList(
              Row.of("Jane", 1, dtSpark),
              Row.of("Bill", 2, dtSpark),
              Row.of("Duke", 3, dtSpark),
              Row.of("Leon", 4, dtSpark)));
    } finally {
      sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, tableName);
    }
  }
}
