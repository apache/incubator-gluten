package org.apache.iceberg.spark.source;

import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class GlutenSparkScanBuilder extends SparkScanBuilder {
    public GlutenSparkScanBuilder(SparkSession spark, Table table, String branch, Schema schema, CaseInsensitiveStringMap options) {
        super(spark, table, branch, schema, options);
    }

    public GlutenSparkScanBuilder(SparkSession spark, Table table, CaseInsensitiveStringMap options) {
        this(spark, table, table.schema(), options);
    }

    public GlutenSparkScanBuilder(SparkSession spark, Table table, String branch, CaseInsensitiveStringMap options) {
        this(spark, table, branch, SnapshotUtil.schemaFor(table, branch), options);
    }

    public GlutenSparkScanBuilder(SparkSession spark, Table table, Schema schema, CaseInsensitiveStringMap options) {
        this(spark, table, (String)null, schema, options);
    }
}
