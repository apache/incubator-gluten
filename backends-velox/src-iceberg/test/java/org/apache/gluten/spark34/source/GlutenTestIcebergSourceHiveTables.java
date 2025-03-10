package org.apache.gluten.spark34.source;

import org.apache.iceberg.spark.source.TestIcebergSourceHiveTables;

// Fallback all the table scan because source table is metadata table with format avro.
public class GlutenTestIcebergSourceHiveTables extends TestIcebergSourceHiveTables {
}
