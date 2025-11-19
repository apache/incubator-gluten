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
package org.apache.gluten.source;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.spark.source.TestIcebergSourceHiveTables;
import org.junit.After;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Map;

// Fallback all the table scan because source table is metadata table with format avro.
public class TestGlutenIcebergSourceHiveTables extends TestIcebergSourceHiveTables {

  private static TableIdentifier currentIdentifier;

  // The BeforeAll does not take effect because junit 4 is used in Gluten
  @BeforeClass
  public static void start() {
    Namespace db = Namespace.of(new String[] {"db"});
    if (!catalog.namespaceExists(db)) {
      catalog.createNamespace(db);
    }
  }

  @After
  public void dropTable() throws IOException {
    if (catalog.tableExists(currentIdentifier)) {
      this.dropTable(currentIdentifier);
    }
  }

  public Table createTable(
      TableIdentifier ident, Schema schema, PartitionSpec spec, Map<String, String> properties) {
    currentIdentifier = ident;
    return catalog.createTable(ident, schema, spec, properties);
  }
}
