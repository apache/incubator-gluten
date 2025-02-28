# Qualification Tool

The Qualification Tool analyzes Spark event log files to determine the compatibility and performance of SQL workloads with Gluten.

## Build

To compile and package the Qualification Tool, run the following Maven command:

```bash
mvn clean package
```

This will create a jar file in the `target` directory.

## Run

To execute the tool, use the following command:

```bash
java -jar target/qualification-tool-1.3.0-SNAPSHOT-jar-with-dependencies.jar -f <eventFile>
```

### Parameters:
- **`-f <eventFile>`**: Path to the Spark event log file(s). This can be:
  - A single event log file.
  - A folder containing multiple event log files.
  - Deeply nested folders of event log files.
  - Compressed event log files.
  - Rolling event log files.
  - Comma separated files
- **`-k <gcsKey>`**: (Optional) Path to Google Cloud Storage service account keys.
- **`-o <output>`**: (Optional) Path to the directory where output will be written. Defaults to a temporary directory.
- **`-t <threads>`**: (Optional) Number of processing threads. Defaults to 4.
- **`-v`**: (Optional) Enable non verbose output. Omit this flag for verbose mode.
- **`-p <project>`**: (Optional) Project ID for the run.
- **`-d <dateFilter>`**: (Optional) Analyze only files created after this date (format: YYYY-MM-DD). Defaults to the last 90 days.

### Example Usage:
```bash
java -jar target/qualification-tool-1.3.0-SNAPSHOT-jar-with-dependencies.jar -f /path/to/eventlog
```

### Advanced Example:
```bash
java -jar target/qualification-tool-1.3.0-SNAPSHOT-jar-with-dependencies.jar -f /path/to/folder -o /output/path -t 8 -d 2023-01-01 -k /path/to/gcs_keys.json -p my_project
```

## Features

- Analyzes Spark SQL execution plans for compatibility with Gluten.
- Supports single files, folders, deeply nested folders, compressed files, and rolling event logs.
- Provides detailed reports on supported and unsupported operations.
- Generates metrics on SQL execution times and operator impact.
- Configurable verbosity and threading.

## How It Works

The Qualification Tool analyzes a Spark plan to determine the compatibility of its nodes / operators and clusters with Gluten. Here's a step-by-step explanation of the process:

### Example Spark Plan

Consider the following Spark plan:

```
            G
        /        \
      G[2]       G[3]
       |            |
      S[2]       G[3]
       |            |
      G          S
    /    \
  G[1]    G
  |
  G[1]
  |
  G
```

- **G**: Represents a plan supported by Gluten.
- **S**: Represents a plan not supported by Gluten.
- **[1], [2], [3]**: Indicates the node belongs to a Whole Stage Code Gen Block (Cluster).

### 1. NodeSupportVisitor

The first step is marking each node as supported (`*`) or not supported (`!`) by Gluten:

```
            *G
        /        \
      *G[2]       *G[3]
       |            |
      !S[2]       *G[3]
       |            |
      *G           !S
    /    \
  *G[1]    *G
  |
  *G[1]
  |
  *G
```

- All supported nodes are marked with `*`.
- All unsupported nodes are marked with `!`.

### 2. ClusterSupportVisitor

The second step marks entire clusters as not supported (`!`) if any node in the cluster is unsupported:

```
            *G
        /        \
      !G[2]       *G[3]
       |            |
      !S[2]       *G[3]
       |            |
      *G           !S
    /    \
  *G[1]    *G
  |
  *G[1]
  |
  *G
```

#### Reasoning:
Although Gluten supports these operators, breaking Whole Stage Code Gen (WSCG) boundaries introduces row-to-columnar and columnar-to-row conversions, degrading performance. Hence, we pessimistically mark the entire cluster as not supported.

### 3. ChildSupportVisitor

The final step marks nodes and their parents as not supported if their children are unsupported:

```
            !G
        /        \
      !G[2]       !G[3]
       |            |
      !S[2]       !G[3]
       |            |
      *G           !S
    /    \
  *G[1]    *G
  |
  *G[1]
  |
  *G
```

#### Reasoning:
If a child node is not supported by Gluten, row-to-columnar and columnar-to-row conversions are added, degrading performance. Therefore, we pessimistically mark such nodes as not supported.

### Sample Output
#### AppsRecommendedForBoost.tsv
| applicationId                     | applicationName                   | batchUuid                               | rddPercentage | unsupportedSqlPercentage | totalTaskTime | supportedTaskTime | supportedSqlPercentage | recommendedForBoost | expectedRuntimeReduction |
|-----------------------------------|-----------------------------------|-----------------------------------------|---------------|--------------------------|---------------|-------------------|-------------------------|---------------------|--------------------------|
| app-20241001064609-0000          | POC - StoreItem        | a56bee42   | 0.0%          | 5.2%                     | 3244484900    | 3074633672        | 94.7%                  | true                | 28.4%                   |
| application_1729530422325_0001   | job-audience-generation-21        | UNKNOWN                                 | 0.3%          | 0.6%                     | 1378942742    | 1365259621        | 99.0%                  | true                | 29.7%                   |
| application_1729410290443_0001   | job-audience-generation-27        | UNKNOWN                                 | 0.0%          | 0.7%                     | 1212612881    | 1203690936        | 99.2%                  | true                | 29.7%                   |
| app-20241001080158-0000          | POC - StoreItem        | 94088807   | 0.0%          | 12.5%                    | 668601513     | 584812056         | 87.4%                  | true                | 26.2%                   |
| application_1729991008434_0001   | DailyLoad                 | UNKNOWN                                 | 0.0%          | 12.6%                    | 17134675      | 14963535          | 87.3%                  | true                | 26.1%                   |
| application_1730097715348_0003   | Spark shell                       | UNKNOWN                                 | 0.0%          | 0.0%                     | 4680          | 4680              | 100.0%                 | true                | 30.0%                   |
| application_1728526688221_0001   | job-audience-generation-27 | UNKNOWN                                 | 0.4%          | 59.4%                    | 805991113     | 323274568         | 40.1%                  | false               | 12.0%                   |
| application_1726070403340_0450   | driver.py                         | UNKNOWN                                 | 0.0%          | 81.1%                    | 398992332     | 75187879          | 18.8%                  | false               | 5.6%                    |
| application_1727842554841_0001   | driver.py                         | UNKNOWN                                 | 0.0%          | 58.3%                    | 166686890     | 69492539          | 41.6%                  | false               | 12.5%                   |
| application_1726070403340_0474   | driver.py                         | UNKNOWN                                 | 0.0%          | 81.6%                    | 325389669     | 59687634          | 18.3%                  | false               | 5.5%                    |
| application_1729133595704_0001   | job-audience-generation-54 | UNKNOWN                                 | 0.6%          | 99.3%                    | 472621872     | 20205             | 0.0%                   | false               | 0.0%                    |
| application_1730097715348_0002   | Spark shell                       | UNKNOWN                                 | 33.8%         | 16.6%                    | 4197          | 2077              | 49.5%                  | false               | 14.8%                   |
| application_1730097715348_0001   | Spark shell                       | UNKNOWN                                 | 0.0%          | 0.0%                     | 0             | 0                 | 0.0%                   | false               | 0.0%                    |
| application_1712155629437_0011   | Spark shell                       | UNKNOWN                                 | 0.0%          | 0.0%                     | 0             | 0                 | 0.0%                   | false               | 0.0%                    |
| application_1712155629437_0012   | Spark shell                       | UNKNOWN                                 | 0.0%          | 0.0%                     | 0             | 0                 | 0.0%                   | false               | 0.0%                    |
| application_1712155629437_0007   | Spark shell                       | UNKNOWN                                 | 0.0%          | 0.0%                     | 0             | 0                 | 0.0%                   | false               | 0.0%                    |
| app-20241120163343-0000          | Config Test                       | c2884285   | 0.0%          | 0.0%                     | 0             | 0                 | 0.0%                   | false               | 0.0%                    |
| application_1712155629437_0008   | Spark shell                       | UNKNOWN                                 | 0.0%          | 0.0%                     | 0             | 0                 | 0.0%                   | false               | 0.0%                    |
| application_1712155629437_0010   | Spark shell                       | UNKNOWN                                 | 0.0%          | 0.0%                     | 0             | 0                 | 0.0%                   | false               | 0.0%                    |
| application_1730097715348_0004   | Spark shell                       | UNKNOWN                                 | 0.0%          | 0.0%                     | 0             | 0                 | 0.0%                   | false               | 0.0%                    |
| application_1718075857669_0261   | UnregisteredAsset | UNKNOWN                                 | 0.0%          | 100.0%                   | 12205458      | 0                 | 0.0%                   | false               | 0.0%                    |
| application_1712155629437_0009   | Spark shell                       | UNKNOWN                                 | 0.0%          | 0.0%                     | 0             | 0                 | 0.0%                   | false               | 0.0%                    |
| application_1712155629437_0013   | Spark shell                       | UNKNOWN                                 | 0.0%          | 0.0%                     | 0             | 0                 | 0.0%                   | false               | 0.0%                    |

#### UnsupportedOperators.tsv
| unsupportedOperator                                    | cumulativeCpuMs | count   |
|--------------------------------------------------------|-----------------|---------|
| Scan with format JSON not supported                   | 1722606716687  | 3105    |
| Parquet with struct                                   | 1722606716687  | 3105    |
| Execute InsertIntoHadoopFsRelationCommand not supported| 37884897400    | 405732  |
| BHJ Not Supported                                     | 20120117622    | 132164  |
| SerializeFromObject not supported                     | 13856742899    | 16098   |
| MapPartitions not supported                           | 13856742899    | 16098   |
| FlatMapGroupsInPandas not supported                   | 257970400      | 224     |
| Sample not supported                                  | 157615298      | 432     |
| DeserializeToObject not supported                     | 157610379      | 6721    |
| MapElements not supported                             | 5261628        | 694     |
| Parquet with struct and map                           | 29020          | 44      |
| Execute CreateViewCommand not supported               | 0              | 157     |
| Execute CreateTableCommand not supported              | 0              | 42      |
| WriteFiles not supported                              | 0              | 20      |
| Execute SetCommand not supported                      | 0              | 12      |
| Execute SaveIntoDataSourceCommand not supported       | 0              | 10      |
| CreateTable not supported                             | 0              | 4       |
| Execute RefreshTableCommand not supported             | 0              | 3       |
| Execute DeltaDynamicPartitionOverwriteCommand not supported | 0       | 2       |
| Execute TruncateTableCommand not supported            | 0              | 2       |
| Execute AlterTableAddPartitionCommand not supported   | 0              | 1       |
| CreateNamespace not supported                         | 0              | 1       |


### Summary

The tool ensures that the Spark plan optimizes performance by:
1. Identifying individual node compatibility.
2. Accounting for cluster boundaries and WSCG optimizations.
3. Considering child dependencies and their impact on parent nodes.

## Requirements

- **Java**: Ensure you have JDK 11 or later installed.
