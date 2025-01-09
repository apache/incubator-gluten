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
java -jar target/qualification-tool-1.2.1-SNAPSHOT-jar-with-dependencies.jar -f <eventFile>
```

### Parameters:
- **`-f <eventFile>`**: Path to the Spark event log file(s). This can be:
  - A single event log file.
  - A folder containing multiple event log files.
  - Deeply nested folders of event log files.
  - Compressed event log files.
  - Rolling event log files.
  - Comma deperated files
- **`-k <gcsKey>`**: (Optional) Path to Google Cloud Storage service account keys.
- **`-o <output>`**: (Optional) Path to the directory where output will be written. Defaults to a temporary directory.
- **`-t <threads>`**: (Optional) Number of processing threads. Defaults to 4.
- **`-v`**: (Optional) Enable non verbose output. Omit this flag for verbose mode.
- **`-p <project>`**: (Optional) Project ID for the run.
- **`-d <dateFilter>`**: (Optional) Analyze only files created after this date (format: YYYY-MM-DD). Defaults to the last 90 days.

### Example Usage:
```bash
java -jar target/qualification-tool-1.2.1-SNAPSHOT-jar-with-dependencies.jar -f /path/to/eventlog
```

### Advanced Example:
```bash
java -jar target/qualification-tool-1.2.1-SNAPSHOT-jar-with-dependencies.jar -f /path/to/folder -o /output/path -t 8 -d 2023-01-01 -k /path/to/gcs_keys.json -p my_project
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

### Summary

The tool ensures that the Spark plan optimizes performance by:
1. Identifying individual node compatibility.
2. Accounting for cluster boundaries and WSCG optimizations.
3. Considering child dependencies and their impact on parent nodes.

## Requirements

- **Java**: Ensure you have JDK 11 or later installed.
- **Maven**: Used for building the project.
