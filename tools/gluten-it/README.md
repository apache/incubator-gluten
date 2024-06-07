# Gluten Integration Testing (gluten-it)

The project makes it easy to test Gluten build locally.

## Gluten ?

Gluten is a native Spark SQL implementation as a standard Spark plug-in.

https://github.com/apache/incubator-gluten

## Getting Started

### 1. Install Gluten in your local machine

See official Gluten build guidance https://github.com/apache/incubator-gluten#how-to-use-gluten

### 2. Install and run gluten-it with Spark version

```sh
cd gluten/tools/gluten-it
mvn clean package -P{Spark-Version}
sbin/gluten-it.sh
```

> Note: *Spark-Version* support *spark-3.2* and *spark-3.3* only

## Usage

### CMD args

```
Usage: gluten-it [-hV] [COMMAND]
Gluten integration test using various of benchmark's data and queries.
  -h, --help      Show this help message and exit.
  -V, --version   Print version information and exit.
Commands:
  data-gen-only    Generate data only.
  queries          Run queries.
  queries-compare  Run queries and do result comparison with baseline preset.
  spark-shell      Open a standard Spark shell.
  parameterized    Run queries with parameterized configurations
```

Also, use `[COMMAND] -h` to view help message for a specific subcommand. For example:

```sh
sbin/gluten-it.sh queries -h
```
