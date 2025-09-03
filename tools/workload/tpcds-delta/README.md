# Test on Velox backend with Delta Lake and TPC-DS workload

This workload example is verified with JDK 8, Spark 3.4.4 and Delta 2.4.0.

## Test dataset

A simple way to generate Delta tables is to use the built-in TPC-DS data generator of `gluten-it`.

```bash
# 1. Switch to gluten-it folder.
cd gluten-it/
# 2. Build gluten-it with Spark 3.4 and Delta support.
mvn clean install -D spark-3.4,delta
# 3. Set JAVA_HOME. For example, JDK 17.
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
# 4. Set JVM heap size. For example, 224G.
export GLUTEN_IT_JVM_ARGS="-Xmx224G"
# 5. Generate the tables.
sbin/gluten-it.sh \
  data-gen-only \
  --data-source=delta \
  --local \
  --benchmark-type=ds \
  --threads=112 \
  -s=100 \
  --gen-partitioned-data \
  --data-dir=/tmp/my-data
```

Meanings of the commands are explained as follows:

- `--benchmark-type=ds`:  "ds" for TPC-DS, "h" for TPC-H.
- `--threads=112`: The parallelism. Ideal to set to the core number.
- `-s=100`: The scale factor.
- `--data-dir=/tmp/my-data`: The target table folder. If it doesn't exist, it will then be created.

When the command is finished, check the generated table folder:

```bash
ls -l /tmp/my-data/
```

You should see a generated table folder in it:

```bash
drwxr-xr-x 20 root root 4096 Sep  1 15:13 tpcds-generated-100.0-delta-partitioned
```

The folder `tpcds-generated-100.0-delta-partitioned` is the generated Delta TPC-DS table. As shown by the folder name, it's partitioned, and with scale factor 100.0.

## Test Queries
We provide the test queries in [TPC-DS Queries](../../../tools/gluten-it/common/src/main/resources/tpcds-queries).
We provide a Scala script in [Run TPC-DS](./run_tpcds) directory about how to run TPC-DS queries on the generated Delta tables.
