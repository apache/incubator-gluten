# Test on Velox backend with Delta Lake and TPC-DS workload

This workload example is verified with JDK 8, Spark 3.4.4 and Delta 2.4.0.

## Test dataset

Use bash script `tpcds-dategen-delta.sh` to generate the data. The script relies on a already-built gluten-it
executable. To build it, following the steps:

```bash
cd ${GLUTEN_HOME}/tools/gluten-it/
mvn clean install -P spark-3.4,delta
```

Then call the data generator script:

```bash
cd ${GLUTEN_HOME}/tools/workload/tpcds-delta/gen_data
./tpcds-dategen-delta.sh
```

Meanings of the commands that are used in the script are explained as follows:

- `--benchmark-type=ds`:  "ds" for TPC-DS, "h" for TPC-H.
- `--threads=112`: The parallelism. Ideal to set to the core number.
- `-s=100`: The scale factor.
- `--data-dir=/tmp/my-data`: The target table folder. If it doesn't exist, it will then be created.

When the command is finished, check the data folder:

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
