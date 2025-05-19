---
layout: page
title: Gluten For Flink with Velox Backend
nav_order: 1
---

# Supported Version

| Type  | Version                      |
|-------|------------------------------|
| Flink | 1.19.2                       |
| OS    | Ubuntu20.04/22.04, Centos7/8 |
| jdk   | openjdk11/jdk17              |
| scala | 2.12                         |

# Prerequisite

Currently, with static build Gluten+Flink+Velox backend supports all the Linux OSes, but is only tested on **Ubuntu20.04**. With dynamic build, Gluten+Velox backend support **Ubuntu20.04/Ubuntu22.04/Centos7/Centos8** and their variants.

Currently, the officially supported Flink version is 1.19.2.

We need to set up the `JAVA_HOME` env. Currently, Gluten supports **java 11** and **java 17**.

**For x86_64**

```bash
## make sure jdk11 is used
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
```

**For aarch64**

```bash
## make sure jdk11 is used
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64
export PATH=$JAVA_HOME/bin:$PATH
```

**Get Velox4j**

Gluten for Flink depends on [Velox4j](https://github.com/velox4j/velox4j) to call velox. This is an experimental feature.
You need to get the Velox4j code, and compile it first.

As some features have not been committed to upstream, you have to use the following fork to run it first.

```bash
## fetch velox4j code
git clone https://github.com/bigo-sg/velox4j.git
cd velox4j
git checkout -b gluten origin/gluten
git reset --hard c069ca63aa3a200b49199f7dfa285f011ce6fda5
mvn clean install
```
**Get gluten**

```bash
## config maven, like proxy in ~/.m2/settings.xml

## fetch gluten code
git clone https://github.com/apache/incubator-gluten.git
```

# Build Gluten Flink with Velox Backend

```
cd /path/to/gluten/gluten-flink
mvn clean package 
```

## Dependency library deployment

You need to get the Velox4j packages and used them with gluten.
Velox4j jar available now is velox4j-0.1.0-SNAPSHOT.jar. 

## Submit the Flink SQL job

Submit test script from `flink run`. You can use the `StreamSQLExample` as an example. 

### Flink local cluster

After deploying flink binaries, please add gluten-flink jar to flink library path,
including gluten-flink-runtime-1.4.0.jar, gluten-flink-loader-1.4.0.jar and Velox4j jars above.
And make them loaded before flink libraries.
Then you can go to flink binary path and use the below scripts to
submit the example job.

```bash
bin/start-cluster.sh
bin/flink run -d -m 0.0.0.0:8080 \
    -c org.apache.flink.table.examples.java.basics.StreamSQLExample \
    lib/flink-examples-table_2.12-1.20.1.jar
```

Then you can get the result in `log/flink-*-taskexecutor-*.out`.
And you can see an operator named `gluten-cal` from the web frontend of your flink job. 

#### All operators executed by native
Another example supports all operators executed by native. 
You can use the data-generator.sql in dev directory.

```bash
bin/sql-client.sh -f data-generator.sql
```

### Flink Yarn per job mode

TODO

## Performance
We are working on supporting the [Nexmark](https://github.com/nexmark/nexmark) benchmark for Flink.
Now the q0 has been supported.

Results show that running with gluten can be 2.x times faster than Flink.

Result using gluten (will support TPS metric soon):
```
-------------------------------- Nexmark Results --------------------------------

+------+-----------------+--------+----------+-----------------+--------------+-----------------+
| Query| Events Num      | Cores  | Time(s)  | Cores * Time(s) | Throughput   | Throughput/Cores|
+------+-----------------+--------+----------+-----------------+--------------+-----------------+
|q0    |100,000,000      |NaN     |161.428   |NaN              |619.47 K/s    |0/s              |
|Total |100,000,000      |NaN     |161.428   |NaN              |619.47 K/s    |0/s              |
+------+-----------------+--------+----------+-----------------+--------------+-----------------+
```

Result using Flink:
```
-------------------------------- Nexmark Results --------------------------------

+------+-----------------+--------+----------+-----------------+--------------+-----------------+
| Query| Events Num      | Cores  | Time(s)  | Cores * Time(s) | Throughput   | Throughput/Cores|
+------+-----------------+--------+----------+-----------------+--------------+-----------------+
|q0    |100,000,000      |1.21    |462.069   |558.210          |216.42 K/s    |179.14/s         |
|Total |100,000,000      |1.208   |462.069   |558.210          |216.42 K/s    |179.14/s         |
+------+-----------------+--------+----------+-----------------+--------------+-----------------+
```
We are still optimizing it.

## Notes:
Now both Gluten for Flink and Velox4j have not a bundled jar including all jars depends on.
So you may have to add these jars by yourself, which may including guava-33.4.0-jre.jar, jackson-core-2.18.0.jar,
jackson-databind-2.18.0.jar, jackson-datatype-jdk8-2.18.0.jar, jackson-annotations-2.18.0.jar, arrow-memory-core-18.1.0.jar,
arrow-memory-unsafe-18.1.0.jar, arrow-vector-18.1.0.jar, flatbuffers-java-24.3.25.jar, arrow-format-18.1.0.jar, arrow-c-data-18.1.0.jar.
We will supply bundled jars soon.
