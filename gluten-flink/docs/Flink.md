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
| scala | 2.12(optional)               |

# Prerequisite

Currently, with static build Gluten+Flink+Velox backend supports all the Linux OSes, but is only tested on **Ubuntu20.04**. With dynamic build, Gluten+Velox backend support **Ubuntu20.04/Ubuntu22.04/Centos7/Centos8** and their variants.

Currently, the officially supported Flink version is 1.19.2.

We need to set up the `JAVA_HOME` env. Currently, Gluten supports **java 11** and **java 17**.

You can follow the steps below to set up the compilation environment for x86_64 in Ubuntu.

```bash
apt update
apt install software-properties-common
add-apt-repository ppa:ubuntu-toolchain-r/test
apt update
apt install gcc-11 g++-11
ln -s /usr/bin/gcc-11 /usr/bin/gcc
ln -s /usrbin/g++-11 /usr/bin/g++


git clone https://github.com/facebookincubator/velox.git
cd velox
./scripts/setup-ubuntu.sh

apt install openjdk-11-jdk maven
apt install patchelf

# optional, no need for now
curl -fL https://github.com/coursier/coursier/releases/latest/download/cs-x86_64-pc-linux.gz | gzip -d > cs && chmod +x cs && ./cs setup
cs install scala:2.12.8 && cs install scalac:2.12.8

```

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

There are currently modifications to velox4j that have not been merged upstream. Please fetch the corresponding branch from https://github.com/bigo-sg/velox4j.git.

```bash
## fetch velox4j code
git clone https://github.com/bigo-sg/velox4j.git
cd velox4j
git checkout -b gluten origin/gluten
mvn clean install -Dgpg.skip=true
```
**Get gluten**

```bash
## config maven, like proxy in ~/.m2/settings.xml

## fetch gluten code
git clone https://github.com/bigo-sg/gluten.git
git checkout flink
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

Deploy local flink cluster as following

#### Prepare flink binaries
##### Download the pre-compiled package from the official website.
```bash
# 1.19.2 is used at present
https://dlcdn.apache.org/flink/flink-1.19.2/flink-1.19.2-bin-scala_2.12.tgz
tar -xf flink-1.19.2-bin-scala_2.12.tgz
cd flink-1.19.2
```
##### Or compile from the source
```bash
git clone https://github.com/apache/flink.git
cd flink
git checkout v-1.19.2 release-1.19.2

java_version=11
export JAVA_HOME=/usr/lib/jvm/java-1.${java_version}.0-openjdk-amd64
echo $JAVA_HOME
export PATH=$JAVA_HOME/bin:$PATH
java -version

./mvn package -DskipTests -Djdk${java_version} -Pjava${java_version}-target -Drat.skip=true
```
The result is in `build-target`.

#### Update flink/bin/config.sh

We need to ensure that Gluten jars are loaded before Flink runtime jars. Update flink/bin/config.sh as follows:
```
47,55c47
<     SCRIPT_PATH=$(readlink -f "${BASH_SOURCE[0]}" 2>/dev/null || echo "${BASH_SOURCE[0]}")
<     FLINK_BIN_DIR=$(dirname $SCRIPT_PATH)
<     raw_gluten_jars=$(cat $FLINK_BIN_DIR/../extra_jars.txt)
<     GLUTEN_JARS=""
<     for jar in $raw_gluten_jars
<     do
<         GLUTEN_JARS=$GLUTEN_JARS""$jar":"
<     done
<     echo "$GLUTEN_JARS""$FLINK_CLASSPATH""$FLINK_DIST"
---
>     echo "$FLINK_CLASSPATH""$FLINK_DIST"
```

and create a file `extra_jars.txt` in flink root dir. Each line in the file contains the absolute path of a JAR file. These JARs will be loaded first.
```
/work/gluten-flink/gluten-flink/loader/target/gluten-flink-loader-1.4.0-SNAPSHOT.jar
/work/gluten-flink/gluten-flink/runtime/target/gluten-flink-runtime-1.4.0-SNAPSHOT.jar
/root/.m2/repository/io/github/zhztheplayer/velox4j/0.1.0-SNAPSHOT/velox4j-0.1.0-SNAPSHOT.jar
/root/.m2/repository/org/apache/arrow/arrow-memory-unsafe/18.1.0/arrow-memory-unsafe-18.1.0.jar
/root/.m2/repository/org/apache/arrow/arrow-vector/18.2.0/arrow-vector-18.2.0.jar
/root/.m2/repository/com/fasterxml/jackson/core/jackson-core/2.18.0/jackson-core-2.18.0.jar
/root/.m2/repository/com/fasterxml/jackson/core/jackson-databind/2.18.0/jackson-databind-2.18.0.jar
/root/.m2/repository/com/fasterxml/jackson/datatype/jackson-datatype-jdk8/2.18.0/jackson-datatype-jdk8-2.18.0.jar
/root/.m2/repository/com/fasterxml/jackson/core/jackson-annotations/2.18.0/jackson-annotations-2.18.0.jar
/root/.m2/repository/org/apache/arrow/arrow-memory-core/18.1.0/arrow-memory-core-18.1.0.jar
/root/.m2/repository/com/google/flatbuffers/flatbuffers-java/24.3.25/flatbuffers-java-24.3.25.jar
/root/.m2/repository/org/apache/arrow/arrow-format/18.1.0/arrow-format-18.1.0.jar
/root/.m2/repository/org/apache/arrow/arrow-c-data/18.1.0/arrow-c-data-18.1.0.jar
/root/.m2/repository/com/google/guava/guava/33.4.0-jre/guava-33.4.0-jre.jar
```

#### Run test


After deploying flink binaries, please add gluten-flink jar to flink library path,
including gluten-flink-runtime-1.4.0.jar, gluten-flink-loader-1.4.0.jar and Velox4j jars above.
And make them loaded before flink libraries.
Then you can go to flink binary path and use the below scripts to
submit the example job.

```bash
bin/start-cluster.sh
bin/flink run -d -m 0.0.0.0:8081 \
    -c org.apache.flink.table.examples.java.basics.StreamSQLExample \
    lib/flink-examples-table_2.12-1.20.1.jar
```

Another example supports all operators executed by native. 
You can use the data-generator.sql in dev directory.

```bash
bin/start-cluster.sh 
./bin/sql-client.sh -f data-generator.sql 
```

If you encounter an error similar to the following:
```
Caused by: java.util.concurrent.CompletionException: org.apache.flink.shaded.netty4.io.netty.channel.AbstractChannel$AnnotatedConnectException: Connection refused: /0.0.0.0:8081
```
Please check flink/conf/config.xml. Changing all localhost entries to 0.0.0.0 may resolve this issue.

Then you can get the result in `log/flink-*-taskexecutor-*.out`.
And you can see an operator named `gluten-cal` from the web frontend of your flink job. 

### Flink Yarn per job mode

TODO

## Performance
Using the data-generator example, it shows that for native execution, it can generate 10,0000
records in about 60ms, while Flink generator 10,000 records in about 600ms. It runs 10 times faster.
More perf cases to be added.

## Notes:
Now both Gluten for Flink and Velox4j have not a bundled jar including all jars depends on.
So you may have to add these jars by yourself, which may including guava-33.4.0-jre.jar, jackson-core-2.18.0.jar,
jackson-databind-2.18.0.jar, jackson-datatype-jdk8-2.18.0.jar, jackson-annotations-2.18.0.jar, arrow-memory-core-18.1.0.jar,
arrow-memory-unsafe-18.1.0.jar, arrow-vector-18.1.0.jar, flatbuffers-java-24.3.25.jar, arrow-format-18.1.0.jar, arrow-c-data-18.1.0.jar.
We will supply bundled jars soon.
