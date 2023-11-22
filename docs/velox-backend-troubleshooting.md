---
layout: page
title: Velox Backend Troubleshooting
nav_order: 10
---
## Troubleshooting

### Fatal error after native exception is thrown

We depend on checking exceptions thrown from native code to validate whether a spark plan can
be really offloaded to native engine. But if `libunwind-dev` is installed, native exception will not be
caught and will interrupt the program. So far, we observed this fatal error can happen only on Ubuntu 20.04.
Please remove `libunwind-dev` and then re-build the project to address this issue.

`sudo apt-get purge --auto-remove libunwind-dev`

### Jar conflict issue

With the latest version of Gluten, there should not be any jar conflict issue anymore. If you still get hit with
such issue, please follow the below instructions.

The potentially conflicting libraries include protobuf (Both Velox and CK backend), flatbuffers (Velox backend), and arrow-* (Velox backend).
These libraries are compiled from source and packed into Gluten's jar. Jvm should search them from Gluten.jar firstly and load them. But for
some reason jvm loads the jars from spark_home/jars which causes conflict. You may use below commands to remove the jars from spark_home/jars.
We are still investigating the root cause. Welcome to share if you have good solution.

```
rm -rf $SPARK_HOME/jars/protobuf-*
# velox backend only
rm -rf $SPARK_HOME/jars/flatbuffers-*
rm -rf $SPARK_HOME/jars/arrow-*
```

### Incompatible class error when using native writer
Gluten native writer overwrite some vanilla spark classes. Therefore, when running a program that uses gluten, it is essential to ensure that the gluten jar is loaded prior to the vanilla spark jar. In this section, we will provide some configuration settings in `$SPARK_HOME/conf/spark-defaults.conf` for Yarn client, Yarn cluster, and Local&Standalone mode to guarantee that the gluten jar is prioritized.

#### Configurations for Yarn Client mode

```
// spark will upload the gluten jar to hdfs and then the nodemanager will fetch the gluten jar before start the executor process. Here also can set the spark.jars.
spark.files = {absolute_path}/gluten-<spark-version>-<gluten-version>-SNAPSHOT-jar-with-dependencies.jar
// The absolute path on running node
spark.driver.extraClassPath={absolute_path}/gluten-<spark-version>-<gluten-version>-SNAPSHOT-jar-with-dependencies.jar
// The relative path under the executor working directory
spark.executor.extraClassPath=./gluten-<spark-version>-<gluten-version>-SNAPSHOT-jar-with-dependencies.jar
```

#### Configurations for Yarn Cluster mode
```
spark.driver.userClassPathFirst = true
spark.executor.userClassPathFirst = true

spark.files = {absolute_path}/gluten-<spark-version>-<gluten-version>-SNAPSHOT-jar-with-dependencies.jar
// The relative path under the executor working directory
spark.driver.extraClassPath=./gluten-<spark-version>-<gluten-version>-SNAPSHOT-jar-with-dependencies.jar
// The relative path under the executor working directory
spark.executor.extraClassPath=./gluten-<spark-version>-<gluten-version>-SNAPSHOT-jar-with-dependencies.jar
```
#### Configurations for Local & Standalone mode
```
// The absolute path on running node
spark.driver.extraClassPath={absolute_path}/gluten-<spark-version>-<gluten-version>-SNAPSHOT-jar-with-dependencies.jar
// The absolute path on running node
spark.executor.extraClassPath={absolute_path}/gluten-<spark-version>-<gluten-version>-SNAPSHOT-jar-with-dependencies.jar
```
