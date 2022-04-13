## Spark Installation
### Option 1: download Spark 3.1.1

Currently Gluten works on the Spark 3.1.1 version.

```
wget http://archive.apache.org/dist/spark/spark-3.1.1/spark-3.1.1-bin-hadoop3.2.tgz
sudo mkdir -p /opt/spark && sudo mv spark-3.1.1-bin-hadoop3.2.tgz /opt/spark
sudo cd /opt/spark && sudo tar -xf spark-3.1.1-bin-hadoop3.2.tgz
export SPARK_HOME=/opt/spark/spark-3.1.1-bin-hadoop3.2/
```

### [Option 2: building Spark from source](https://spark.apache.org/docs/latest/building-spark.html)

``` shell
git clone https://github.com/intel-bigdata/spark.git
cd spark && git checkout native-sql-engine-clean
# check spark supported hadoop version
grep \<hadoop\.version\> -r pom.xml
    <hadoop.version>2.7.4</hadoop.version>
    <hadoop.version>3.2.0</hadoop.version>
# so we should build spark specifying hadoop version as 3.2
./build/mvn -Pyarn -Phadoop-3.2 -Dhadoop.version=3.2.0 -DskipTests clean install
```
Specify SPARK_HOME to spark path

``` shell
export SPARK_HOME=${HADOOP_PATH}
```

## Hadoop Installation

Currently we test Gluten on the Hadoop 3.2.0 version.


### Option 1: download Hadoop 

``` shell
wget https://archive.apache.org/dist/hadoop/common/hadoop-3.2.0/hadoop-3.2.0.tar.gz
sudo mkdir -p /opt/hadoop && sudo mv hadoop-3.2.0.tar.gz /opt/hadoop
sudo cd /opt/hadoop && sudo tar -xf hadoop-3.2.0.tar.gz
export HADOOP_HOME=/opt/hadoop/hadoop-3.2.0/
```


### Option 2: building Hadoop from source

``` shell
git clone https://github.com/apache/hadoop.git
cd hadoop
git checkout rel/release-3.2.0
# only build binary for hadoop
mvn clean install -Pdist -DskipTests -Dtar
# build binary and native library such as libhdfs.so for hadoop
# mvn clean install -Pdist,native -DskipTests -Dtar
```

``` shell
export HADOOP_HOME=${HADOOP_PATH}/hadoop-dist/target/hadoop-3.2.0/
```
