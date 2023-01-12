Object stores offered by CSPs such as AWS S3 are important for users of Gluten to store their data. This doc will discuss all details of configs, and use cases around using Gluten with object stores. 

Velox supports local cache when reading data from HDFS/S3. The feature is very useful if remote storage is slow, e.g., reading from a public S3 bucket. With this feature, Velox can asynchronously cache the data on local disk when reading from remote storage, and the future reading requests on already cached blocks will be serviced from local cache files. To enable the local caching feature, below configurations are required:
```
spark.gluten.sql.columnar.backend.velox.cacheEnabled // enable or disable velox cache, default False
spark.gluten.sql.columnar.backend.velox.cachePath  // the folder to store the cache files, default to /tmp
spark.gluten.sql.columnar.backend.velox.cacheSize  // the total size of the cache, default to 128MB
spark.gluten.sql.columnar.backend.velox.cacheShards // the shards of the cache, default to 1
```

It's recommended to mount SSDs to the cache path to get the best performance of local caching. 
On the start up of Spark context, the cache files will be allocated under "spark.gluten.sql.columnar.backend.velox.cachePath", with UUID based suffix, e.g. "/tmp/cache.13e8ab65-3af4-46ac-8d28-ff99b2a9ec9b0". 
Gluten is not able to reuse older caches for now, and the old cache files are left there after Spark context shutdown.

Running spark in standalone mode

If you are running Spark in standalone mode, please make sure that the gluten jars path is part of the following configs in spark-defaults.conf:
spark.driver.extraClassPath
spark.executor.extraClassPath

For example:
spark.driver.extraClassPath        /home/ubuntu/spark/jars/jars_ext/aws-java-sdk-bundle-1.11.271.jar:/home/ubuntu/gluten_jars/gluten-spark3.2_2.12-1.0.0-SNAPSHOT-jar-with-dependencies.jar
spark.driver.extraClassPath        /home/ubuntu/spark/jars/jars_ext/aws-java-sdk-bundle-1.11.271.jar:/home/ubuntu/gluten_jars/gluten-spark3.2_2.12-1.0.0-SNAPSHOT-jar-with-dependencies.jar
