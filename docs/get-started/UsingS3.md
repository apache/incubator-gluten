---
layout: page
title: Using S3 with Gluten
nav_order: 3
parent: Getting-Started
---
Object stores offered by CSPs such as AWS S3 are important for users of Gluten to store their data. This doc will discuss all details of configs, and use cases around using Gluten with object stores. 

## 1 Local Caching support

Velox supports a local cache when reading data from HDFS/S3. The feature is very useful if remote storage is slow, e.g., reading from a public S3 bucket and stronger performance is desired. With this feature, Velox can asynchronously cache the data on local disk when reading from remote storage, and the future reading requests on already cached blocks will be serviced from local cache files. To enable the local caching feature, below configurations are required:
```
spark.gluten.sql.columnar.backend.velox.cacheEnabled // enable or disable velox cache, default false.
spark.gluten.sql.columnar.backend.velox.memCacheSize // the total size of in-mem cache, default is 128MB.
spark.gluten.sql.columnar.backend.velox.ssdCachePath  // the folder to store the cache files, default is "/tmp".
spark.gluten.sql.columnar.backend.velox.ssdCacheSize  // the total size of the SSD cache, default is 128MB. Velox will do in-mem cache only if this value is 0.
spark.gluten.sql.columnar.backend.velox.ssdCacheShards // the shards of the SSD cache, default is 1.
spark.gluten.sql.columnar.backend.velox.ssdCacheIOThreads // the IO threads for cache promoting, default is 1. Velox will try to do "read-ahead" if this value is bigger than 1 
spark.gluten.sql.columnar.backend.velox.ssdODirect // enbale or disable O_DIRECT on cache write, default false.
```

It's recommended to mount SSDs to the cache path to get the best performance of local caching. On the start up of Spark context, the cache files will be allocated under "spark.gluten.sql.columnar.backend.velox.cachePath", with UUID based suffix, e.g. "/tmp/cache.13e8ab65-3af4-46ac-8d28-ff99b2a9ec9b0". Gluten is not able to reuse older caches for now, and the old cache files are left there after Spark context shutdown.

## 2 Working with S3

## 2.1 Configuring S3 endpoint

In order to use an S3 endpoint as your data source, please ensure you are using the following S3 configs in your spark-defaults.conf. If you're experiencing any issues authenticating to S3 with additional auth mechanisms, please reach out to us using the 'Issues' tab: 
```shell script
spark.hadoop.fs.s3a.impl           				org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3a.aws.credentials.provider 	org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
spark.hadoop.fs.s3a.access.key     				XXXXXXXXX
spark.hadoop.fs.s3a.secret.key     				XXXXXXXXX
spark.hadoop.fs.s3a.endpoint 					https://s3.us-west-1.amazonaws.com
spark.hadoop.fs.s3a.connection.ssl.enabled 		true	
spark.hadoop.fs.s3a.path.style.access 			false
```

## 2.2 Running spark in standalone mode

If you are running Spark in standalone mode, please make sure that the gluten jars path is part of the following configs in spark-defaults.conf:
spark.driver.extraClassPath
spark.executor.extraClassPath

For example:
```shell script
spark.driver.extraClassPath        /home/ubuntu/spark/jars/jars_ext/aws-java-sdk-bundle-1.11.271.jar:/home/ubuntu/gluten_jars/<gluten-jar>
spark.executor.extraClassPath        /home/ubuntu/spark/jars/jars_ext/aws-java-sdk-bundle-1.11.271.jar:/home/ubuntu/gluten_jars/<gluten-jar>
```
