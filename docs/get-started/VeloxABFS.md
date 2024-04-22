---
layout: page
title: Using ABFS with Gluten
nav_order: 6
parent: Getting-Started
---
ABFS is an important data store for big data users. This doc discusses config details and use cases of Gluten with ABFS. To use an ABFS account as your data source, please ensure you use the listed ABFS config in your spark-defaults.conf. If you would like to authenticate with ABFS using additional auth mechanisms, please reach out using the 'Issues' tab.

# Working with ABFS

## Configuring ABFS Access Token

To configure access to your storage account, replace <storage-account> with the name of your account. This property aligns with Spark configurations. By setting this config multiple times using different storage account names, you can access multiple ABFS accounts.

```sh
spark.hadoop.fs.azure.account.key.<storage-account>.dfs.core.windows.net  XXXXXXXXX
```

### Other authentatication methods are not yet supported.

# Local Caching support

Velox supports a local cache when reading data from HDFS/S3/ABFS. With this feature, Velox can asynchronously cache the data on local disk when reading from remote storage and future read requests on previously cached blocks will be serviced from local cache files. To enable the local caching feature, the following configurations are required:

```
spark.gluten.sql.columnar.backend.velox.cacheEnabled      // enable or disable velox cache, default false.
spark.gluten.sql.columnar.backend.velox.memCacheSize      // the total size of in-mem cache, default is 128MB.
spark.gluten.sql.columnar.backend.velox.ssdCachePath      // the folder to store the cache files, default is "/tmp".
spark.gluten.sql.columnar.backend.velox.ssdCacheSize      // the total size of the SSD cache, default is 128MB. Velox will do in-mem cache only if this value is 0.
spark.gluten.sql.columnar.backend.velox.ssdCacheShards    // the shards of the SSD cache, default is 1.
spark.gluten.sql.columnar.backend.velox.ssdCacheIOThreads // the IO threads for cache promoting, default is 1. Velox will try to do "read-ahead" if this value is bigger than 1 
spark.gluten.sql.columnar.backend.velox.ssdODirect        // enable or disable O_DIRECT on cache write, default false.
```

It's recommended to mount SSDs to the cache path to get the best performance of local caching. Cache files will be written to "spark.gluten.sql.columnar.backend.velox.cachePath", with UUID based suffix, e.g. "/tmp/cache.13e8ab65-3af4-46ac-8d28-ff99b2a9ec9b0". Gluten cannot reuse older caches for now, and the old cache files are left after Spark context shutdown.
