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

Velox supports a local cache when reading data from ABFS. Please refer [Velox Local Cache](VeloxLocalCache.md) part for more detailed configurations.