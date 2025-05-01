---
layout: page
title: Using S3 with Gluten
nav_order: 3
parent: Getting-Started
---
Object stores offered by CSPs such as AWS S3 are important for users of Gluten to store their data. This doc will discuss all details of configs, and use cases around using Gluten with object stores. In order to use an S3 endpoint as your data source, please ensure you are using the following S3 configs in your spark-defaults.conf. If you're experiencing any issues authenticating to S3 with additional auth mechanisms, please reach out to us using the 'Issues' tab.

# Working with S3

## Configuring S3 endpoint

S3 provides the endpoint based method to access the files, here's the example configuration. Users may need to modify some values based on real setup.

```sh
spark.hadoop.fs.s3a.impl                        org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3a.aws.credentials.provider    org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
spark.hadoop.fs.s3a.access.key                  XXXXXXXXX
spark.hadoop.fs.s3a.secret.key                  XXXXXXXXX
spark.hadoop.fs.s3a.endpoint                    https://s3.us-west-1.amazonaws.com
spark.hadoop.fs.s3a.connection.ssl.enabled      true
spark.hadoop.fs.s3a.path.style.access           false
```

## Configuring S3 instance credentials

S3 also provides other methods for accessing, you can also use instance credentials by setting the following config

```
spark.hadoop.fs.s3a.use.instance.credentials true
```
Note that in this case, "spark.hadoop.fs.s3a.endpoint" won't take affect as Gluten will use the endpoint set during instance creation.

## Configuring S3 IAM roles
You can also use iam role credentials by setting the following configurations. Instance credentials have higher priority than iam credentials.

```
spark.hadoop.fs.s3a.iam.role  xxxx
spark.hadoop.fs.s3a.iam.role.session.name xxxx
```

Note that `spark.hadoop.fs.s3a.iam.role.session.name` is optional.

## Other authentatication methods are not supported yet

- [AWS temporary credential](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp_request.html)

## Log granularity of AWS C++ SDK in velox

You can change log granularity of AWS C++ SDK by setting the `spark.gluten.velox.awsSdkLogLevel` configuration. The Allowed values are:
 "OFF", "FATAL", "ERROR", "WARN", "INFO", "DEBUG", "TRACE".

## Configuring Whether To Use Proxy From Env for S3 C++ Client
You can change whether to use proxy from env for S3 C++ client by setting the `spark.gluten.velox.s3UseProxyFromEnv` configuration. The Allowed values are:
 "false", "true".

## Configuring S3 Payload Signing Policy
You can change the S3 payload signing policy by setting the `spark.gluten.velox.s3PayloadSigningPolicy` configuration. The Allowed values are:
 "Always", "RequestDependent", "Never".  
- When set to "Always", the payload checksum is included in the signature calculation.  
- When set to "RequestDependent", the payload checksum is included based on the value returned by "AmazonWebServiceRequest::SignBody()".  

## Configuring S3 Log Location
You can set the log location by setting the `spark.gluten.velox.s3LogLocation` configuration.

# Local Caching support

Velox supports a local cache when reading data from S3. Please refer [Velox Local Cache](VeloxLocalCache.md) part for more detailed configurations.