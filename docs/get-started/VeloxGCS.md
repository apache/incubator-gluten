---
layout: page
title: Using GCS with Gluten
nav_order: 5
parent: Getting-Started
---
Object stores offered by CSPs such as GCS are important for users of Gluten to store their data. This doc will discuss all details of configs, and use cases around using Gluten with object stores. In order to use a GCS endpoint as your data source, please ensure you are using the following GCS configs in your spark-defaults.conf. If you're experiencing any issues authenticating to GCS with additional auth mechanisms, please reach out to us using the 'Issues' tab.

# Working with GCS

## Installing the gcloud CLI

To access GCS Objects using Gluten and Velox, first you have to [download an install the gcloud CLI] (https://cloud.google.com/sdk/docs/install).


## Configuring GCS using a user account

This is recommended for regular users, follow the [instructions to authorize a user account](https://cloud.google.com/sdk/docs/authorizing#user-account).
After these steps, no specific configuration is required for Gluten, since the authorization was handled entirely by the gcloud tool.


## Configuring GCS using a credential file

For workloads that need to be fully automated, manually authorizing can be problematic. For such cases it is better to use a json file with the credentials.
This is described in the [instructions to configure a service account]https://cloud.google.com/sdk/docs/authorizing#service-account.

Such json file with the credetials can be passed to Gluten:

```sh
spark.hadoop.fs.gs.auth.type                         SERVICE_ACCOUNT_JSON_KEYFILE
spark.hadoop.fs.gs.auth.service.account.json.keyfile // path to the json file with the credentials.
```

## Configuring GCS endpoints

For cases when a GCS mock is used, an optional endpoint can be provided:
```sh
spark.hadoop.fs.gs.storage.root.url  // url to the mock gcs service including starting with http or https
```