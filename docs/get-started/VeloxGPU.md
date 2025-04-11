---
layout: page
title: Velox GPU
nav_order: 9
parent: Getting-Started
---

# GPU Support in Velox Backend

This is an experimental feature in velox, so as Gluten. Now it only supports OrderBy operator.

Velox has several GPU support implementations, Gluten only enables cudf.

## GPU environment

It requires to install the cuda 12.8.0, driver and the nvidia-container-toolkit.

Refers to [start_cudf.sh](https://github.com/apache/incubator-gluten/tree/main/dev/start_cudf.sh) 
to set the ``host`` environment and start the container.

> You may need to reboot after install the GPU driver.

## GPU implementation

Invokes [CUDF](https://docs.rapids.ai/api/cudf/stable/libcudf_docs/) API to support the Velox operators.

Suppose we have a velox PlanNode, convert it to the GPU operator or CPU operator depending on the
config `spark.gluten.sql.columnar.cudf` which decides registering cudf driver adapter or not.

Besides, config `spark.gluten.debug.enabled` true can print the operator replacement information.

## Docker images
This docker image contains Spark at env $SPARK_HOME, Gluten at /opt/gluten, take a try if you are interested on it.
The Gluten has been built with Spark3.4.
```
docker pull apache/gluten:centos-9-jdk8-cudf
docker run --name gpu_gluten_container --gpus all -itd apache/gluten:centos-9-jdk8-cudf
```

# Branch
The [PR](https://github.com/facebookincubator/velox/pull/12735/) has not been merged to
facebookincubator/velox, so use a fixed Gluten branch `cudf` and corresponding oap-project velox branch `cudf`.

# Relevant link

Cuda Toolkit 12.8: https://developer.nvidia.com/cuda-downloads?target_os=Linux
Cuda Container ToolKit: https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/latest/install-guide.html
Cudf Document: https://docs.rapids.ai/api/libcudf/legacy/namespacecudf
