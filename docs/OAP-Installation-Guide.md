# OAP Installation Guide

This document introduces how to install OAP and its dependencies on your cluster nodes by ***Conda***. 
Follow steps below on ***every node*** of your cluster to set right environment for each machine.

## Contents
  - [Prerequisites](#prerequisites)
  - [Installing OAP](#installing-oap)
  - [Configuration](#configuration)

### Prerequisites 

- **OS Requirements**  
We have tested OAP on Fedora 29 and CentOS 7.6 (kernel-4.18.16). We recommend you use **Fedora 29 CentOS 7.6 or above**. Besides, for [Memkind](https://github.com/memkind/memkind/tree/v1.10.1-rc2) we recommend you use **kernel above 3.10**.

- **Conda Requirements**   
Install Conda on your cluster nodes with below commands and follow the prompts on the installer screens.:
```bash
$ wget -c https://repo.continuum.io/miniconda/Miniconda2-latest-Linux-x86_64.sh
$ chmod +x Miniconda2-latest-Linux-x86_64.sh 
$ bash Miniconda2-latest-Linux-x86_64.sh 
```
For changes to take effect, ***close and re-open*** your current shell. 
To test your installation,  run the command `conda list` in your terminal window. A list of installed packages appears if it has been installed correctly.

### Installing OAP

Create a Conda environment and install OAP Conda package.

```bash
$ conda create -n oapenv -c conda-forge -c intel -y oap=1.2.0
```

Once finished steps above, you have completed OAP dependencies installation and OAP building, and will find built OAP jars under `$HOME/miniconda2/envs/oapenv/oap_jars`

Dependencies below are required by OAP and all of them are included in OAP Conda package, they will be automatically installed in your cluster when you Conda install OAP. Ensure you have activated environment which you created in the previous steps.

- [Arrow](https://github.com/oap-project/arrow/tree/v4.0.0-oap-1.2.0)
- [Plasma](http://arrow.apache.org/blog/2017/08/08/plasma-in-memory-object-store/)
- [Memkind](https://github.com/memkind/memkind/tree/v1.10.1)
- [Vmemcache](https://github.com/pmem/vmemcache.git)
- [HPNL](https://anaconda.org/intel/hpnl)
- [PMDK](https://github.com/pmem/pmdk)  
- [OneAPI](https://software.intel.com/content/www/us/en/develop/tools/oneapi.html)


#### Extra Steps for Shuffle Remote PMem Extension

If you use one of OAP features -- [PMem Shuffle](https://github.com/oap-project/pmem-shuffle) with **RDMA**, you need to configure and validate RDMA, please refer to [PMem Shuffle](https://github.com/oap-project/pmem-shuffle#4-configure-and-validate-rdma) for the details.


###  Configuration

Once finished steps above, make sure libraries installed by Conda can be linked by Spark, please add the following configuration settings to `$SPARK_HOME/conf/spark-defaults.conf`.

```
spark.executorEnv.LD_LIBRARY_PATH   $HOME/miniconda2/envs/oapenv/lib
spark.executor.extraLibraryPath     $HOME/miniconda2/envs/oapenv/lib
spark.driver.extraLibraryPath       $HOME/miniconda2/envs/oapenv/lib
spark.executor.extraClassPath       $HOME/miniconda2/envs/oapenv/oap_jars/$OAP_FEATURE.jar
spark.driver.extraClassPath         $HOME/miniconda2/envs/oapenv/oap_jars/$OAP_FEATURE.jar
```

Then you can follow the corresponding feature documents for more details to use them.






