# Setup, Build and Benchmark Spark/Gluten with Jupyter Notebook

This guide provides notebooks and scripts for conducting performance testing in Gluten. The standard approach involves setting up the test environment on a bare-metal machine or cloud instance and running performance tests with TPC-H/TPC-DS workloads. These scripts enable users to reproduce our performance results in their own environment.

## Environment Setup

The recommended OS is ubuntu22.04 with kernel 5.15. To prepare the environment, run [initialize.ipynb](./initialize.ipynb), which will:

- Install system dependencies and set up jupyter notebook
- Configure Hadoop and Spark
- Configure kernel parameters
- Build Gluten using Docker
- Generate TPC-H/TPC-DS tables

## Running TPC-H/TPC-DS Benchmarks

To run TPC-H/TPC-DS benchmarks, use [tpc_workload.ipynb](./tpc_workload.ipynb). You can create a copy of the notebook and modify the parameters defined in this notebook to run different workloads. However, creating and modifying a copy each time you change workloads can be inconvenient. Instead, it's recommended to use Papermill to pass parameters via the command line for greater flexibility.

The required parameters are specified in [params.yaml.template](./params.yaml.template). To use it, create your own YAML file by copying and modifying the template. The command to run the notebook is:

```bash
papermill tpc_workload.ipynb --inject-output-path -f params.yaml gluten_tpch.ipynb
```
After execution, the output notebook will be saved as `gluten_tpch.ipynb`.

If you want to use different parameters, you can specify them via the `-p` option. It will overwrite the previously defined parameters in `params.yaml`. e.g. To switch to the TPC-DS workload, run:

```bash
papermill tpc_workload.ipynb --inject-output-path -f params.yaml -p workoad tpcds gluten_tpcds.ipynb
```

Please refer to the Papermill documentation for additional usage details.

We also provide a script [run_tpc_workload.sh](./run_tpc_workload.sh). This script wraps the Papermill command, automatically renaming the output notebook with a timestamp and application ID to prevent overwriting existing output files.

## Analyzing Performance Results

You can check the **Show Performance** section in the output notebook after execution. It shows the cpu% per query, and draws charts for the cpu%, memory throughput, disk throughput/util%, network throughput and pagefaults.
