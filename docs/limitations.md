# Limitations for Gazelle Plugin

## Spark compability
Gazelle Plugin currenlty works with Spark 3.0.0 only. There are still some trouble with latest Shuffle/AQE API from Spark 3.0.1, 3.0.2 or 3.1.x.

## Operator limitations
All performance critical operators in TPC-H/TPC-DS should be supported. For those unsupported operators, Gazelle Plugin will automatically fallback to row operators in vanilla Spark.

### Columnar Projection with Filter
We used 16 bit selection vector for filter so the max batch size need to be < 65536

### Columnar Sort
Columnar Sort does not support spill to disk yet. To reduce the peak memory usage, we used smaller data structure(uin16_t), so this limits 
- the max batch size to be < 65536
- the number of batches in one partiton to be < 65536


