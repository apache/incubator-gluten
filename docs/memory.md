# Memory allocation in Gazelle Plugin

## Java memory allocation
By default, Arrow columnar vector Java API is using netty [pooledbytebuffer allocator](https://github.com/apache/arrow/blob/master/java/memory/memory-netty/src/main/java/io/netty/buffer/PooledByteBufAllocatorL.java), which will try to hold on the "free memory" by not returning back to System immediately for better performance. This will result big memory footprint on operators relying on this API, e.g., [CoalesceBatches](https://github.com/oap-project/gazelle_plugin/blob/master/native-sql-engine/core/src/main/scala/com/intel/oap/execution/CoalesceBatchesExec.scala). We changed to use unsafe API since 1.2 release, which means the freed memory will be returned to system directly. Performance tests showed the performance of this change is negatable. 

## Native memory allocation
Modern memory allocators like jemalloc will not return the just freed memory to system to achieve better performance, also a set of memory allocation pools will be used to reduce the lock contention. Recent version of glibc also [used](https://sourceware.org/bugzilla/show_bug.cgi?id=11261) similar design - by default it has 8 * cores pools, each with 64MB size. This will introduce a big memory footprint. Jemalloc's pool is smaller relatively - it has 4 * cores pools, each with 2MB size. Both glibc/jemalloc provides turning knobs to control these behaviors. Using lower number of pools can reduce the memory footprint, but also may impact the performance. It's a tradeoff on memory vs. performance.

## Turnings to reduce memory footprint

- Gazelle Plugin 1.2+
- Using jemalloc in Arrow build
```
-DARROW_USEJEMALLOC=True
```
- Build Gazelle Plugin with arrow-unsafe profile
```
mvn clean pacakge -DskipTests -Parrow-unsafe
```
- LD_PRELOAD jemalloc in each Spark executor
```
--conf spark.executorEnv.LD_PRELOAD=/path/to/libjemalloc.so
```
- Reduce number memory pools in jemalloc
```
--conf spark.executorEnv.MALLOC_CONF=narenas:2
```
- Reduce number memory pools in glibc
```
 --conf spark.executorEnv.MALLOC_ARENA_MAX=2
```