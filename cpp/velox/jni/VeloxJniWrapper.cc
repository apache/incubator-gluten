/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <jni.h>

#include <glog/logging.h>
#include <jni/JniCommon.h>
#include <velox/connectors/hive/PartitionIdGenerator.h>
#include <velox/exec/OperatorUtils.h>

#include <exception>
#include "JniUdf.h"
#include "compute/Runtime.h"
#include "compute/VeloxBackend.h"
#include "compute/VeloxRuntime.h"
#include "config/GlutenConfig.h"
#include "jni/JniError.h"
#include "jni/JniFileSystem.h"
#include "memory/VeloxColumnarBatch.h"
#include "memory/VeloxMemoryManager.h"
#include "shuffle/rss/RssPartitionWriter.h"
#include "substrait/SubstraitToVeloxPlanValidator.h"
#include "utils/ObjectStore.h"
#include "utils/VeloxBatchResizer.h"
#include "velox/common/base/BloomFilter.h"
#include "velox/common/file/FileSystems.h"

#ifdef GLUTEN_ENABLE_GPU
#include "cudf/CudfPlanValidator.h"
#endif

#ifdef GLUTEN_ENABLE_ENHANCED_FEATURES
#include "IcebergNestedField.pb.h"
#endif

using namespace gluten;
using namespace facebook;

namespace {
jclass infoCls;
jmethodID infoClsInitMethod;

jclass blockStripesClass;
jmethodID blockStripesConstructor;
} // namespace

#ifdef __cplusplus
extern "C" {
#endif

jint JNI_OnLoad(JavaVM* vm, void*) {
  JNIEnv* env;
  if (vm->GetEnv(reinterpret_cast<void**>(&env), jniVersion) != JNI_OK) {
    return JNI_ERR;
  }

  getJniCommonState()->ensureInitialized(env);
  getJniErrorState()->ensureInitialized(env);
  initVeloxJniFileSystem(env);
  initVeloxJniUDF(env);

  infoCls = createGlobalClassReferenceOrError(env, "Lorg/apache/gluten/validate/NativePlanValidationInfo;");
  infoClsInitMethod = getMethodIdOrError(env, infoCls, "<init>", "(ILjava/lang/String;)V");

  blockStripesClass =
      createGlobalClassReferenceOrError(env, "Lorg/apache/spark/sql/execution/datasources/BlockStripes;");
  blockStripesConstructor = getMethodIdOrError(env, blockStripesClass, "<init>", "(J[J[II[[B)V");

  DLOG(INFO) << "Loaded Velox backend.";

  return jniVersion;
}

void JNI_OnUnload(JavaVM* vm, void*) {
  JNIEnv* env;
  vm->GetEnv(reinterpret_cast<void**>(&env), jniVersion);

  env->DeleteGlobalRef(blockStripesClass);
  env->DeleteGlobalRef(infoCls);

  finalizeVeloxJniUDF(env);
  finalizeVeloxJniFileSystem(env);
  getJniErrorState()->close();
  getJniCommonState()->close();
  google::ShutdownGoogleLogging();
}

JNIEXPORT void JNICALL Java_org_apache_gluten_init_NativeBackendInitializer_initialize( // NOLINT
    JNIEnv* env,
    jclass,
    jobject jListener,
    jbyteArray conf) {
  JNI_METHOD_START
  JavaVM* vm;
  if (env->GetJavaVM(&vm) != JNI_OK) {
    throw GlutenException("Unable to get JavaVM instance");
  }
  auto safeArray = getByteArrayElementsSafe(env, conf);
  // Create a global allocation listener that reserves global off-heap memory from Java-side GlobalOffHeapMemory utility
  // class.
  std::unique_ptr<AllocationListener> listener = std::make_unique<SparkAllocationListener>(vm, jListener);
  auto sparkConf = parseConfMap(env, safeArray.elems(), safeArray.length());
  VeloxBackend::create(std::move(listener), sparkConf);
  JNI_METHOD_END()
}

JNIEXPORT void JNICALL Java_org_apache_gluten_init_NativeBackendInitializer_shutdown( // NOLINT
    JNIEnv* env,
    jclass) {
  JNI_METHOD_START
  VeloxBackend::get()->tearDown();
  JNI_METHOD_END()
}

JNIEXPORT void JNICALL Java_org_apache_gluten_udf_UdfJniWrapper_registerFunctionSignatures( // NOLINT
    JNIEnv* env,
    jclass) {
  JNI_METHOD_START
  jniRegisterFunctionSignatures(env);
  JNI_METHOD_END()
}

JNIEXPORT jobject JNICALL
Java_org_apache_gluten_vectorized_PlanEvaluatorJniWrapper_nativeValidateWithFailureReason( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jbyteArray planArray) {
  JNI_METHOD_START
  const auto ctx = getRuntime(env, wrapper);
  const auto safeArray = getByteArrayElementsSafe(env, planArray);
  const auto planData = safeArray.elems();
  const auto planSize = env->GetArrayLength(planArray);
  const auto runtime = dynamic_cast<VeloxRuntime*>(ctx);
  if (runtime->debugModeEnabled()) {
    try {
      const auto jsonPlan = substraitFromPbToJson("Plan", planData, planSize);
      LOG(INFO) << std::string(50, '#') << " received substrait::Plan: for validation";
      LOG(INFO) << jsonPlan;
    } catch (const std::exception& e) {
      LOG(WARNING) << "Error converting Substrait plan for validation to JSON: " << e.what();
    }
  }

  const auto pool = defaultLeafVeloxMemoryPool().get();
  SubstraitToVeloxPlanValidator planValidator(pool);
  ::substrait::Plan subPlan;
  parseProtobuf(planData, planSize, &subPlan);

  try {
    const auto isSupported = planValidator.validate(subPlan);
    const auto logs = planValidator.getValidateLog();
    std::string concatLog;
    for (int i = 0; i < logs.size(); i++) {
      concatLog += logs[i] + "@";
    }
    return env->NewObject(infoCls, infoClsInitMethod, isSupported, env->NewStringUTF(concatLog.c_str()));
  } catch (std::invalid_argument& e) {
    LOG(INFO) << "Failed to validate substrait plan because " << e.what();
    return env->NewObject(infoCls, infoClsInitMethod, false, env->NewStringUTF(""));
  }
  JNI_METHOD_END(nullptr)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_columnarbatch_VeloxColumnarBatchJniWrapper_from( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong handle) {
  JNI_METHOD_START
  auto ctx = getRuntime(env, wrapper);
  auto runtime = dynamic_cast<VeloxRuntime*>(ctx);

  auto batch = ObjectStore::retrieve<ColumnarBatch>(handle);
  auto newBatch = VeloxColumnarBatch::from(runtime->memoryManager()->getLeafMemoryPool().get(), batch);
  return ctx->saveObject(newBatch);
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_columnarbatch_VeloxColumnarBatchJniWrapper_compose( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlongArray batchHandles) {
  JNI_METHOD_START
  auto ctx = getRuntime(env, wrapper);
  auto runtime = dynamic_cast<VeloxRuntime*>(ctx);

  int handleCount = env->GetArrayLength(batchHandles);
  auto safeArray = getLongArrayElementsSafe(env, batchHandles);

  std::vector<std::shared_ptr<ColumnarBatch>> batches;
  for (int i = 0; i < handleCount; ++i) {
    int64_t handle = safeArray.elems()[i];
    auto batch = ObjectStore::retrieve<ColumnarBatch>(handle);
    batches.push_back(batch);
  }
  auto newBatch = VeloxColumnarBatch::compose(runtime->memoryManager()->getLeafMemoryPool().get(), std::move(batches));
  return ctx->saveObject(newBatch);
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_utils_VeloxBloomFilterJniWrapper_empty( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jint capacity) {
  JNI_METHOD_START
  auto ctx = getRuntime(env, wrapper);
  auto filter = std::make_shared<velox::BloomFilter<std::allocator<uint64_t>>>();
  filter->reset(capacity);
  GLUTEN_CHECK(filter->isSet(), "Bloom-filter is not initialized");
  return ctx->saveObject(filter);
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_utils_VeloxBloomFilterJniWrapper_init( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jbyteArray data) {
  JNI_METHOD_START
  auto safeArray = getByteArrayElementsSafe(env, data);
  auto ctx = getRuntime(env, wrapper);
  auto filter = std::make_shared<velox::BloomFilter<std::allocator<uint64_t>>>();
  uint8_t* serialized = safeArray.elems();
  filter->merge(reinterpret_cast<char*>(serialized));
  return ctx->saveObject(filter);
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT void JNICALL Java_org_apache_gluten_utils_VeloxBloomFilterJniWrapper_insertLong( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong handle,
    jlong item) {
  JNI_METHOD_START
  auto filter = ObjectStore::retrieve<velox::BloomFilter<std::allocator<uint64_t>>>(handle);
  GLUTEN_CHECK(filter->isSet(), "Bloom-filter is not initialized");
  filter->insert(folly::hasher<int64_t>()(item));
  JNI_METHOD_END()
}

JNIEXPORT jboolean JNICALL Java_org_apache_gluten_utils_VeloxBloomFilterJniWrapper_mightContainLong( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong handle,
    jlong item) {
  JNI_METHOD_START
  auto filter = ObjectStore::retrieve<velox::BloomFilter<std::allocator<uint64_t>>>(handle);
  GLUTEN_CHECK(filter->isSet(), "Bloom-filter is not initialized");
  bool out = filter->mayContain(folly::hasher<int64_t>()(item));
  return out;
  JNI_METHOD_END(false)
}

JNIEXPORT jboolean JNICALL
Java_org_apache_gluten_utils_VeloxBloomFilterJniWrapper_mightContainLongOnSerializedBloom( // NOLINT
    JNIEnv* env,
    jclass,
    jlong address,
    jlong item) {
  JNI_METHOD_START
  bool out = velox::BloomFilter<>::mayContain(reinterpret_cast<const char*>(address), folly::hasher<int64_t>()(item));
  return out;
  JNI_METHOD_END(false)
}

namespace {
static std::vector<char> serialize(BloomFilter<std::allocator<uint64_t>>* bf) {
  uint32_t size = bf->serializedSize();
  std::vector<char> buffer;
  buffer.reserve(size);
  char* data = buffer.data();
  bf->serialize(data);
  return buffer;
}
} // namespace

JNIEXPORT void JNICALL Java_org_apache_gluten_utils_VeloxBloomFilterJniWrapper_mergeFrom( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong handle,
    jlong other) {
  JNI_METHOD_START
  auto to = ObjectStore::retrieve<velox::BloomFilter<std::allocator<uint64_t>>>(handle);
  auto from = ObjectStore::retrieve<velox::BloomFilter<std::allocator<uint64_t>>>(other);
  GLUTEN_CHECK(to->isSet(), "Bloom-filter is not initialized");
  GLUTEN_CHECK(from->isSet(), "Bloom-filter is not initialized");
  std::vector<char> serialized = serialize(from.get());
  to->merge(serialized.data());
  JNI_METHOD_END()
}

JNIEXPORT jbyteArray JNICALL Java_org_apache_gluten_utils_VeloxBloomFilterJniWrapper_serialize( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong handle) {
  JNI_METHOD_START
  auto filter = ObjectStore::retrieve<velox::BloomFilter<std::allocator<uint64_t>>>(handle);
  GLUTEN_CHECK(filter->isSet(), "Bloom-filter is not initialized");
  std::vector<char> buffer = serialize(filter.get());
  auto size = buffer.capacity();
  jbyteArray out = env->NewByteArray(size);
  env->SetByteArrayRegion(out, 0, size, reinterpret_cast<jbyte*>(buffer.data()));
  return out;
  JNI_METHOD_END(nullptr)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_utils_VeloxBatchResizerJniWrapper_create( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jint minOutputBatchSize,
    jint maxOutputBatchSize,
    jobject jIter) {
  JNI_METHOD_START
  auto ctx = getRuntime(env, wrapper);
  auto pool = dynamic_cast<VeloxMemoryManager*>(ctx->memoryManager())->getLeafMemoryPool();
  auto iter = makeJniColumnarBatchIterator(env, jIter, ctx);
  auto appender = std::make_shared<ResultIterator>(
      std::make_unique<VeloxBatchResizer>(pool.get(), minOutputBatchSize, maxOutputBatchSize, std::move(iter)));
  return ctx->saveObject(appender);
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT jboolean JNICALL
Java_org_apache_gluten_utils_VeloxFileSystemValidationJniWrapper_allSupportedByRegisteredFileSystems( // NOLINT
    JNIEnv* env,
    jclass,
    jobjectArray stringArray) {
  JNI_METHOD_START
  int size = env->GetArrayLength(stringArray);
  for (int i = 0; i < size; i++) {
    jstring string = (jstring)(env->GetObjectArrayElement(stringArray, i));
    std::string path = jStringToCString(env, string);
    if (!velox::filesystems::isPathSupportedByRegisteredFileSystems(path)) {
      return false;
    }
  }
  return true;
  JNI_METHOD_END(false)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_datasource_VeloxDataSourceJniWrapper_init( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jstring filePath,
    jlong cSchema,
    jbyteArray options) {
  JNI_METHOD_START
  auto ctx = gluten::getRuntime(env, wrapper);
  auto runtime = dynamic_cast<VeloxRuntime*>(ctx);

  ObjectHandle handle = kInvalidObjectHandle;

  if (cSchema == -1) {
    // Only inspect the schema and not write
    handle = ctx->saveObject(runtime->createDataSource(jStringToCString(env, filePath), nullptr));
  } else {
    auto safeArray = gluten::getByteArrayElementsSafe(env, options);
    auto datasourceOptions = gluten::parseConfMap(env, safeArray.elems(), safeArray.length());
    auto& sparkConf = ctx->getConfMap();
    datasourceOptions.insert(sparkConf.begin(), sparkConf.end());
    auto schema = gluten::arrowGetOrThrow(arrow::ImportSchema(reinterpret_cast<struct ArrowSchema*>(cSchema)));
    handle = ctx->saveObject(runtime->createDataSource(jStringToCString(env, filePath), schema));
    auto datasource = ObjectStore::retrieve<VeloxDataSource>(handle);
    datasource->init(datasourceOptions);
  }

  return handle;
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT void JNICALL Java_org_apache_gluten_datasource_VeloxDataSourceJniWrapper_inspectSchema( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong dsHandle,
    jlong cSchema) {
  JNI_METHOD_START
  auto datasource = ObjectStore::retrieve<VeloxDataSource>(dsHandle);
  datasource->inspectSchema(reinterpret_cast<struct ArrowSchema*>(cSchema));
  JNI_METHOD_END()
}

JNIEXPORT void JNICALL Java_org_apache_gluten_datasource_VeloxDataSourceJniWrapper_close( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong dsHandle) {
  JNI_METHOD_START
  auto datasource = ObjectStore::retrieve<VeloxDataSource>(dsHandle);
  datasource->close();
  ObjectStore::release(dsHandle);
  JNI_METHOD_END()
}

JNIEXPORT void JNICALL Java_org_apache_gluten_datasource_VeloxDataSourceJniWrapper_writeBatch( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong dsHandle,
    jlong batchHandle) {
  JNI_METHOD_START
  auto datasource = ObjectStore::retrieve<VeloxDataSource>(dsHandle);
  auto batch = ObjectStore::retrieve<ColumnarBatch>(batchHandle);
  datasource->write(batch);
  JNI_METHOD_END()
}

JNIEXPORT jobject JNICALL
Java_org_apache_gluten_datasource_VeloxDataSourceJniWrapper_splitBlockByPartitionAndBucket( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong batchHandle,
    jintArray partitionColIndices,
    jboolean hasBucket) {
  JNI_METHOD_START

  GLUTEN_CHECK(!hasBucket, "Bucketing not supported by splitBlockByPartitionAndBucket");

  const auto ctx = gluten::getRuntime(env, wrapper);
  const auto batch = ObjectStore::retrieve<ColumnarBatch>(batchHandle);

  auto partitionKeyArray = gluten::getIntArrayElementsSafe(env, partitionColIndices);
  int numPartitionKeys = partitionKeyArray.length();
  std::vector<uint32_t> partitionColIndicesVec;
  for (int i = 0; i < numPartitionKeys; ++i) {
    const auto partitionColumnIndex = partitionKeyArray.elems()[i];
    GLUTEN_CHECK(partitionColumnIndex < batch->numColumns(), "Partition column index overflow");
    partitionColIndicesVec.emplace_back(partitionColumnIndex);
  }

  std::vector<int32_t> dataColIndicesVec;
  for (int i = 0; i < batch->numColumns(); ++i) {
    if (std::find(partitionColIndicesVec.begin(), partitionColIndicesVec.end(), i) == partitionColIndicesVec.end()) {
      // The column is not a partition column. Add it to the data column vector.
      dataColIndicesVec.emplace_back(i);
    }
  }

  auto pool = dynamic_cast<VeloxMemoryManager*>(ctx->memoryManager())->getLeafMemoryPool();
  const auto veloxBatch = VeloxColumnarBatch::from(pool.get(), batch);
  const auto inputRowVector = veloxBatch->getRowVector();
  const auto numRows = inputRowVector->size();

  connector::hive::PartitionIdGenerator idGen{
      asRowType(inputRowVector->type()), partitionColIndicesVec, 128, pool.get(), true};
  raw_vector<uint64_t> partitionIds{};
  idGen.run(inputRowVector, partitionIds);
  GLUTEN_CHECK(partitionIds.size() == numRows, "Mismatched number of partition ids");
  const auto numPartitions = static_cast<int32_t>(idGen.numPartitions());

  std::vector<vector_size_t> partitionSizes(numPartitions);
  std::vector<BufferPtr> partitionRows(numPartitions);
  std::vector<vector_size_t*> rawPartitionRows(numPartitions);
  std::fill(partitionSizes.begin(), partitionSizes.end(), 0);

  for (auto row = 0; row < numRows; ++row) {
    const auto partitionId = partitionIds[row];
    ++partitionSizes[partitionId];
  }

  for (int partitionId = 0; partitionId < numPartitions; ++partitionId) {
    partitionRows[partitionId] = allocateIndices(partitionSizes[partitionId], pool.get());
    rawPartitionRows[partitionId] = partitionRows[partitionId]->asMutable<vector_size_t>();
  }

  std::vector<vector_size_t> partitionNextRowOffset(numPartitions);
  std::fill(partitionNextRowOffset.begin(), partitionNextRowOffset.end(), 0);
  for (auto row = 0; row < numRows; ++row) {
    const auto partitionId = partitionIds[row];
    rawPartitionRows[partitionId][partitionNextRowOffset[partitionId]] = row;
    ++partitionNextRowOffset[partitionId];
  }

  jobjectArray partitionHeadingRowBytesArray = env->NewObjectArray(numPartitions, env->FindClass("[B"), nullptr);
  std::vector<jlong> partitionBatchHandles(numPartitions);

  for (int partitionId = 0; partitionId < numPartitions; ++partitionId) {
    const vector_size_t partitionSize = partitionSizes[partitionId];
    if (partitionSize == 0) {
      continue;
    }

    const RowVectorPtr rowVector = partitionSize == inputRowVector->size()
        ? inputRowVector
        : exec::wrap(partitionSize, partitionRows[partitionId], inputRowVector);

    const std::shared_ptr<VeloxColumnarBatch> partitionBatch = std::make_shared<VeloxColumnarBatch>(rowVector);
    const std::shared_ptr<VeloxColumnarBatch> partitionBatchWithoutPartitionColumns =
        partitionBatch->select(pool.get(), dataColIndicesVec);
    partitionBatchHandles[partitionId] = ctx->saveObject(partitionBatchWithoutPartitionColumns);
    const auto headingRow = partitionBatch->toUnsafeRow(0);
    const auto headingRowBytes = headingRow.data();
    const auto headingRowNumBytes = headingRow.size();

    jbyteArray jHeadingRowBytes = env->NewByteArray(headingRowNumBytes);
    env->SetByteArrayRegion(jHeadingRowBytes, 0, headingRowNumBytes, reinterpret_cast<const jbyte*>(headingRowBytes));
    env->SetObjectArrayElement(partitionHeadingRowBytesArray, partitionId, jHeadingRowBytes);
  }

  jlongArray partitionBatchArray = env->NewLongArray(numPartitions);
  env->SetLongArrayRegion(partitionBatchArray, 0, numPartitions, partitionBatchHandles.data());

  jobject blockStripes = env->NewObject(
      blockStripesClass,
      blockStripesConstructor,
      batchHandle,
      partitionBatchArray,
      nullptr,
      batch->numColumns(),
      partitionHeadingRowBytesArray);
  return blockStripes;
  JNI_METHOD_END(nullptr)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_columnarbatch_VeloxColumnarBatchJniWrapper_slice( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong veloxBatchHandle,
    jint offset,
    jint limit) {
  JNI_METHOD_START
  auto ctx = getRuntime(env, wrapper);
  auto batch = ObjectStore::retrieve<ColumnarBatch>(veloxBatchHandle);

  auto numRows = batch->numRows();
  if (limit >= numRows) {
    return veloxBatchHandle;
  }

  auto veloxBatch = std::dynamic_pointer_cast<VeloxColumnarBatch>(batch);
  VELOX_CHECK_NOT_NULL(veloxBatch, "Expected VeloxColumnarBatch but got a different type.");

  auto rowVector = veloxBatch->getRowVector();
  auto prunedVector = rowVector->slice(offset, limit);

  auto prunedRowVector = std::dynamic_pointer_cast<facebook::velox::RowVector>(prunedVector);
  VELOX_CHECK_NOT_NULL(prunedRowVector, "Expected RowVector but got a different type.");

  auto prunedBatch = std::make_shared<VeloxColumnarBatch>(prunedRowVector);

  jlong prunedHandle = ctx->saveObject(prunedBatch);
  return prunedHandle;

  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT void JNICALL Java_org_apache_gluten_monitor_VeloxMemoryProfiler_start( // NOLINT
    JNIEnv* env,
    jclass) {
  JNI_METHOD_START
#ifdef ENABLE_JEMALLOC_STATS
  bool active = true;
  mallctl("prof.active", NULL, NULL, &active, sizeof(bool));
#endif
  JNI_METHOD_END()
}

JNIEXPORT void JNICALL Java_org_apache_gluten_monitor_VeloxMemoryProfiler_dump( // NOLINT
    JNIEnv* env,
    jclass) {
  JNI_METHOD_START
#ifdef ENABLE_JEMALLOC_STATS
  mallctl("prof.dump", NULL, NULL, NULL, 0);
#endif
  JNI_METHOD_END()
}

JNIEXPORT void JNICALL Java_org_apache_gluten_monitor_VeloxMemoryProfiler_stop( // NOLINT
    JNIEnv* env,
    jclass) {
  JNI_METHOD_START
#ifdef ENABLE_JEMALLOC_STATS
  bool active = false;
  mallctl("prof.active", NULL, NULL, &active, sizeof(bool));
#endif
  JNI_METHOD_END()
}

JNIEXPORT jlong JNICALL
Java_org_apache_gluten_vectorized_CelebornPartitionWriterJniWrapper_createPartitionWriter( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jint numPartitions,
    jstring codecJstr,
    jstring codecBackendJstr,
    jint compressionLevel,
    jint compressionBufferSize,
    jint pushBufferMaxSize,
    jlong sortBufferMaxSize,
    jobject partitionPusher) {
  JNI_METHOD_START
  JavaVM* vm;
  if (env->GetJavaVM(&vm) != JNI_OK) {
    throw GlutenException("Unable to get JavaVM instance");
  }

  const auto ctx = getRuntime(env, wrapper);

  jclass celebornPartitionPusherClass =
      createGlobalClassReferenceOrError(env, "Lorg/apache/spark/shuffle/CelebornPartitionPusher;");
  jmethodID celebornPushPartitionDataMethod =
      getMethodIdOrError(env, celebornPartitionPusherClass, "pushPartitionData", "(I[BI)I");
  std::shared_ptr<JavaRssClient> celebornClient =
      std::make_shared<JavaRssClient>(vm, partitionPusher, celebornPushPartitionDataMethod);

  auto partitionWriterOptions = std::make_shared<RssPartitionWriterOptions>(
      compressionBufferSize,
      pushBufferMaxSize > 0 ? pushBufferMaxSize : kDefaultPushMemoryThreshold,
      sortBufferMaxSize > 0 ? sortBufferMaxSize : kDefaultSortBufferThreshold);

  auto partitionWriter = std::make_shared<RssPartitionWriter>(
      numPartitions,
      createArrowIpcCodec(getCompressionType(env, codecJstr), getCodecBackend(env, codecBackendJstr), compressionLevel),
      ctx->memoryManager(),
      partitionWriterOptions,
      celebornClient);

  return ctx->saveObject(partitionWriter);
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT jlong JNICALL
Java_org_apache_gluten_vectorized_UnifflePartitionWriterJniWrapper_createPartitionWriter( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jint numPartitions,
    jstring codecJstr,
    jstring codecBackendJstr,
    jint compressionLevel,
    jint compressionBufferSize,
    jint pushBufferMaxSize,
    jlong sortBufferMaxSize,
    jobject partitionPusher) {
  JNI_METHOD_START
  JavaVM* vm;
  if (env->GetJavaVM(&vm) != JNI_OK) {
    throw GlutenException("Unable to get JavaVM instance");
  }

  const auto ctx = getRuntime(env, wrapper);

  jclass unifflePartitionPusherClass =
      createGlobalClassReferenceOrError(env, "Lorg/apache/spark/shuffle/writer/PartitionPusher;");
  jmethodID unifflePushPartitionDataMethod =
      getMethodIdOrError(env, unifflePartitionPusherClass, "pushPartitionData", "(I[BI)I");
  std::shared_ptr<JavaRssClient> uniffleClient =
      std::make_shared<JavaRssClient>(vm, partitionPusher, unifflePushPartitionDataMethod);

  auto partitionWriterOptions = std::make_shared<RssPartitionWriterOptions>(
      compressionBufferSize,
      pushBufferMaxSize > 0 ? pushBufferMaxSize : kDefaultPushMemoryThreshold,
      sortBufferMaxSize > 0 ? sortBufferMaxSize : kDefaultSortBufferThreshold);

  auto partitionWriter = std::make_shared<RssPartitionWriter>(
      numPartitions,
      createArrowIpcCodec(getCompressionType(env, codecJstr), getCodecBackend(env, codecBackendJstr), compressionLevel),
      ctx->memoryManager(),
      partitionWriterOptions,
      uniffleClient);

  return ctx->saveObject(partitionWriter);
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT jboolean JNICALL Java_org_apache_gluten_config_ConfigJniWrapper_isEnhancedFeaturesEnabled( // NOLINT
    JNIEnv* env,
    jclass) {
#ifdef GLUTEN_ENABLE_ENHANCED_FEATURES
  return true;
#else
  return false;
#endif
}

#ifdef GLUTEN_ENABLE_GPU
JNIEXPORT jboolean JNICALL Java_org_apache_gluten_cudf_VeloxCudfPlanValidatorJniWrapper_validate( // NOLINT
    JNIEnv* env,
    jclass,
    jbyteArray planArr) {
  JNI_METHOD_START
  auto safePlanArray = getByteArrayElementsSafe(env, planArr);
  auto planSize = env->GetArrayLength(planArr);
  ::substrait::Plan substraitPlan;
  parseProtobuf(safePlanArray.elems(), planSize, &substraitPlan);
  // get the task and driver, validate the plan, if return all operator except table scan is offloaded, validate true.
  return CudfPlanValidator::validate(substraitPlan);
  JNI_METHOD_END(false)
}
#endif

#ifdef GLUTEN_ENABLE_ENHANCED_FEATURES
JNIEXPORT jlong JNICALL Java_org_apache_gluten_execution_IcebergWriteJniWrapper_init( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong cSchema,
    jint format,
    jstring directory,
    jstring codecJstr,
    jbyteArray partition,
  jbyteArray fieldBytes) {
  JNI_METHOD_START
  auto ctx = getRuntime(env, wrapper);
  auto runtime = dynamic_cast<VeloxRuntime*>(ctx);
  auto backendConf = VeloxBackend::get()->getBackendConf()->rawConfigs();
  auto sparkConf = ctx->getConfMap();
  sparkConf.merge(backendConf);
  auto safeArray = gluten::getByteArrayElementsSafe(env, partition);
  auto arrowSchema = reinterpret_cast<struct ArrowSchema*>(cSchema);
  auto rowType = asRowType(importFromArrow(*arrowSchema));
  ArrowSchemaRelease(arrowSchema);
  auto spec = parseIcebergPartitionSpec(safeArray.elems(), safeArray.length(), rowType);
  auto safeArrayField = gluten::getByteArrayElementsSafe(env, fieldBytes);
  gluten::IcebergNestedField protoField;
  gluten::parseProtobuf(safeArrayField.elems(), safeArrayField.length(), &protoField);
  return ctx->saveObject(runtime->createIcebergWriter(
      rowType,
      format,
      jStringToCString(env, directory),
      facebook::velox::common::stringToCompressionKind(jStringToCString(env, codecJstr)),
      spec,
      protoField,
      sparkConf));
  JNI_METHOD_END(kInvalidObjectHandle)
}

JNIEXPORT void JNICALL Java_org_apache_gluten_execution_IcebergWriteJniWrapper_write( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong writerHandle,
    jlong batchHandle) {
  JNI_METHOD_START
  auto batch = ObjectStore::retrieve<ColumnarBatch>(batchHandle);
  auto writer = ObjectStore::retrieve<IcebergWriter>(writerHandle);
  writer->write(*(std::dynamic_pointer_cast<VeloxColumnarBatch>(batch)));
  JNI_METHOD_END()
}

JNIEXPORT jobjectArray JNICALL Java_org_apache_gluten_execution_IcebergWriteJniWrapper_commit( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong writerHandle) {
  JNI_METHOD_START
  auto writer = ObjectStore::retrieve<IcebergWriter>(writerHandle);
  auto commitMessages = writer->commit();
  jobjectArray ret =
      env->NewObjectArray(commitMessages.size(), env->FindClass("java/lang/String"), env->NewStringUTF(""));
  for (auto i = 0; i < commitMessages.size(); i++) {
    env->SetObjectArrayElement(ret, i, env->NewStringUTF(commitMessages[i].data()));
  }
  return ret;

  JNI_METHOD_END(nullptr)
}
#endif

#ifdef __cplusplus
}
#endif
