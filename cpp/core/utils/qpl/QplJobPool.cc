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

#include "utils/qpl/QplJobPool.h"
#include "utils/Macros.h"

#include <arrow/util/logging.h>
#include <iostream>

namespace gluten {
namespace qpl {

std::array<qpl_job*, QplJobHWPool::MAX_JOB_NUMBER> QplJobHWPool::jobPool;
std::array<std::atomic_bool, QplJobHWPool::MAX_JOB_NUMBER> QplJobHWPool::jobLocks;
bool QplJobHWPool::jobPoolReady = false;
std::unique_ptr<uint8_t[]> QplJobHWPool::hwJobsBuffer;

QplJobHWPool& QplJobHWPool::GetInstance() {
  static QplJobHWPool pool;
  return pool;
}

QplJobHWPool::QplJobHWPool() : randomEngine(std::random_device()()), distribution(0, MAX_JOB_NUMBER - 1) {
  uint64_t initTime = 0;
  TIME_NANO(initTime, InitJobPool());
  DLOG(INFO) << "Init job pool took " << 1.0 * initTime / 1e6 << "ms";
}

QplJobHWPool::~QplJobHWPool() {
  for (uint32_t i = 0; i < MAX_JOB_NUMBER; ++i) {
    if (jobPool[i]) {
      while (!tryLockJob(i))
        ;
      qpl_fini_job(jobPool[i]);
      unLockJob(i);
      jobPool[i] = nullptr;
    }
  }
  jobPoolReady = false;
}

void QplJobHWPool::InitJobPool() {
  uint32_t jobSize = 0;
  const char* qpl_version = qpl_get_library_version();

  // Get size required for saving a single qpl job object
  qpl_get_job_size(qpl_path_hardware, &jobSize);
  // Allocate entire buffer for storing all job objects
  hwJobsBuffer = std::make_unique<uint8_t[]>(jobSize * MAX_JOB_NUMBER);
  // Initialize pool for storing all job object pointers
  // Reallocate buffer by shifting address offset for each job object.
  for (uint32_t index = 0; index < MAX_JOB_NUMBER; ++index) {
    qpl_job* qplJobPtr = reinterpret_cast<qpl_job*>(hwJobsBuffer.get() + index * jobSize);
    if (auto status = qpl_init_job(qpl_path_hardware, qplJobPtr); status != QPL_STS_OK) {
      jobPoolReady = false;
      ARROW_LOG(WARNING)
          << "Initialization of hardware-assisted DeflateQpl codec failed at index: " << index
          << ", falling back to SW codec. (Details: QplJobHWPool->qpl_init_job with error code: " << status
          << " - please refer to qpl_status in ./contrib/qpl/include/qpl/c_api/status.h). "
          << "Please check if Intel In-Memory Analytics Accelerator (IAA) is properly set up. QPL Version: "
          << qpl_version;
      return;
    }
    jobPool[index] = qplJobPtr;
    jobLocks[index].store(false);
  }
  ARROW_LOG(WARNING) << "Initialization of hardware-assisted DeflateQpl codec succeeded.";
  jobPoolReady = true;
}

qpl_job* QplJobHWPool::AcquireJob(uint32_t& jobId) {
  if (!IsJobPoolReady()) {
    return nullptr;
  }
  uint32_t retry = 0;
  auto index = distribution(randomEngine);
  while (!tryLockJob(index)) {
    index = distribution(randomEngine);
    retry++;
    if (retry > MAX_JOB_NUMBER) {
      return nullptr;
    }
  }
  jobId = MAX_JOB_NUMBER - index;
  DLOG(INFO) << "Acquired job index " << index << " after " << retry << " retries.";
  return jobPool[index];
}

void QplJobHWPool::ReleaseJob(uint32_t jobId) {
  if (IsJobPoolReady()) {
    auto index = MAX_JOB_NUMBER - jobId;
    unLockJob(index);
  }
}

bool QplJobHWPool::tryLockJob(uint32_t index) {
  CheckJobIndex(index);
  bool expected = false;
  return jobLocks[index].compare_exchange_strong(expected, true);
}

void QplJobHWPool::unLockJob(uint32_t index) {
  CheckJobIndex(index);
  jobLocks[index].store(false);
}

} // namespace qpl
} // namespace gluten
