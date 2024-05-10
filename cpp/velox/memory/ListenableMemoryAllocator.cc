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

#include "ListenableMemoryAllocator.h"

#include "velox/common/base/BitUtil.h"

namespace gluten {

bool ListenableMemoryAllocator::allocate(int64_t size, void** out) {
  updateReservation(size);
  bool succeed = delegated_->allocate(size, out);
  if (!succeed) {
    updateReservation(-size);
  }
  return succeed;
}

bool ListenableMemoryAllocator::allocateZeroFilled(int64_t nmemb, int64_t size, void** out) {
  updateReservation(size * nmemb);
  bool succeed = delegated_->allocateZeroFilled(nmemb, size, out);
  if (!succeed) {
    updateReservation(-size * nmemb);
  }
  return succeed;
}

bool ListenableMemoryAllocator::allocateAligned(uint64_t alignment, int64_t size, void** out) {
  updateReservation(size);
  bool succeed = delegated_->allocateAligned(alignment, size, out);
  if (!succeed) {
    updateReservation(-size);
  }
  return succeed;
}

bool ListenableMemoryAllocator::reallocate(void* p, int64_t size, int64_t newSize, void** out) {
  int64_t diff = newSize - size;
  updateReservation(diff);
  bool succeed = delegated_->reallocate(p, size, newSize, out);
  if (!succeed) {
    updateReservation(-diff);
  }
  return succeed;
}

bool ListenableMemoryAllocator::reallocateAligned(
    void* p,
    uint64_t alignment,
    int64_t size,
    int64_t newSize,
    void** out) {
  int64_t diff = newSize - size;
  updateReservation(diff);
  bool succeed = delegated_->reallocateAligned(p, alignment, size, newSize, out);
  if (!succeed) {
    updateReservation(-diff);
  }
  return succeed;
}

bool ListenableMemoryAllocator::free(void* p, int64_t size) {
  updateReservation(-size);
  bool succeed = delegated_->free(p, size);
  if (!succeed) {
    updateReservation(size);
  }
  return succeed;
}

int64_t ListenableMemoryAllocator::getBytes() const {
  return reservationBytes_;
}

int64_t ListenableMemoryAllocator::peakBytes() const {
  return peakBytes_;
}

void ListenableMemoryAllocator::updateReservation(int64_t size) {
  if (size == 0) {
    return;
  }
  if (size > 0) {
    if (reservationBytes_ - usedBytes_ < size) {
      auto roundSize = facebook::velox::bits::roundUp(size, blockSize_);
      listener_->allocationChanged(roundSize);
      reservationBytes_ += roundSize;
      peakBytes_ = std::max(peakBytes_, reservationBytes_);
    }
    usedBytes_ += size;
  } else {
    usedBytes_ += size;
    auto unreservedSize = ((reservationBytes_ - usedBytes_) / blockSize_) * blockSize_;
    listener_->allocationChanged(-unreservedSize);
    reservationBytes_ -= unreservedSize;
  }
}
} // namespace gluten
