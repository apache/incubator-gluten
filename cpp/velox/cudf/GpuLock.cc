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

#include "GpuLock.h"
#include <mutex>
#include <condition_variable>
#include <optional>
#include <stdexcept>

namespace gluten {

static std::mutex gGpuMutex;
static std::condition_variable gGpuCv;
static std::optional<std::thread::id> gGpuOwner;

void lockGpu() {
    std::thread::id tid = std::this_thread::get_id();
    std::unique_lock<std::mutex> lock(gGpuMutex);
    if (gGpuOwner == tid) {
        // Reentrant call from the same thread — do nothing
        return;
    }


    // Wait until the GPU lock becomes available
    gGpuCv.wait(lock, [] {
        return !gGpuOwner.has_value();
    });

    // Acquire ownership
    gGpuOwner = tid;
}

void unlockGpu() {
    std::thread::id tid = std::this_thread::get_id();
    std::unique_lock<std::mutex> lock(gGpuMutex);
    if (!gGpuOwner.has_value() || gGpuOwner != tid) {
        throw std::runtime_error("unlockGpu() called by non-owner thread!");
    }

    // Release ownership
    gGpuOwner = std::nullopt;

    // Notify one waiting thread
    lock.unlock();
    gGpuCv.notify_one();
}


} // namespace gluten
