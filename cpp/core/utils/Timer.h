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

#include <chrono>

using namespace std::chrono;
using TimePoint = time_point<steady_clock, nanoseconds>;

namespace gluten {
class Timer {
 public:
  explicit Timer() = default;

  void start() {
    running_ = true;
    startTime_ = std::chrono::steady_clock::now();
  }

  void stop() {
    if (!running_) {
      return;
    }
    running_ = false;
    realTimeUsed_ += duration_cast<nanoseconds>(std::chrono::steady_clock::now() - startTime_).count();
  }

  bool running() const {
    return running_;
  }

  // REQUIRES: timer is not running
  uint64_t realTimeUsed() const {
    return realTimeUsed_;
  }

 private:
  bool running_ = false; // Is the timer running
  TimePoint startTime_{}; // If running_

  // Accumulated time so far (does not contain current slice if running_)
  uint64_t realTimeUsed_ = 0;
};
} // namespace gluten
