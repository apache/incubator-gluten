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

#pragma once

#include "velox/dwio/common/DirectBufferedInput.h"

namespace gluten {

class GlutenDirectBufferedInput : public facebook::velox::dwio::common::BufferedInput {
 public:
  static constexpr int32_t kTinySize = 2'000;

  GlutenDirectBufferedInput(
      std::shared_ptr<facebook::velox::ReadFile> readFile,
      const facebook::velox::dwio::common::MetricsLogPtr& metricsLog,
      facebook::velox::StringIdLease fileNum,
      std::shared_ptr<facebook::velox::cache::ScanTracker> tracker,
      facebook::velox::StringIdLease groupId,
      std::shared_ptr<facebook::velox::io::IoStatistics> ioStats,
      std::shared_ptr<facebook::velox::filesystems::File::IoStats> fsStats,
      folly::Executor* executor,
      const facebook::velox::io::ReaderOptions& readerOptions,
      folly::F14FastMap<std::string, std::string> fileReadOps = {})
      : BufferedInput(
            std::move(readFile),
            readerOptions.memoryPool(),
            metricsLog,
            ioStats.get(),
            fsStats.get(),
            kMaxMergeDistance,
            std::nullopt,
            std::move(fileReadOps)),
        fileNum_(std::move(fileNum)),
        tracker_(std::move(tracker)),
        groupId_(std::move(groupId)),
        ioStats_(std::move(ioStats)),
        fsStats_(std::move(fsStats)),
        executor_(executor),
        fileSize_(input_->getLength()),
        options_(readerOptions) {}

  ~GlutenDirectBufferedInput() override {
    requests_.clear();
    for (auto& load : coalescedLoads_) {
      if (load->state() == facebook::velox::cache::CoalescedLoad::State::kLoading) {
        folly::SemiFuture<bool> waitFuture(false);
        if (!load->loadOrFuture(&waitFuture)) {
          auto& exec = folly::QueuedImmediateExecutor::instance();
          std::move(waitFuture).via(&exec).wait();
        }
      }
      load->cancel();
    }
    coalescedLoads_.clear();
  }

  std::unique_ptr<facebook::velox::dwio::common::SeekableInputStream> enqueue(
      facebook::velox::common::Region region,
      const facebook::velox::dwio::common::StreamIdentifier* sid) override;

  bool supportSyncLoad() const override {
    return false;
  }

  void load(const facebook::velox::dwio::common::LogType /*unused*/) override;

  bool isBuffered(uint64_t offset, uint64_t length) const override;

  bool shouldPreload(int32_t numPages = 0) override;

  bool shouldPrefetchStripes() const override {
    return false;
  }

  void setNumStripes(int32_t numStripes) override {
    auto* stats = tracker_->fileGroupStats();
    if (stats) {
      stats->recordFile(fileNum_.id(), groupId_.id(), numStripes);
    }
  }

  virtual std::unique_ptr<facebook::velox::dwio::common::BufferedInput> clone() const override {
    return std::unique_ptr<GlutenDirectBufferedInput>(
        new GlutenDirectBufferedInput(input_, fileNum_, tracker_, groupId_, ioStats_, fsStats_, executor_, options_));
  }

  facebook::velox::memory::MemoryPool* pool() const {
    return pool_;
  }

  /// Returns the CoalescedLoad that contains the correlated loads for
  /// 'stream' or nullptr if none. Returns nullptr on all but first
  /// call for 'stream' since the load is to be triggered by the first
  /// access.
  std::shared_ptr<facebook::velox::dwio::common::DirectCoalescedLoad> coalescedLoad(
      const facebook::velox::dwio::common::SeekableInputStream* stream);

  std::unique_ptr<facebook::velox::dwio::common::SeekableInputStream>
  read(uint64_t offset, uint64_t length, facebook::velox::dwio::common::LogType logType) const override;

  folly::Executor* executor() const override {
    return executor_;
  }

  uint64_t nextFetchSize() const override {
    VELOX_NYI();
  }

 private:
  /// Constructor used by clone().
  GlutenDirectBufferedInput(
      std::shared_ptr<facebook::velox::dwio::common::ReadFileInputStream> input,
      facebook::velox::StringIdLease fileNum,
      std::shared_ptr<facebook::velox::cache::ScanTracker> tracker,
      facebook::velox::StringIdLease groupId,
      std::shared_ptr<facebook::velox::io::IoStatistics> ioStats,
      std::shared_ptr<facebook::velox::filesystems::File::IoStats> fsStats,
      folly::Executor* executor,
      const facebook::velox::io::ReaderOptions& readerOptions)
      : BufferedInput(std::move(input), readerOptions.memoryPool()),
        fileNum_(std::move(fileNum)),
        tracker_(std::move(tracker)),
        groupId_(std::move(groupId)),
        ioStats_(std::move(ioStats)),
        fsStats_(std::move(fsStats)),
        executor_(executor),
        fileSize_(input_->getLength()),
        options_(readerOptions) {}

  std::vector<int32_t> groupRequests(
      const std::vector<facebook::velox::dwio::common::LoadRequest*>& requests,
      bool prefetch) const;

  // Makes a CoalescedLoad for 'requests' to be read together, coalescing IO if
  // appropriate. If 'prefetch' is set, schedules the CoalescedLoad on
  // 'executor_'. Links the CoalescedLoad  to all DirectInputStreams that it
  // covers.
  void readRegion(const std::vector<facebook::velox::dwio::common::LoadRequest*>& requests, bool prefetch);

  // Read coalesced regions.  Regions are grouped together using `groupEnds'.
  // For example if there are 5 regions, 1 and 2 are coalesced together and 3,
  // 4, 5 are coalesced together, we will have {2, 5} in `groupEnds'.
  void readRegions(
      const std::vector<facebook::velox::dwio::common::LoadRequest*>& requests,
      bool prefetch,
      const std::vector<int32_t>& groupEnds);

  // Holds the reference on the memory pool for async load in case of early task
  // terminate.
  struct AsyncLoadHolder {
    std::shared_ptr<facebook::velox::cache::CoalescedLoad> load;
    std::shared_ptr<facebook::velox::memory::MemoryPool> pool;

    ~AsyncLoadHolder() {
      // Release the load reference before the memory pool reference.
      // This is to make sure the memory pool is not destroyed before we free up
      // the allocated buffers.
      // This is to handle the case that the associated task has already
      // destroyed before the async load is done. The async load holds
      // the last reference to the memory pool in that case.
      load.reset();
      pool.reset();
    }
  };

  const facebook::velox::StringIdLease fileNum_;
  const std::shared_ptr<facebook::velox::cache::ScanTracker> tracker_;
  const facebook::velox::StringIdLease groupId_;
  const std::shared_ptr<facebook::velox::io::IoStatistics> ioStats_;
  const std::shared_ptr<facebook::velox::filesystems::File::IoStats> fsStats_;
  folly::Executor* const executor_;
  const uint64_t fileSize_;

  // Regions that are candidates for loading.
  std::vector<facebook::velox::dwio::common::LoadRequest> requests_;

  // Coalesced loads spanning multiple streams in one IO.
  folly::Synchronized<folly::F14FastMap<
      const facebook::velox::dwio::common::SeekableInputStream*,
      std::shared_ptr<facebook::velox::dwio::common::DirectCoalescedLoad>>>
      streamToCoalescedLoad_;

  // Distinct coalesced loads in 'coalescedLoads_'.
  std::vector<std::shared_ptr<facebook::velox::cache::CoalescedLoad>> coalescedLoads_;

  facebook::velox::io::ReaderOptions options_;
};

} // namespace gluten
