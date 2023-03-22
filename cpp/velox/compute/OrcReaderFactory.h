#pragma once

#include "velox/dwio/dwrf/reader/DwrfReader.h"

namespace gluten {

using namespace facebook;

class OrcReaderFactory : public velox::dwio::common::ReaderFactory {
 public:
  OrcReaderFactory() : velox::dwio::common::ReaderFactory(velox::dwio::common::FileFormat::ORC) {}

  std::unique_ptr<velox::dwio::common::Reader> createReader(
      std::unique_ptr<velox::dwio::common::BufferedInput> input,
      const velox::dwio::common::ReaderOptions& options) override {
    return velox::dwrf::DwrfReader::create(std::move(input), options);
  }
};

inline void registerOrcReaderFactory() {
  velox::dwio::common::registerReaderFactory(std::make_shared<OrcReaderFactory>());
}

inline void unregisterOrcReaderFactory() {
  velox::dwio::common::unregisterReaderFactory(velox::dwio::common::FileFormat::ORC);
}

}; // namespace gluten
