#pragma once
#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <Processors/Formats/Impl/ORCBlockInputFormat.h>
/// there are destructor not be overrided warnings in orc lib, ignore them
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsuggest-destructor-override"
#include <orc/Reader.hh>
#include <orc/OrcFile.hh>
#pragma GCC diagnostic pop
#include <orc/Exceptions.hh>
#include <memory>

namespace local_engine{

class ArrowInputFile : public orc::InputStream {
 public:
  explicit ArrowInputFile(const std::shared_ptr<arrow::io::RandomAccessFile>& file_)
      : file(file_) {}

  uint64_t getLength() const override;

  uint64_t getNaturalReadSize() const override;

  void read(void* buf, uint64_t length, uint64_t offset) override;

  const std::string& getName() const override;

 private:
  std::shared_ptr<arrow::io::RandomAccessFile> file;
};

class OrcUtil
{
public:
    static std::unique_ptr<orc::Reader> createOrcReader(std::shared_ptr<arrow::io::RandomAccessFile> file_);

    static size_t countIndicesForType(std::shared_ptr<arrow::DataType> type);
    static void getFileReaderAndSchema(
        DB::ReadBuffer & in,
        std::unique_ptr<arrow::adapters::orc::ORCFileReader> & file_reader,
        std::shared_ptr<arrow::Schema> & schema,
        const DB::FormatSettings & format_settings,
        std::atomic<int> & is_stopped);
};

}
