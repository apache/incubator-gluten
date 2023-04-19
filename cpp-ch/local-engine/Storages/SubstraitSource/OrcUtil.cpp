#include "OrcUtil.h"
#include <Processors/Formats/Impl/ArrowBufferedStreams.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <boost/algorithm/string/case_conv.hpp>
#include <Common/Exception.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>


#define ORC_THROW_NOT_OK(s)                   \
  do {                                        \
    arrow::Status _s = (s);                   \
    if (!_s.ok()) {                           \
      DB::WriteBufferFromOwnString ss;        \
      ss << "Arrow error: " << _s.ToString(); \
      throw orc::ParseError(ss.str());        \
    }                                         \
  } while (0)

#define ORC_ASSIGN_OR_THROW_IMPL(status_name, lhs, rexpr) \
  auto status_name = (rexpr);                             \
  ORC_THROW_NOT_OK(status_name.status());                 \
  lhs = std::move(status_name).ValueOrDie();

#define ORC_ASSIGN_OR_THROW(lhs, rexpr)                                              \
  ORC_ASSIGN_OR_THROW_IMPL(ARROW_ASSIGN_OR_RAISE_NAME(_error_or_value, __COUNTER__), \
                           lhs, rexpr);

#define ORC_BEGIN_CATCH_NOT_OK try {
#define ORC_END_CATCH_NOT_OK                          \
  }                                                   \
  catch (const orc::ParseError& e) {                  \
    return arrow::Status::IOError(e.what());          \
  }                                                   \
  catch (const orc::InvalidArgument& e) {             \
    return arrow::Status::Invalid(e.what());          \
  }                                                   \
  catch (const orc::NotImplementedYet& e) {           \
    return arrow::Status::NotImplemented(e.what());   \
  }

#define ORC_CATCH_NOT_OK(_s)  \
  ORC_BEGIN_CATCH_NOT_OK(_s); \
  ORC_END_CATCH_NOT_OK


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}
}
namespace local_engine
{
uint64_t ArrowInputFile::getLength() const
{
    ORC_ASSIGN_OR_THROW(int64_t size, file->GetSize());
    return static_cast<uint64_t>(size);
}

uint64_t ArrowInputFile::getNaturalReadSize() const
{
    return 128 * 1024;
}

void ArrowInputFile::read(void * buf, uint64_t length, uint64_t offset)
{
    ORC_ASSIGN_OR_THROW(int64_t bytes_read, file->ReadAt(offset, length, buf));

    if (static_cast<uint64_t>(bytes_read) != length)
    {
        throw orc::ParseError("Short read from arrow input file");
    }
}

const std::string & ArrowInputFile::getName() const
{
    static const std::string filename("ArrowInputFile");
    return filename;
}

arrow::Status innerCreateOrcReader(std::shared_ptr<arrow::io::RandomAccessFile> file_, std::unique_ptr<orc::Reader> * orc_reader)
{
    std::unique_ptr<ArrowInputFile> io_wrapper(new ArrowInputFile(file_));
    orc::ReaderOptions options;
    ORC_CATCH_NOT_OK(*orc_reader = std::move(orc::createReader(std::move(io_wrapper), options)));

    return arrow::Status::OK();

}

std::unique_ptr<orc::Reader> OrcUtil::createOrcReader(std::shared_ptr<arrow::io::RandomAccessFile> file_)
{
    std::unique_ptr<orc::Reader> orc_reader;
    auto status = innerCreateOrcReader(file_, &orc_reader);
    if (!status.ok())
    {
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Create orc reader failed. {}", status.message());
    }
    return orc_reader;
}

size_t OrcUtil::countIndicesForType(std::shared_ptr<arrow::DataType> type)
{
    if (type->id() == arrow::Type::LIST)
        return countIndicesForType(static_cast<arrow::ListType *>(type.get())->value_type()) + 1;

    if (type->id() == arrow::Type::STRUCT)
    {
        int indices = 1;
        auto * struct_type = static_cast<arrow::StructType *>(type.get());
        for (int i = 0; i != struct_type->num_fields(); ++i)
            indices += countIndicesForType(struct_type->field(i)->type());
        return indices;
    }

    if (type->id() == arrow::Type::MAP)
    {
        auto * map_type = static_cast<arrow::MapType *>(type.get());
        return countIndicesForType(map_type->key_type()) + countIndicesForType(map_type->item_type()) + 1;
    }

    return 1;
}

void OrcUtil::getFileReaderAndSchema(
    DB::ReadBuffer & in,
    std::unique_ptr<arrow::adapters::orc::ORCFileReader> & file_reader,
    std::shared_ptr<arrow::Schema> & schema,
    const DB::FormatSettings & format_settings,
    std::atomic<int> & is_stopped)
{
    auto arrow_file = DB::asArrowFile(in, format_settings, is_stopped, "ORC", ORC_MAGIC_BYTES);
    if (is_stopped)
        return;

    auto result = arrow::adapters::orc::ORCFileReader::Open(arrow_file, arrow::default_memory_pool());
    if (!result.ok())
        throw DB::Exception(result.status().ToString(), DB::ErrorCodes::BAD_ARGUMENTS);
    file_reader = std::move(result).ValueOrDie();

    auto read_schema_result = file_reader->ReadSchema();
    if (!read_schema_result.ok())
        throw DB::Exception(read_schema_result.status().ToString(), DB::ErrorCodes::BAD_ARGUMENTS);
    schema = std::move(read_schema_result).ValueOrDie();

    if (format_settings.use_lowercase_column_name)
    {
        std::vector<std::shared_ptr<::arrow::Field>> fields;
        fields.reserve(schema->num_fields());
        for (int i = 0; i < schema->num_fields(); ++i)
        {
            const auto& field = schema->field(i);
            auto name = field->name();
            boost::to_lower(name);
            fields.push_back(field->WithName(name));
        }
        schema = arrow::schema(fields, schema->metadata());
    }
}
}
