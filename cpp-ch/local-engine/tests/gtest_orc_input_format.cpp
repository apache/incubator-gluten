#include <future>
#include <memory>
#include <Core/Block.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadBufferFromFile.h>
#include <Processors/Formats/Impl/ArrowBufferedStreams.h>
#include <Storages/SubstraitSource/OrcFormatFile.h>
#include <Storages/SubstraitSource/OrcUtil.h>
#include <gtest/gtest.h>


class TestOrcInputFormat : public local_engine::ORCBlockInputFormat
{
public:
    explicit TestOrcInputFormat(
        DB::ReadBuffer & in_,
        DB::Block header_,
        const DB::FormatSettings & format_settings_,
        const std::vector<local_engine::StripeInformation> & stripes_)
        : local_engine::ORCBlockInputFormat(in_, header_, format_settings_, stripes_)
    {}

    DB::Chunk callGenerate()
    {
        return generate();
    }
};

static std::string orc_file_path = "./utils/local-engine/tests/data/lineitem.orc";

static DB::Block buildLineitemHeader()
{
    /*
    `l_orderkey` bigint COMMENT 'oops',
    `l_partkey` bigint COMMENT 'oops',
    `l_suppkey` bigint COMMENT 'oops',
    `l_linenumber` int COMMENT 'oops',
    `l_quantity` double COMMENT 'oops',
    `l_extendedprice` double COMMENT 'oops',
    `l_discount` double COMMENT 'oops',
    `l_tax` double COMMENT 'oops',
    `l_returnflag` string COMMENT 'oops',
    `l_linestatus` string COMMENT 'oops',
    `l_shipdate` date COMMENT 'oops',
    `l_commitdate` date COMMENT 'oops',
    `l_receiptdate` date COMMENT 'oops',
    `l_shipinstruct` string COMMENT 'oops',
    `l_shipmode` string COMMENT 'oops',
    `l_comment` string COMMENT 'oops')
    */

    DB::Block header;

    auto bigint_ty = std::make_shared<DB::DataTypeInt64>();
    auto int_ty = std::make_shared<DB::DataTypeInt32>();
    auto double_ty = std::make_shared<DB::DataTypeFloat64>();
    auto string_ty = std::make_shared<DB::DataTypeString>();
    auto date_ty = std::make_shared<DB::DataTypeDate>();

    auto l_orderkey_col = bigint_ty->createColumn();
    DB::ColumnWithTypeAndName l_orderkey(std::move(l_orderkey_col), bigint_ty, "l_orderkey");
    header.insert(l_orderkey);
    DB::ColumnWithTypeAndName l_partkey(std::move(bigint_ty->createColumn()), bigint_ty, "l_partkey");
    header.insert(l_partkey);
    DB::ColumnWithTypeAndName l_suppkey(std::move(bigint_ty->createColumn()), bigint_ty, "l_suppkey");
    header.insert(l_suppkey);
    DB::ColumnWithTypeAndName l_linenumber(std::move(int_ty->createColumn()), int_ty, "l_linenumber");
    header.insert(l_linenumber);
    DB::ColumnWithTypeAndName l_quantity(std::move(double_ty->createColumn()), double_ty, "l_quantity");
    header.insert(l_quantity);
    DB::ColumnWithTypeAndName l_extendedprice(std::move(double_ty->createColumn()), double_ty, "l_extendedprice");
    header.insert(l_extendedprice);
    DB::ColumnWithTypeAndName l_discount(std::move(double_ty->createColumn()), double_ty, "l_discount");
    header.insert(l_discount);
    DB::ColumnWithTypeAndName l_tax(std::move(double_ty->createColumn()), double_ty, "l_tax");
    header.insert(l_tax);
    DB::ColumnWithTypeAndName l_returnflag(std::move(string_ty->createColumn()), string_ty, "l_returnflag");
    header.insert(l_returnflag);
    DB::ColumnWithTypeAndName l_linestatus(std::move(string_ty->createColumn()), string_ty, "l_linestatus");
    header.insert(l_linestatus);
    DB::ColumnWithTypeAndName l_shipdate(std::move(date_ty->createColumn()), date_ty, "l_shipdate");
    header.insert(l_shipdate);
    DB::ColumnWithTypeAndName l_commitdate(std::move(date_ty->createColumn()), date_ty, "l_commitdate");
    header.insert(l_commitdate);
    DB::ColumnWithTypeAndName l_receiptdate(std::move(date_ty->createColumn()), date_ty, "l_receiptdate");
    header.insert(l_receiptdate);
    DB::ColumnWithTypeAndName l_shipinstruct(std::move(string_ty->createColumn()), string_ty, "l_shipinstruct");
    header.insert(l_shipinstruct);
    DB::ColumnWithTypeAndName l_shipmode(std::move(string_ty->createColumn()), string_ty, "l_shipmode");
    header.insert(l_shipmode);
    DB::ColumnWithTypeAndName l_comment(std::move(string_ty->createColumn()), string_ty, "l_comment");
    header.insert(l_comment);

    return header;
}

std::vector<local_engine::StripeInformation> collectRequiredStripes(DB::ReadBuffer* read_buffer)
{
    std::vector<local_engine::StripeInformation> stripes;
    DB::FormatSettings format_settings;
    format_settings.seekable_read = true;
    std::atomic<int> is_stopped{0};
    auto arrow_file = DB::asArrowFile(*read_buffer, format_settings, is_stopped, "ORC", ORC_MAGIC_BYTES);
    auto orc_reader = local_engine::OrcUtil::createOrcReader(arrow_file);
    auto num_stripes = orc_reader->getNumberOfStripes();

    size_t total_num_rows = 0;
    for (size_t i = 0; i < num_stripes; ++i)
    {
        auto stripe_metadata = orc_reader->getStripe(i);
        auto offset = stripe_metadata->getOffset();
        local_engine::StripeInformation stripe_info;
        stripe_info.index = i;
        stripe_info.offset = stripe_metadata->getLength();
        stripe_info.length = stripe_metadata->getLength();
        stripe_info.num_rows = stripe_metadata->getNumberOfRows();
        stripe_info.start_row = total_num_rows;
        stripes.emplace_back(stripe_info);
        total_num_rows += stripe_metadata->getNumberOfRows();
    }
    return stripes;

}

TEST(OrcInputFormat, CallGenerate)
{
    auto file_in = std::make_shared<DB::ReadBufferFromFile>(orc_file_path);
    auto stripes = collectRequiredStripes(file_in.get());
    DB::FormatSettings format_settings;
    auto input_format = std::make_shared<TestOrcInputFormat>(*file_in, buildLineitemHeader(), format_settings, stripes);
    auto chunk = input_format->callGenerate();
    EXPECT_TRUE(chunk.getNumRows() == 2);
}
