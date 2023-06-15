#include "ParquetColumnChunkReader.h"
#include "Utils.h"
#include <IO/ReadBufferFromMemory.h>
#include <snappy.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

ParquetColumnChunkReader::ParquetColumnChunkReader(
    size_t max_def_level_,
    size_t max_rep_level_,
    const parquet::format::ColumnChunk * column_chunk_,
    const ColumnReaderOptions & opts_)
    : max_def_level(max_def_level_),
    max_rep_level(max_rep_level_),
    chunk_metadata(column_chunk_),
    opts(opts_)
{
}

void ParquetColumnChunkReader::init(size_t chunk_size_) {
    chunk_size = chunk_size_;
    int64_t start_offset = 0;
    if (metadata().__isset.dictionary_page_offset) {
        start_offset = metadata().dictionary_page_offset;
    } else {
        start_offset = metadata().data_page_offset;
    }
    size_t size = metadata().total_compressed_size;
    SeekableReadBuffer * stream = opts.stream;
    if (!stream) {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "stream is missing");
    }
    std::unique_ptr<PaddedPODArray<char>> page_data = std::make_unique<PaddedPODArray<char>>();
    page_data->resize_fill(size);
    stream->seek(start_offset, SEEK_SET);
    auto bytes = stream->readBig(page_data->data(), size);
    chassert(bytes == size);
    SeekableReadBufferPtr page_stream = std::make_shared<ReadBufferFromMemory>(page_data->data(), page_data->size());
    page_reader = std::make_unique<PageReader>(page_stream, size);
    page_reader->page_data = std::move(page_data);
    auto compress_type = metadata().codec;
    compress_codec = ICompressCodec::getCompressCodec(getCompressionMethod(compress_type));
    chunk_size = chunk_size_;
}

void ParquetColumnChunkReader::loadHeader()
{
    parsePageHeader();
}
void ParquetColumnChunkReader::parsePageHeader()
{
    chassert(page_parse_state == INITIALIZED || page_parse_state == PAGE_DATA_PARSED);
    page_reader->nextHeader();

    if (page_reader->currentHeader()->type == parquet::format::PageType::DATA_PAGE) {
        const auto& header = *page_reader->currentHeader();
        num_values = header.data_page_header.num_values;
    }
    page_parse_state = PAGE_HEADER_PARSED;
}
void ParquetColumnChunkReader::loadPage()
{
    if (page_parse_state == PAGE_DATA_PARSED) {
        return;
    }
    if (page_parse_state != PAGE_HEADER_PARSED) {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Page header has not been parsed before loading page data");
    }
    parsePageData();
}

void ParquetColumnChunkReader::parsePageData()
{
    switch (page_reader->currentHeader()->type) {
        case parquet::format::PageType::DATA_PAGE:
            parseDataPage();
            break;
        case parquet::format::PageType::DICTIONARY_PAGE:
            parseDictPage();
            break;
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Not supported page type: {}", page_reader->currentHeader()->type);
    }
}

void ParquetColumnChunkReader::tryLoadDictionary()
{
    if (dict_page_parsed) {
        return;
    }
    parsePageHeader();
    if (!currentPageIsDict()) {
        return;
    }
    parseDictPage();
}


void ParquetColumnChunkReader::parseDataPage()
{
    if (page_parse_state == PAGE_DATA_PARSED) {
        return;
    }
    if (page_parse_state != PAGE_HEADER_PARSED) {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Error state");
    }

    const auto& header = *page_reader->currentHeader();

    size_t compressed_size = header.compressed_page_size;
    size_t uncompressed_size = header.uncompressed_page_size;
    readAndDecompressPageData(compressed_size, uncompressed_size, true);

    // parse levels
    if (max_rep_level > 0) {
        rep_level_decoder.parse(header.data_page_header.repetition_level_encoding, max_rep_level,
                                                 header.data_page_header.num_values, &data);
    }
    if (max_def_level > 0) {
        def_level_decoder.parse(header.data_page_header.definition_level_encoding, max_def_level,
                                                 header.data_page_header.num_values, &data);
    }

    auto encoding = header.data_page_header.encoding;
    // change the deprecated encoding to RLE_DICTIONARY
    if (encoding == parquet::format::Encoding::PLAIN_DICTIONARY) {
        encoding = parquet::format::Encoding::RLE_DICTIONARY;
    }

    cur_decoder = decoders[static_cast<int>(encoding)].get();
    if (cur_decoder == nullptr) {
        const EncodingInfo enc_info = EncodingInfo::get(metadata().type, encoding);
        std::unique_ptr<Decoder> decoder = enc_info.createDecoder();

        cur_decoder = decoder.get();
        decoders[static_cast<int>(encoding)] = std::move(decoder);
    }

//    cur_decoder->setTypeLegth(type_length);
    cur_decoder->setData(data);

    page_parse_state = PAGE_DATA_PARSED;
}

void ParquetColumnChunkReader::parseDictPage()
{
    if (dict_page_parsed) {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There are two dictionary page in this column");
    }
    
    const parquet::format::PageHeader& header = *page_reader->currentHeader();
    chassert(parquet::format::PageType::DICTIONARY_PAGE == header.type);

    uint32_t compressed_size = header.compressed_page_size;
    uint32_t uncompressed_size = header.uncompressed_page_size;
    readAndDecompressPageData(compressed_size, uncompressed_size, true);

    parquet::format::Encoding::type dict_encoding = header.dictionary_page_header.encoding;
    // Using the PLAIN_DICTIONARY enum value is deprecated in the Parquet 2.0 specification.
    // Prefer using RLE_DICTIONARY in a data page and PLAIN in a dictionary page for Parquet 2.0+ files.
    // refer: https://github.com/apache/parquet-format/blob/master/Encodings.md
    if (dict_encoding == parquet::format::Encoding::PLAIN_DICTIONARY) {
        dict_encoding = parquet::format::Encoding::PLAIN;
    }

    EncodingInfo code_info = EncodingInfo::get(metadata().type, dict_encoding);
    std::unique_ptr<Decoder> dict_decoder = code_info.createDecoder();
    dict_decoder->setData(data);
//    dict_decoder->setTypeLength(type_length);
//    if (dict_encoding == parquet::format::Encoding::PLAIN)
//    {
//        int plain_encoding = static_cast<int>(parquet::format::Encoding::PLAIN_DICTIONARY);
//        decoders[plain_encoding] = std::move(dict_decoder);
//        cur_decoder = decoders[plain_encoding].get();
//        dict_page_parsed = true;
//    }
//    else
//    {
        // TODO
        code_info = EncodingInfo::get(metadata().type, parquet::format::Encoding::RLE_DICTIONARY);
        std::unique_ptr<Decoder> decoder = code_info.createDecoder();
        decoder->setDict(chunk_size, header.dictionary_page_header.num_values, *dict_decoder);
        int rle_encoding = static_cast<int>(parquet::format::Encoding::RLE_DICTIONARY);
        decoders[rle_encoding] = std::move(decoder);
        cur_decoder = decoders[rle_encoding].get();
        dict_page_parsed = true;
//    }

    page_parse_state = PAGE_DATA_PARSED;
}

void ParquetColumnChunkReader::readAndDecompressPageData(size_t compressed_size, size_t uncompressed_size, bool is_compressed)
{
    if (is_compressed) {
        compressed_data_buf.reserve(compressed_size);
        page_reader->readBytes(compressed_data_buf.data(), compressed_size);
        ReadBufferFromMemory data_read_buffer(compressed_data_buf.data(), compressed_size);
        uncompressed_buf.reserve(uncompressed_size);
        // refactor decompress code
        if (compress_codec)
        {
            compress_codec->decompress(compressed_data_buf.data(), compressed_size, uncompressed_buf.data(), uncompressed_size);
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "missing compress codec");
        }
    } else {
        uncompressed_buf.reserve(uncompressed_size);
        page_reader->readBytes(uncompressed_buf.data(), uncompressed_buf.size());
    }
    data.data = uncompressed_buf.data();
    data.size = uncompressed_size;
}
void ParquetColumnChunkReader::skipPage()
{
    if (page_parse_state == PAGE_DATA_PARSED) {
        return;
    }
    if (page_parse_state != PAGE_HEADER_PARSED) {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Error state");
    }
    const auto& header = *page_reader->currentHeader();
    uint32_t compressed_size = header.compressed_page_size;
    uint32_t uncompressed_size = header.uncompressed_page_size;
    size_t size = compress_codec ? compressed_size : uncompressed_size;
    page_reader->skipBytes(size);
    page_parse_state = PAGE_DATA_PARSED;
}

void ParquetColumnChunkReader::skipRows(size_t rows)
{
    if (rows == 0) return;
    chassert(cur_decoder != nullptr);
    cur_decoder->skipRows(rows);
}

bool ParquetColumnChunkReader::currentPageIsDict()
{
    const auto * header = page_reader->currentHeader();
    return header->type == parquet::format::PageType::DICTIONARY_PAGE;
}


std::pair<ColumnPtr, ColumnPtr> ParquetColumnChunkReader::readMinMaxColumn()
{
    chassert(opts.stats_type);
    chassert(canUseMinMaxStats());
    const auto * header = page_reader->currentHeader();
    ColumnPtr min_column;
    ColumnPtr max_column;
    Int64 min_value = 0;
    Int64 max_value = 0;
    if (!header->data_page_header.statistics.min.empty())
    {
        WhichDataType which(opts.stats_type);
        if (which.isInt64())
        {

            PlainDecoder<Int64>::decode(header->data_page_header.statistics.min, &min_value);
            PlainDecoder<Int64>::decode(header->data_page_header.statistics.max, &max_value);
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "unsupported stats type {}", opts.stats_type->getName());
        }
    }
    else
    {
        WhichDataType which(opts.stats_type);
        if (which.isInt64())
        {
            PlainDecoder<Int64>::decode(header->data_page_header.statistics.min_value, &min_value);
            PlainDecoder<Int64>::decode(header->data_page_header.statistics.max_value, &max_value);
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "unsupported stats type {}", opts.stats_type->getName());
        }
    }
    min_column = opts.stats_type->createColumnConst(1, min_value);
    max_column = opts.stats_type->createColumnConst(1, max_value);
    return std::pair(min_column, max_column);
}

}
