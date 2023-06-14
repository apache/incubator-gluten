add_library(
        parquet
        ParquetFileReader.h
        ParquetFileReader.cpp
        ParquetRowGroupReader.h
        ParquetRowGroupReader.cpp
        ParquetColumnChunkReader.h
        ParquetColumnChunkReader.cpp
        PageReader.h
        PageReader.cpp
        RleEncoding.cpp
        RleEncoding.h
        Decoder.h
        Decoder.cpp
        ParquetColumnReader.cpp
        ParquetColumnReader.h
        Utils.h
        StoredColumnReader.cpp
        StoredColumnReader.h
        bit_packing.h
        bit_packing.inline.h
        schema.h
        schema.cpp
        metadata.cpp
        metadata.h
        param.h
        bit_util.cpp
        bit_util.h
        CompressCodec.cpp
        CompressCodec.h
        CustomParquetBlockInputFormat.cpp
        CustomParquetBlockInputFormat.h
        Filters.cpp
        Filters.h)

target_include_directories(parquet PUBLIC
        ${ClickHouse_SOURCE_DIR}/contrib/arrow/cpp/src/
        ${CMAKE_BINARY_DIR}/contrib/thrift-cmake/thrift)

target_compile_options(parquet PUBLIC
        -Wno-suggest-destructor-override
        -Wno-suggest-override
        -Wno-reserved-identifier
        -Wno-old-style-cast
        -Wno-zero-as-null-pointer-constant
        -Wno-shorten-64-to-32
        -Wno-undefined-reinterpret-cast
        )

target_link_libraries(parquet
        clickhouse_common_io
        dbms
        ch_contrib::thrift
)
