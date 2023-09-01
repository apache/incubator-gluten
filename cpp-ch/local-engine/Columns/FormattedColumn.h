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
#include <Columns/IColumn.h>
#include <Columns/IColumnImpl.h>
#include <Common/COW.h>
#include <Common/Exception.h>
#include <Common/PODArray_fwd.h>
#include <Core/Field.h>
#include <Poco/Logger.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}
}

namespace local_engine
{

// Not a writable column. It's useful to share intemediate data between processors.
template <typename Decoder>
class FormattedColumn : public COWHelper<DB::IColumn, FormattedColumn<Decoder>>
{
public:
    using Base = COWHelper<DB::IColumn, FormattedColumn<Decoder>>;
    using Ptr = Base::Ptr;
    using MutablePtr = Base::MutablePtr;
    using DecoderPtr = std::shared_ptr<Decoder>;
    using DecodedElement = Decoder::Element;
    using DecodedElementPtr = std::shared_ptr<DecodedElement>;
    using DecodedElementContainer = std::vector<DecodedElementPtr>;

    DecodedElementPtr getDecodedElement(size_t row) const
    {
        assert(row < decoded_elements.size());
        return decoded_elements[row];
    }

    const DB::IColumn & getNestedColumnRef() const { return *nested_column; }
    const DB::ColumnPtr & getNestedColumnPtr() const { return nested_column; }

    bool canBeInsideNullable() const override { return true; }

private:
    friend class COWHelper<DB::IColumn, FormattedColumn<Decoder>>;
    DecoderPtr decoder;
    Base::WrappedPtr nested_column;
    DecodedElementContainer decoded_elements;

    FormattedColumn() = default;
    FormattedColumn(const FormattedColumn & other)
        : decoder(other.decoder)
        , nested_column(other.nested_column)
    {
    }

    explicit FormattedColumn(DB::ColumnPtr nested_column_, DecoderPtr decoder_)
        : decoder(decoder_)
        , nested_column(nested_column_)
    {
        initializationDecode();
    }

    void initializationDecode()
    {
        size_t rows = nested_column->size();
        decoded_elements.resize(rows);
        for (size_t i = 0; i < rows; ++i)
        {
            decoded_elements[i] = decoder->decode(nested_column->getDataAt(i));
        }
    }

    const char * getFamilyName() const override { return "Formatted"; }
    std::string getName() const override { return "Formatted(" + nested_column->getName() + ")"; }
    DB::TypeIndex getDataType() const override { return nested_column->getDataType(); }

    DB::MutableColumnPtr cloneResized(size_t) const override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Not support cloneResized.");
    }

    size_t size() const override
    {
        return nested_column->size();
    }

    DB::ColumnPtr cut(size_t, size_t) const override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Not support cut.");
    }

    DB::ColumnPtr replicate(const DB::IColumn::Offsets &) const override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Not support replicate.");
    }

    DB::ColumnPtr filter(const DB::IColumn::Filter &, ssize_t) const override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Not support filter.");
    }

    void expand(const DB::IColumn::Filter &, bool) override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Not support expand");
    }

    DB::ColumnPtr permute(const DB::IColumn::Permutation &, size_t) const override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Not support permute.");
    }

    DB::ColumnPtr index(const DB::IColumn &, size_t) const override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Not support index.");
    }

    std::vector<DB::MutableColumnPtr> scatter(DB::IColumn::ColumnIndex,
                                              const DB::IColumn::Selector &) const override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Not support scatter.");
    }

    void getExtremes(DB::Field &, DB::Field &) const override {}

    size_t byteSize() const override { return nested_column->byteSize(); }
    size_t byteSizeAt(size_t n) const override { return nested_column->byteSizeAt(n); }
    size_t allocatedBytes() const override { return nested_column->allocatedBytes(); }

    DB::Field operator[](size_t) const override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Cannot get value from {}", getName());
    }

    void get(size_t, DB::Field &) const override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Cannot get value from {}", getName());
    }

    StringRef getDataAt(size_t) const override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Cannot get value from {}", getName());
    }

    bool isDefaultAt(size_t) const override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "isDefaultAt is not implemented for {}", getName());
    }

    void insert(const DB::Field &) override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Cannot insert into {}", getName());
    }

    void insertDefault() override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Cannot insert into {}", getName());
    }

    void insertFrom(const DB::IColumn &, size_t) override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Cannot insert into {}", getName());
    }

    void insertRangeFrom(const DB::IColumn &, size_t, size_t) override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Cannot insert into {}", getName());
    }

    void insertData(const char *, size_t) override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Cannot insert into {}", getName());
    }

    StringRef serializeValueIntoArena(size_t, DB::Arena &, char const *&, const UInt8 *) const override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Cannot serializeValueIntoArena into {}", getName());
    }

    const char * deserializeAndInsertFromArena(const char *) override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Cannot deserializeAndInsertFromArena into {}", getName());
    }

    const char * skipSerializedInArena(const char *) const override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Cannot skipSerializedInArena into {}", getName());
    }

    void updateHashWithValue(size_t, SipHash &) const override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "updateHashWithValue is not implemented for {}", getName());
    }

    void updateWeakHash32(DB::WeakHash32 &) const override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "updateWeakHash32 is not implemented for {}", getName());
    }

    void updateHashFast(SipHash &) const override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "updateHashFast is not implemented for {}", getName());
    }

    void popBack(size_t) override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "popBack is not implemented for {}", getName());
    }

    int compareAt(size_t, size_t, const DB::IColumn &, int) const override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "compareAt is not implemented for {}", getName());
    }

    void compareColumn(const DB::IColumn &, size_t, DB::PaddedPODArray<UInt64> *, DB::PaddedPODArray<Int8> &, int, int) const override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "compareColumn is not implemented for {}", getName());
    }

    bool hasEqualValues() const override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "hasEqualValues is not implemented for {}", getName());
    }

    void getPermutation(DB::IColumn::PermutationSortDirection, DB::IColumn::PermutationSortStability,
                        size_t, int, DB::IColumn::Permutation &) const override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "getPermutation is not implemented for {}", getName());
    }

    void updatePermutation(DB::IColumn::PermutationSortDirection, DB::IColumn::PermutationSortStability,
                        size_t, int, DB::IColumn::Permutation &, DB::EqualRanges &) const override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "updatePermutation is not implemented for {}", getName());
    }

    void gather(DB::ColumnGathererStream &) override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Method gather is not supported for {}", getName());
    }

    double getRatioOfDefaultRows(double) const override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Method getRatioOfDefaultRows is not supported for {}", getName());
    }

    UInt64 getNumberOfDefaultRows() const override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Method getNumberOfDefaultRows is not supported for {}", getName());
    }

    void getIndicesOfNonDefaultRows(DB::IColumn::Offsets &, size_t, size_t) const override
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Method getIndicesOfNonDefaultRows is not supported for {}", getName());
    }
};

}
