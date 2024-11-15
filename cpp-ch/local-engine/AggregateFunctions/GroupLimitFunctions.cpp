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
#include <vector>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/IAggregateFunction_fwd.h>
#include <Columns/ColumnArray.h>
#include <Core/Field.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Interpreters/Context.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>
#include <Common/Exception.h>
#include <Common/SortUtils.h>

#include <Poco/Logger.h>
#include <Common/logger_useful.h>

namespace local_engine
{

template <SortOrder direction>
class Sorter
{
public:
};

struct RowNumGroupArraySortedData
{
public:
    using Data = std::pair<DB::Tuple, DB::Tuple>;
    std::vector<Data> values;

    // return a < b
    static bool compare(const Data & lhs, const Data & rhs, const std::vector<Int8> & directions, const std::vector<Int8> & nulls_first)
    {
        const auto & a = lhs.second;
        const auto & b = rhs.second;
        for (size_t i = 0; i < a.size(); ++i)
        {
            bool a_is_null = a[i].isNull();
            bool b_is_null = b[i].isNull();
            if (a_is_null && b_is_null)
            {
                continue;
            }
            else if (nulls_first[i])
            {
                if (a_is_null)
                    return false;
                else if (b_is_null)
                    return true;
                else if (a[i] < b[i])
                    return directions[i];
                else if (a[i] > b[i])
                    return !directions[i];
            }
            else
            {
                if (a_is_null)
                    return true;
                else if (b_is_null)
                    return false;
                else if (a[i] < b[i])
                    return directions[i];
                else if (a[i] > b[i])
                    return !directions[i];
            }
        }
        return false;
    }

    ALWAYS_INLINE void heapReplaceTop(const std::vector<Int8> & directions, const std::vector<Int8> & nulls_first)
    {
        size_t size = values.size();
        if (size < 2)
            return;
        size_t child_index = 1;
        if (size > 2 && compare(values[1], values[2], directions, nulls_first))
            ++child_index;

        if (compare(values[child_index], values[0], directions, nulls_first))
            return;

        size_t current_index = 0;
        auto current = values[current_index];
        do
        {
            values[current_index] = values[child_index];
            current_index = child_index;

            child_index = 2 * child_index + 1;

            if (child_index >= size)
                break;

            if ((child_index + 1) < size && compare(values[child_index], values[child_index + 1], directions, nulls_first))
                ++child_index;
        } while (!compare(values[child_index], current, directions, nulls_first));

        values[current_index] = current;
    }

    ALWAYS_INLINE void
    addElement(Data data, const std::vector<Int8> & directions, const std::vector<Int8> & nulls_first, size_t max_elements)
    {
        if (values.size() >= max_elements)
        {
            if (!compare(data, values[0], directions, nulls_first))
                return;
            values[0] = std::move(data);
            heapReplaceTop(directions, nulls_first);
            return;
        }
        values.push_back(std::move(data));
        auto cmp = [&directions, &nulls_first](const Data & a, const Data & b) { return compare(a, b, directions, nulls_first); };
        std::push_heap(values.begin(), values.end(), cmp);
    }

    ALWAYS_INLINE void sortAndLimit(size_t max_elements, const std::vector<Int8> & directions, const std::vector<Int8> & nulls_first)
    {
        ::sort(
            values.begin(),
            values.end(),
            [&directions, &nulls_first](const Data & a, const Data & b) { return compare(a, b, directions, nulls_first); });
        if (values.size() > max_elements)
            values.resize(max_elements);
    }

    ALWAYS_INLINE void
    insertResultInto(DB::IColumn & to, size_t max_elements, const std::vector<Int8> & directions, const std::vector<Int8> & nulls_first)
    {
        auto & result_array = assert_cast<DB::ColumnArray &>(to);
        auto & result_array_offsets = result_array.getOffsets();

        sortAndLimit(max_elements, directions, nulls_first);

        result_array_offsets.push_back(result_array_offsets.back() + values.size());

        if (values.empty())
            return;
        auto & result_array_data = result_array.getData();
        for (int i = 0, sz = static_cast<int>(values.size()); i < sz; ++i)
        {
            auto & value = values[i].first;
            value.push_back(i + 1);
            result_array_data.insert(value);
        }
    }
};

static DB::DataTypePtr getReultDataType(DB::DataTypePtr data_type)
{
    const auto * tuple_type = typeid_cast<const DB::DataTypeTuple *>(data_type.get());
    if (!tuple_type)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Tuple type is expected, but got: {}", data_type->getName());
    DB::DataTypes element_types = tuple_type->getElements();
    std::vector<String> element_names = tuple_type->getElementNames();
    element_types.push_back(std::make_shared<DB::DataTypeInt32>());
    element_names.push_back("row_num");
    auto nested_tuple_type = std::make_shared<DB::DataTypeTuple>(element_types, element_names);
    return std::make_shared<DB::DataTypeArray>(nested_tuple_type);
}
class RowNumGroupArraySorted final : public DB::IAggregateFunctionDataHelper<RowNumGroupArraySortedData, RowNumGroupArraySorted>
{
public:
    explicit RowNumGroupArraySorted(DB::DataTypePtr value_data_type, DB::DataTypePtr order_data_type, const DB::Array & parameters_)
        : DB::IAggregateFunctionDataHelper<RowNumGroupArraySortedData, RowNumGroupArraySorted>(
              {value_data_type, order_data_type}, parameters_, getReultDataType(value_data_type))
    {
        const auto * order_tuple = typeid_cast<const DB::DataTypeTuple *>(order_data_type.get());
        if (!order_tuple)
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Tuple type is expected, but got: {}", order_data_type->getName());
        if (parameters_.size() != order_tuple->getElements().size() + 1)
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Number of sort directions should be equal to number of order columns");

        limit = parameters_[0].safeGet<UInt64>();

        for (size_t i = 1; i < parameters_.size(); ++i)
        {
            auto direction = magic_enum::enum_cast<SortOrder>(parameters_[i].safeGet<String>());
            if (!direction.has_value())
                throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unknow direction: {}", parameters_[i].safeGet<String>());
            if (direction.value() == SortOrder::ASC_NULLS_FIRST)
            {
                directions.push_back(1);
                nulls_first.push_back(1);
            }
            else if (direction.value() == SortOrder::ASC_NULLS_LAST)
            {
                directions.push_back(1);
                nulls_first.push_back(0);
            }
            else if (direction.value() == SortOrder::DESC_NULLS_FIRST)
            {
                directions.push_back(0);
                nulls_first.push_back(1);
            }
            else if (direction.value() == SortOrder::DESC_NULLS_LAST)
            {
                directions.push_back(0);
                nulls_first.push_back(0);
            }
            else
                throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unknown sort direction: {}", magic_enum::enum_name(direction.value()));
        }
        DB::DataTypeTuple serialization_data_type({value_data_type, order_data_type});
        serialization = serialization_data_type.getDefaultSerialization();
    }

    String getName() const override { return "rowNumGroupArraySorted"; }

    void add(DB::AggregateDataPtr __restrict place, const DB::IColumn ** columns, size_t row_num, DB::Arena * /*arena*/) const override
    {
        LOG_ERROR(getLogger("RowNumGroupArraySorted"), "xxx add");
        auto & data = this->data(place);
        DB::Tuple data_tuple = (*columns[0])[row_num].safeGet<DB::Tuple>();
        DB::Tuple order_tuple = (*columns[1])[row_num].safeGet<DB::Tuple>();
        this->data(place).addElement(
            std::make_pair<DB::Tuple, DB::Tuple>(std::move(data_tuple), std::move(order_tuple)), directions, nulls_first, limit);
    }

    void merge(DB::AggregateDataPtr __restrict place, DB::ConstAggregateDataPtr rhs, DB::Arena * /*arena*/) const override
    {
        LOG_ERROR(getLogger("RowNumGroupArraySorted"), "xxx merge");
        auto & rhs_values = this->data(rhs).values;
        for (auto & rhs_element : rhs_values)
            this->data(place).addElement(rhs_element, directions, nulls_first, limit);
    }

    void serialize(DB::ConstAggregateDataPtr __restrict place, DB::WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        auto & values = this->data(place).values;
        size_t size = values.size();
        DB::writeVarUInt(size, buf);

        for (const auto & [value, order] : values)
        {
            DB::Tuple data = {value, order};
            serialization->serializeBinary(data, buf, {});
        }
    }

    void deserialize(
        DB::AggregateDataPtr __restrict place, DB::ReadBuffer & buf, std::optional<size_t> /* version */, DB::Arena *) const override
    {
        size_t size = 0;
        DB::readVarUInt(size, buf);

        auto & values = this->data(place).values;
        values.reserve(size);
        for (size_t i = 0; i < size; ++i)
        {
            DB::Field data;
            serialization->deserializeBinary(data, buf, {});
            DB::Tuple tuple_data = data.safeGet<DB::Tuple>();
            DB::Tuple a = tuple_data[0].safeGet<DB::Tuple>();
            DB::Tuple b = tuple_data[1].safeGet<DB::Tuple>();
            auto value = std::make_pair<DB::Tuple, DB::Tuple>(std::move(a), std::move(b));
            values.emplace_back(value);
        }
    }

    void insertResultInto(DB::AggregateDataPtr __restrict place, DB::IColumn & to, DB::Arena * /*arena*/) const override
    {
        LOG_ERROR(getLogger("RowNumGroupArraySorted"), "xxx insertResultInto");
        this->data(place).insertResultInto(to, limit, directions, nulls_first);
    }

    bool allocatesMemoryInArena() const override { return true; }

private:
    size_t limit = 0;
    std::vector<Int8> directions;
    std::vector<Int8> nulls_first;
    DB::SerializationPtr serialization;
};


DB::AggregateFunctionPtr createAggregateFunctionRowNumGroupArray(
    const std::string & name, const DB::DataTypes & argument_types, const DB::Array & parameters, const DB::Settings *)
{
    std::string query = "x desc nulls first, y asc";
    DB::ParserOrderByExpressionList order_parser;
    auto ast = DB::parseQuery(order_parser, query, 100, 100, 100);
    LOG_DEBUG(getLogger("RowNumGroupArraySorted"), "xxx test ast\n{}", DB::queryToString(ast));
    for (const auto & data_type : argument_types)
        LOG_ERROR(getLogger("RowNumGroupArraySorte"), "xxx arg type: {}", data_type->getName());
    return std::make_shared<RowNumGroupArraySorted>(argument_types[0], argument_types[1], parameters);
}

void registerAggregateFunctionRowNumGroup(DB::AggregateFunctionFactory & factory)
{
    DB::AggregateFunctionProperties properties = {.returns_default_when_only_null = false, .is_order_dependent = false};

    factory.registerFunction("rowNumGroupArraySorted", {createAggregateFunctionRowNumGroupArray, properties});
}
}
