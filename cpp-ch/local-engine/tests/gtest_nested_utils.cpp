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
#include <Core/ColumnWithTypeAndName.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/NestedUtils.h>
#include <gtest/gtest.h>

using namespace DB;

GTEST_TEST(NestedUtils, flatten)
{
    Block header;

    /// Nullable tuple
    {
        ColumnWithTypeAndName col;
        col.name = "nt";
        String str_type = "Nullable(Tuple(a Nullable(Int32), b Nullable(String)))";
        col.type = DataTypeFactory::instance().get(str_type);
        header.insert(std::move(col));
    }

    /// Non-nullable tuple
    {
        ColumnWithTypeAndName col;
        col.name = "t";
        String str_type = "Tuple(a Nullable(Int32), b Nullable(String))";
        col.type = DataTypeFactory::instance().get(str_type);
        header.insert(std::move(col));
    }

    Block input_block = header.cloneEmpty();
    auto mutable_columns = input_block.mutateColumns();

    /// Insert into column nt
    {
        /// null
        mutable_columns[0]->insert({});
    }
    {
        /// tuple with null a
        Tuple tuple(2);
        tuple[1] = "gluten";
        mutable_columns[0]->insert(tuple);
    }
    {
        /// tuple with null b
        Tuple tuple(2);
        tuple[0] = Int32(100);
        mutable_columns[0]->insert(tuple);
    }

    {
        /// tuple with a and b not null
        Tuple tuple(2);
        tuple[0] = Int32(101);
        tuple[1] = "spark";
        mutable_columns[0]->insert(tuple);
    }

    /// Insert into column t
    {
        Tuple tuple(2);
        mutable_columns[1]->insert(tuple);
    }
    {
        /// tuple with null a
        Tuple tuple(2);
        tuple[1] = "gluten";
        mutable_columns[1]->insert(tuple);
    }
    {
        /// tuple with null b
        Tuple tuple(2);
        tuple[0] = Int32(100);
        mutable_columns[1]->insert(tuple);
    }
    {
        /// tuple with a and b not null
        Tuple tuple(2);
        tuple[0] = Int32(101);
        tuple[1] = "spark";
        mutable_columns[1]->insert(tuple);
    }

    input_block.setColumns(std::move(mutable_columns));
    Block output_block = Nested::flatten(input_block);
    EXPECT_EQ(output_block.rows(), input_block.rows());
    EXPECT_EQ(output_block.columns(), 4);
    output_block.checkNumberOfRows();

    /// Check first column nt.a
    {
        const auto & column = output_block.getByPosition(0);
        EXPECT_EQ(column.name, "nt.a");
        EXPECT_EQ(column.type->getName(), "Nullable(Int32)");

        const auto & col_nt_a = column.column;
        EXPECT_TRUE((*col_nt_a)[0] == Field{});
        EXPECT_TRUE((*col_nt_a)[1] == Field{});
        EXPECT_TRUE((*col_nt_a)[2] == Int32(100));
        EXPECT_TRUE((*col_nt_a)[3] == Int32(101));
    }

    /// Check second column nt.b
    {
        const auto & column = output_block.getByPosition(1);
        EXPECT_EQ(column.name, "nt.b");
        EXPECT_EQ(column.type->getName(), "Nullable(String)");

        const auto & col_nt_b = column.column;
        EXPECT_TRUE((*col_nt_b)[0] == Field{});
        EXPECT_TRUE((*col_nt_b)[1] == String("gluten"));
        EXPECT_TRUE((*col_nt_b)[2] == Field{});
        EXPECT_TRUE((*col_nt_b)[3] == String("spark"));
    }

    /// Check third column t.a
    {
        const auto & column = output_block.getByPosition(2);
        EXPECT_EQ(column.name, "t.a");
        EXPECT_EQ(column.type->getName(), "Nullable(Int32)");

        const auto & col_t_a = column.column;
        EXPECT_TRUE((*col_t_a)[0] == Field{});
        EXPECT_TRUE((*col_t_a)[1] == Field{});
        EXPECT_TRUE((*col_t_a)[2] == Int32(100));
        EXPECT_TRUE((*col_t_a)[3] == Int32(101));
    }

    /// Check fourth column t.b
    {
        const auto & column = output_block.getByPosition(3);
        EXPECT_EQ(column.name, "t.b");
        EXPECT_EQ(column.type->getName(), "Nullable(String)");

        const auto & col_t_b = column.column;
        EXPECT_TRUE((*col_t_b)[0] == Field{});
        EXPECT_TRUE((*col_t_b)[1] == String("gluten"));
        EXPECT_TRUE((*col_t_b)[2] == Field{});
        EXPECT_TRUE((*col_t_b)[3] == String("spark"));
    }
}
