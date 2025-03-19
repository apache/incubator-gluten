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
#include <Parser/aggregate_function_parser/CommonAggregateFunctionParser.h>
#include <Parser/AggregateFunctionParser.h>


namespace local_engine
{

REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(Sum, sum, sum)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(Avg, avg, avg)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(Min, min, min)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(Max, max, max)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(StdDevPop, stddev_pop, stddev_pop)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(BitAnd, bit_and, groupBitAnd)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(BitOr, bit_or, groupBitOr)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(BitXor, bit_xor, groupBitXor)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(CovarPop, covar_pop, covarPop)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(CovarSamp, covar_samp, covarSamp)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(VarSamp, var_samp, varSamp)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(VarPop, var_pop, varPop)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(Corr, corr, corr)

REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(First, first, first_value_respect_nulls)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(FirstIgnoreNull, first_ignore_null, first_value)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(Last, last, last_value_respect_nulls)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(LastIgnoreNull, last_ignore_null, last_value)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(DenseRank, dense_rank, dense_rank)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(PercentRank, percent_rank, percent_rank)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(Rank, rank, rank)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(RowNumber, row_number, row_number)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(CountDistinct, count_distinct, uniqExact)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(BitmapAggregator, bitmapaggregator, bitmapaggregator)
}
