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

#include <iostream>
#include <string>
#include <vector>

namespace gluten {

// the functions defined in this file are used to debug
// the token `ToString` means the method of `ToString()`
// the token `2String` means the method of `toString()`

template <typename T>
static inline void Print(const T& t) {}

template <typename T>
static inline void PrintLF(const T& t) {}

template <typename A, typename B>
static inline void Print(const A& a, const B& b) {}

template <typename A, typename B>
static inline void PrintLF(const A& a, const B& b) {}

template <typename A, typename B>
static inline void PrintSplit(const A& a, const B& b, const std::string split = ": ") {}

template <typename A, typename B>
static inline void PrintSplitLF(const A& a, const B& b, const std::string split = ": ") {}

template <typename A, typename B>
static inline void PrintEQ(const A& a, const B& b) {}

template <typename A, typename B>
static inline void PrintEQLF(const A& a, const B& b) {}

template <typename A, typename B>
static inline void PrintVS(const A& a, const B& b) {}

template <typename A, typename B>
static inline void PrintVSLF(const A& a, const B& b) {}

template <typename E>
static inline void PrintElement(const E& e, bool first = false) {}

template <typename ITERATOR>
static inline void PrintRange(ITERATOR begin, ITERATOR end) {}

template <typename C>
static inline void PrintContainer(const C& c, const std::string& containerName = "") {}

template <typename A, typename B>
static inline void PrintAB2String(const A& a, const B& b) {}

template <typename T>
static inline void Print2String(const T& t, const std::string& prefix = "") {}

template <typename ITERATOR>
static inline void PrintRangeToString(ITERATOR begin, ITERATOR end) {}

template <typename C>
static inline void PrintContainerToString(const C& c, const std::string& containerName = "") {}

template <typename C>
static inline void PrintContainer2String(const C& c, const std::string& containerName = "") {}

template <typename C>
static inline void PrintVectorToString(const C& c, const std::string& containerName = "") {}

template <typename C>
static inline void PrintVector2String(const C& c, const std::string& containerName = "") {}

template <typename V>
static inline void PrintVectorMapping(const V& v, const std::string& vectorName = "") {}

template <typename V>
static inline void PrintVectorRange(const V& v, unsigned int from, unsigned int to) {}

#define PRINT(a)
#define PRINTLF(a)

#define PRINT_FUNCTION_NAME()
#define PRINT_FUNCTION_SPLIT_LINE()

#define PRINT_CONTAINER(c)

#define PRINT_CONTAINER_TO_STRING(c)
#define PRINT_CONTAINER_2_STRING(c)

#define PRINT_VECTOR_TO_STRING(v)
#define PRINT_VECTOR_2_STRING(v)

#define PRINT_VECTOR_MAPPING(v)

} // namespace gluten
