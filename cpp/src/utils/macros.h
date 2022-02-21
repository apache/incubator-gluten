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

#include <time.h>

#include <chrono>

#include "utils/exception.h"

#define TIME_NANO_DIFF(finish, start) \
  (finish.tv_sec - start.tv_sec) * 1000000000 + (finish.tv_nsec - start.tv_nsec)

#define TIME_MICRO_OR_RAISE(time, expr)                                                 \
  do {                                                                                  \
    auto start = std::chrono::steady_clock::now();                                      \
    auto __s = (expr);                                                                  \
    if (!__s.ok()) {                                                                    \
      return __s;                                                                       \
    }                                                                                   \
    auto end = std::chrono::steady_clock::now();                                        \
    time += std::chrono::duration_cast<std::chrono::microseconds>(end - start).count(); \
  } while (false);

#define TIME_MICRO_OR_THROW(time, expr)                                                 \
  do {                                                                                  \
    auto start = std::chrono::steady_clock::now();                                      \
    auto __s = (expr);                                                                  \
    if (!__s.ok()) {                                                                    \
      throw JniPendingException(__s.message());                                         \
    }                                                                                   \
    auto end = std::chrono::steady_clock::now();                                        \
    time += std::chrono::duration_cast<std::chrono::microseconds>(end - start).count(); \
  } while (false);

#define TIME_MICRO(time, res, expr)                                                     \
  do {                                                                                  \
    auto start = std::chrono::steady_clock::now();                                      \
    res = (expr);                                                                       \
    auto end = std::chrono::steady_clock::now();                                        \
    time += std::chrono::duration_cast<std::chrono::microseconds>(end - start).count(); \
  } while (false);

#define TIME_NANO_OR_RAISE(time, expr)                                                 \
  do {                                                                                 \
    auto start = std::chrono::steady_clock::now();                                     \
    auto __s = (expr);                                                                 \
    if (!__s.ok()) {                                                                   \
      return __s;                                                                      \
    }                                                                                  \
    auto end = std::chrono::steady_clock::now();                                       \
    time += std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count(); \
  } while (false);

#define TIME_NANO_OR_THROW(time, expr)                                                 \
  do {                                                                                 \
    auto start = std::chrono::steady_clock::now();                                     \
    auto __s = (expr);                                                                 \
    if (!__s.ok()) {                                                                   \
      throw JniPendingException(__s.message());                                        \
    }                                                                                  \
    auto end = std::chrono::steady_clock::now();                                       \
    time += std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count(); \
  } while (false);

#define VECTOR_PRINT(v, name)          \
  std::cout << "[" << name << "]:";    \
  for (int i = 0; i < v.size(); i++) { \
    if (i != v.size() - 1)             \
      std::cout << v[i] << ",";        \
    else                               \
      std::cout << v[i];               \
  }                                    \
  std::cout << std::endl;

#define THROW_NOT_OK(expr)                      \
  do {                                          \
    auto __s = (expr);                          \
    if (!__s.ok()) {                            \
      throw JniPendingException(__s.message()); \
    }                                           \
  } while (false);

#define TIME_TO_STRING(time) \
  (time > 10000 ? time / 1000 : time) << (time > 10000 ? " ms" : " us")

#define TIME_NANO_TO_STRING(time)                                \
  (time > 1e7 ? time / 1e6 : ((time > 1e4) ? time / 1e3 : time)) \
      << (time > 1e7 ? "ms" : (time > 1e4 ? "us" : "ns"))
