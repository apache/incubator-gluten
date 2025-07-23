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

#include "shuffle/Dictionary.h"
#include "utils/Exception.h"

namespace gluten {

static ShuffleDictionaryWriterFactory dictionaryWriterFactory;

void registerShuffleDictionaryWriterFactory(ShuffleDictionaryWriterFactory factory) {
  if (dictionaryWriterFactory) {
    throw GlutenException("DictionaryWriter factory already registered.");
  }
  dictionaryWriterFactory = std::move(factory);
}

std::unique_ptr<ShuffleDictionaryWriter> createDictionaryWriter(
    MemoryManager* memoryManager,
    arrow::util::Codec* codec) {
  if (!dictionaryWriterFactory) {
    throw GlutenException("DictionaryWriter factory not registered.");
  }
  return dictionaryWriterFactory(memoryManager, codec);
}

} // namespace gluten
