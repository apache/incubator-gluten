/*
 * licensed to the apache software foundation (asf) under one or more
 * contributor license agreements.  see the notice file distributed with
 * this work for additional information regarding copyright ownership.
 * the asf licenses this file to you under the apache license, version 2.0
 * (the "license"); you may not use this file except in compliance with
 * the license.  you may obtain a copy of the license at
 *
 *    http://www.apache.org/licenses/license-2.0
 *
 * unless required by applicable law or agreed to in writing, software
 * distributed under the license is distributed on an "as is" basis,
 * without warranties or conditions of any kind, either express or implied.
 * see the license for the specific language governing permissions and
 * limitations under the license.
 */

#include "shuffle/Dictionary.h"

namespace gluten {

static DictionaryWriterFactory dictionaryWriterFactory;

void registerShuffleDictionaryWriterFactory(DictionaryWriterFactory factory) {
  if (dictionaryWriterFactory) {
    throw std::runtime_error("DictionaryWriter factory already registered.");
  }
  dictionaryWriterFactory = std::move(factory);
}

std::unique_ptr<DictionaryWriter> createDictionaryWriter(MemoryManager* memoryManager) {
  auto& factory = dictionaryWriterFactory;
  if (!factory) {
    throw std::runtime_error("DictionaryWriter factory not registered.");
  }
  return factory(memoryManager);
}

} // namespace gluten
