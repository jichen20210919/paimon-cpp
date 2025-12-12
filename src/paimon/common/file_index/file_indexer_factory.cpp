/*
 * Copyright 2024-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "paimon/file_index/file_indexer_factory.h"

#include <cassert>
#include <utility>

#include "paimon/factories/factory_creator.h"
#include "paimon/file_index/file_indexer.h"
#include "paimon/status.h"

namespace paimon {
FileIndexerFactory::~FileIndexerFactory() = default;

Result<std::unique_ptr<FileIndexer>> FileIndexerFactory::Get(
    const std::string& identifier, const std::map<std::string, std::string>& options) {
    auto factory_creator = FactoryCreator::GetInstance();
    if (factory_creator == nullptr) {
        assert(false);
        return Status::Invalid("factory creator is null pointer");
    }
    auto file_indexer_factory =
        dynamic_cast<FileIndexerFactory*>(factory_creator->Create(identifier));
    if (file_indexer_factory == nullptr) {
        // if an index type is not found, return nullptr to skip this index instead of return error
        return std::unique_ptr<FileIndexer>();
    }
    return file_indexer_factory->Create(options);
}
}  // namespace paimon
