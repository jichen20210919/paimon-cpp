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

#include "paimon/format/file_format_factory.h"

#include "fmt/format.h"
#include "paimon/factories/factory_creator.h"
#include "paimon/format/file_format.h"
#include "paimon/status.h"

namespace paimon {

FileFormatFactory::~FileFormatFactory() = default;

Result<std::unique_ptr<FileFormat>> FileFormatFactory::Get(
    const std::string& identifier, const std::map<std::string, std::string>& options) {
    auto factory_creator = FactoryCreator::GetInstance();
    if (factory_creator == nullptr) {
        return Status::Invalid("factory creator is null pointer");
    }
    auto file_format_factory =
        dynamic_cast<FileFormatFactory*>(factory_creator->Create(identifier));
    if (file_format_factory == nullptr) {
        return Status::Invalid(fmt::format(
            "Could not find a FileFormatFactory implementation class for format '{}'", identifier));
    }
    return file_format_factory->Create(options);
}

}  // namespace paimon
