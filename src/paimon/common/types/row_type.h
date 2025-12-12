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

#pragma once

#include <memory>

#include "arrow/api.h"
#include "paimon/common/types/data_type.h"
#include "rapidjson/allocators.h"
#include "rapidjson/document.h"
#include "rapidjson/rapidjson.h"

namespace arrow {
class DataType;
class KeyValueMetadata;
}  // namespace arrow

namespace paimon {

class RowType : public DataType {
 public:
    static constexpr char TYPE[] = "ROW";

    RowType(const std::shared_ptr<arrow::DataType>& type, bool nullable,
            const std::shared_ptr<const arrow::KeyValueMetadata>& metadata);
    rapidjson::Value ToJson(rapidjson::Document::AllocatorType* allocator) const
        noexcept(false) override;
};

}  // namespace paimon
