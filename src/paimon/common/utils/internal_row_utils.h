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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/type.h"
#include "paimon/common/data/binary_array.h"
#include "paimon/common/data/binary_array_writer.h"
#include "paimon/common/data/binary_string.h"
#include "paimon/common/data/internal_array.h"
#include "paimon/common/data/internal_row.h"
#include "paimon/result.h"

namespace paimon {
class MemoryPool;

class InternalRowUtils {
 public:
    InternalRowUtils() = delete;
    ~InternalRowUtils() = delete;

    static std::vector<std::optional<std::string>> FromStringArrayData(const InternalArray* data) {
        std::vector<std::optional<std::string>> strs;
        strs.reserve(data->Size());
        for (auto i = 0; i < data->Size(); i++) {
            if (!data->IsNullAt(i)) {
                strs.emplace_back(data->GetString(i).ToString());
            } else {
                strs.emplace_back(std::nullopt);
            }
        }
        return strs;
    }

    static std::vector<std::string> FromNotNullStringArrayData(const InternalArray* data) {
        std::vector<std::string> strs;
        strs.reserve(data->Size());
        for (auto i = 0; i < data->Size(); i++) {
            strs.push_back(data->GetString(i).ToString());
        }
        return strs;
    }

    static BinaryArray ToStringArrayData(const std::vector<std::optional<std::string>>& strs,
                                         const std::shared_ptr<MemoryPool>& memory_pool) {
        BinaryArray array;
        BinaryArrayWriter writer(&array, strs.size(), 8, memory_pool.get());
        for (size_t i = 0; i < strs.size(); i++) {
            if (strs[i] == std::nullopt) {
                writer.SetNullAt(i);
            } else {
                writer.WriteString(i, BinaryString::FromString(strs[i].value(), memory_pool.get()));
            }
        }
        writer.Complete();
        return array;
    }

    static BinaryArray ToNotNullStringArrayData(const std::vector<std::string>& strs,
                                                const std::shared_ptr<MemoryPool>& memory_pool) {
        BinaryArray array;
        BinaryArrayWriter writer(&array, strs.size(), 8, memory_pool.get());
        for (size_t i = 0; i < strs.size(); i++) {
            writer.WriteString(i, BinaryString::FromString(strs[i], memory_pool.get()));
        }
        writer.Complete();
        return array;
    }

    static Result<std::vector<InternalRow::FieldGetterFunc>> CreateFieldGetters(
        const std::shared_ptr<arrow::Schema>& value_schema, bool use_view) {
        std::vector<InternalRow::FieldGetterFunc> getters;
        getters.reserve(value_schema->num_fields());
        for (int32_t i = 0; i < value_schema->num_fields(); i++) {
            const auto& field_type = value_schema->field(i)->type();
            PAIMON_ASSIGN_OR_RAISE(InternalRow::FieldGetterFunc getter,
                                   InternalRow::CreateFieldGetter(i, field_type, use_view));
            getters.emplace_back(getter);
        }
        return getters;
    }
};
}  // namespace paimon
