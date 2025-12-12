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
#include <string>
#include <unordered_map>

#include "paimon/result.h"
#include "paimon/visibility.h"

namespace arrow {
class Field;
class KeyValueMetadata;
class Schema;
class StructArray;
}  // namespace arrow

namespace paimon {
/// Utils for blob type.
class PAIMON_EXPORT BlobUtils {
 public:
    BlobUtils() = delete;
    ~BlobUtils() = delete;

    struct SeparatedSchemas {
        /// Non-blob fields
        std::shared_ptr<arrow::Schema> main_schema;
        /// Blob fields only
        std::shared_ptr<arrow::Schema> blob_schema;
    };

    struct SeparatedStructArrays {
        /// Non-blob fields
        std::shared_ptr<arrow::StructArray> main_array;
        /// Blob fields only
        std::shared_ptr<arrow::StructArray> blob_array;
    };

    static SeparatedSchemas SeparateBlobSchema(const std::shared_ptr<arrow::Schema>& schema);

    static Result<SeparatedStructArrays> SeparateBlobArray(
        const std::shared_ptr<arrow::StructArray>& struct_array);

    static bool IsBlobField(const std::shared_ptr<arrow::Field>& field);
    static bool IsBlobMetadata(const std::shared_ptr<const arrow::KeyValueMetadata>& metadata);
    static bool IsBlobFile(const std::string& file_name);

    static std::shared_ptr<arrow::Field> ToArrowField(
        const std::string& field_name, bool nullable = false,
        std::unordered_map<std::string, std::string> metadata = {});
};

}  // namespace paimon
