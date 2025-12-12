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
#include <cassert>
#include <cstdint>
#include <memory>
#include <string_view>
#include <utility>
#include <vector>

#include "paimon/common/data/binary_array.h"
#include "paimon/common/data/binary_string.h"
#include "paimon/common/data/internal_array.h"
#include "paimon/common/data/internal_map.h"
#include "paimon/common/data/internal_row.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {
class Bytes;
class InternalMap;
class InternalRow;

/// The array which is made up by several arrays.
class DataEvolutionArray : public InternalArray {
 public:
    DataEvolutionArray(const std::vector<BinaryArray>& arrays,
                       const std::vector<int32_t>& array_offsets,
                       const std::vector<int32_t>& field_offsets)
        : arrays_(arrays), array_offsets_(array_offsets), field_offsets_(field_offsets) {
        assert(!arrays_.empty());
    }

    int32_t Size() const override {
        return array_offsets_.size();
    }

    bool IsNullAt(int32_t pos) const override {
        if (array_offsets_[pos] < 0) {
            return true;
        }
        return arrays_[array_offsets_[pos]].IsNullAt(field_offsets_[pos]);
    }

    bool GetBoolean(int32_t pos) const override {
        return arrays_[array_offsets_[pos]].GetBoolean(field_offsets_[pos]);
    }

    char GetByte(int32_t pos) const override {
        return arrays_[array_offsets_[pos]].GetByte(field_offsets_[pos]);
    }

    int16_t GetShort(int32_t pos) const override {
        return arrays_[array_offsets_[pos]].GetShort(field_offsets_[pos]);
    }

    int32_t GetInt(int32_t pos) const override {
        return arrays_[array_offsets_[pos]].GetInt(field_offsets_[pos]);
    }

    int32_t GetDate(int32_t pos) const override {
        return arrays_[array_offsets_[pos]].GetDate(field_offsets_[pos]);
    }

    int64_t GetLong(int32_t pos) const override {
        return arrays_[array_offsets_[pos]].GetLong(field_offsets_[pos]);
    }

    float GetFloat(int32_t pos) const override {
        return arrays_[array_offsets_[pos]].GetFloat(field_offsets_[pos]);
    }

    double GetDouble(int32_t pos) const override {
        return arrays_[array_offsets_[pos]].GetDouble(field_offsets_[pos]);
    }

    BinaryString GetString(int32_t pos) const override {
        return arrays_[array_offsets_[pos]].GetString(field_offsets_[pos]);
    }

    std::string_view GetStringView(int32_t pos) const override {
        return arrays_[array_offsets_[pos]].GetStringView(field_offsets_[pos]);
    }

    Decimal GetDecimal(int32_t pos, int32_t precision, int32_t scale) const override {
        return arrays_[array_offsets_[pos]].GetDecimal(field_offsets_[pos], precision, scale);
    }

    Timestamp GetTimestamp(int32_t pos, int32_t precision) const override {
        return arrays_[array_offsets_[pos]].GetTimestamp(field_offsets_[pos], precision);
    }

    std::shared_ptr<Bytes> GetBinary(int32_t pos) const override {
        return arrays_[array_offsets_[pos]].GetBinary(field_offsets_[pos]);
    }

    std::shared_ptr<InternalArray> GetArray(int32_t pos) const override {
        return arrays_[array_offsets_[pos]].GetArray(field_offsets_[pos]);
    }

    std::shared_ptr<InternalMap> GetMap(int32_t pos) const override {
        return arrays_[array_offsets_[pos]].GetMap(field_offsets_[pos]);
    }

    std::shared_ptr<InternalRow> GetRow(int32_t pos, int32_t num_fields) const override {
        return arrays_[array_offsets_[pos]].GetRow(field_offsets_[pos], num_fields);
    }

    Result<std::vector<char>> ToBooleanArray() const override {
        return Status::Invalid("DataEvolutionArray not support ToBooleanArray");
    }

    Result<std::vector<char>> ToByteArray() const override {
        return Status::Invalid("DataEvolutionArray not support ToByteArray");
    }

    Result<std::vector<int16_t>> ToShortArray() const override {
        return Status::Invalid("DataEvolutionArray not support ToShortArray");
    }

    Result<std::vector<int32_t>> ToIntArray() const override {
        return Status::Invalid("DataEvolutionArray not support ToIntArray");
    }

    Result<std::vector<int64_t>> ToLongArray() const override {
        std::vector<int64_t> result;
        result.reserve(Size());
        for (int32_t i = 0; i < Size(); i++) {
            assert(!IsNullAt(i));
            result.push_back(GetLong(i));
        }
        return result;
    }

    Result<std::vector<float>> ToFloatArray() const override {
        return Status::Invalid("DataEvolutionArray not support ToFloatArray");
    }

    Result<std::vector<double>> ToDoubleArray() const override {
        return Status::Invalid("DataEvolutionArray not support ToDoubleArray");
    }

 private:
    std::vector<BinaryArray> arrays_;
    std::vector<int32_t> array_offsets_;
    std::vector<int32_t> field_offsets_;
};
}  // namespace paimon
