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
#include <string>
#include <string_view>
#include <vector>

#include "fmt/format.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/binary_string.h"
#include "paimon/common/data/internal_array.h"
#include "paimon/common/data/internal_map.h"
#include "paimon/common/data/internal_row.h"
#include "paimon/common/types/row_kind.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/result.h"

namespace paimon {
class Bytes;
class InternalArray;
class InternalMap;
class RowKind;

/// The row which is made up by several rows.
class DataEvolutionRow : public InternalRow {
 public:
    DataEvolutionRow(const std::vector<BinaryRow>& rows, const std::vector<int32_t>& row_offsets,
                     const std::vector<int32_t>& field_offsets)
        : rows_(rows), row_offsets_(row_offsets), field_offsets_(field_offsets) {
        assert(!rows_.empty());
    }

    Result<const RowKind*> GetRowKind() const override {
        if (!row_kind_) {
            PAIMON_ASSIGN_OR_RAISE(row_kind_, rows_[0].GetRowKind());
        }
        return row_kind_;
    }

    void SetRowKind(const RowKind* kind) override {
        row_kind_ = kind;
    }

    int32_t GetFieldCount() const override {
        return field_offsets_.size();
    }

    bool IsNullAt(int32_t pos) const override {
        if (row_offsets_[pos] < 0) {
            return true;
        }
        return rows_[row_offsets_[pos]].IsNullAt(field_offsets_[pos]);
    }

    bool GetBoolean(int32_t pos) const override {
        return rows_[row_offsets_[pos]].GetBoolean(field_offsets_[pos]);
    }

    char GetByte(int32_t pos) const override {
        return rows_[row_offsets_[pos]].GetByte(field_offsets_[pos]);
    }

    int16_t GetShort(int32_t pos) const override {
        return rows_[row_offsets_[pos]].GetShort(field_offsets_[pos]);
    }

    int32_t GetInt(int32_t pos) const override {
        return rows_[row_offsets_[pos]].GetInt(field_offsets_[pos]);
    }

    int32_t GetDate(int32_t pos) const override {
        return rows_[row_offsets_[pos]].GetDate(field_offsets_[pos]);
    }

    int64_t GetLong(int32_t pos) const override {
        return rows_[row_offsets_[pos]].GetLong(field_offsets_[pos]);
    }

    float GetFloat(int32_t pos) const override {
        return rows_[row_offsets_[pos]].GetFloat(field_offsets_[pos]);
    }

    double GetDouble(int32_t pos) const override {
        return rows_[row_offsets_[pos]].GetDouble(field_offsets_[pos]);
    }

    BinaryString GetString(int32_t pos) const override {
        return rows_[row_offsets_[pos]].GetString(field_offsets_[pos]);
    }

    std::shared_ptr<Bytes> GetBinary(int32_t pos) const override {
        return rows_[row_offsets_[pos]].GetBinary(field_offsets_[pos]);
    }

    std::string_view GetStringView(int32_t pos) const override {
        return rows_[row_offsets_[pos]].GetStringView(field_offsets_[pos]);
    }

    Timestamp GetTimestamp(int32_t pos, int32_t precision) const override {
        return rows_[row_offsets_[pos]].GetTimestamp(field_offsets_[pos], precision);
    }

    Decimal GetDecimal(int32_t pos, int32_t precision, int32_t scale) const override {
        return rows_[row_offsets_[pos]].GetDecimal(field_offsets_[pos], precision, scale);
    }

    std::shared_ptr<InternalRow> GetRow(int32_t pos, int32_t num_fields) const override {
        return rows_[row_offsets_[pos]].GetRow(field_offsets_[pos], num_fields);
    }

    std::shared_ptr<InternalArray> GetArray(int32_t pos) const override {
        return rows_[row_offsets_[pos]].GetArray(field_offsets_[pos]);
    }

    std::shared_ptr<InternalMap> GetMap(int32_t pos) const override {
        return rows_[row_offsets_[pos]].GetMap(field_offsets_[pos]);
    }

    std::string ToString() const override {
        return "DataEvolutionRow";
    }

 private:
    mutable const RowKind* row_kind_ = nullptr;
    std::vector<BinaryRow> rows_;
    std::vector<int32_t> row_offsets_;
    std::vector<int32_t> field_offsets_;
};
}  // namespace paimon
