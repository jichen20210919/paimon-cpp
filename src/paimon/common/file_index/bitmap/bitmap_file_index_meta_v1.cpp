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

#include "paimon/common/file_index/bitmap/bitmap_file_index_meta_v1.h"

#include <cassert>
#include <functional>
#include <utility>

#include "paimon/file_index/file_index_result.h"
#include "paimon/fs/file_system.h"
#include "paimon/io/buffered_input_stream.h"
#include "paimon/io/data_input_stream.h"
#include "paimon/predicate/literal.h"

namespace paimon {
BitmapFileIndexMetaV1::BitmapFileIndexMetaV1(const FieldType& type, int32_t start,
                                             int32_t total_length,
                                             const std::shared_ptr<MemoryPool>& pool)
    : BitmapFileIndexMeta(type, total_length, pool), start_(start) {}

BitmapFileIndexMetaV1::BitmapFileIndexMetaV1(const FieldType& type, int32_t row_count,
                                             bool has_null_value, const Entry& null_value_entry,
                                             std::vector<Entry>&& write_entries,
                                             const std::shared_ptr<MemoryPool>& pool)
    : BitmapFileIndexMeta(type, row_count, has_null_value, null_value_entry,
                          std::move(write_entries), pool) {}

Result<const BitmapFileIndexMeta::Entry*> BitmapFileIndexMetaV1::FindEntry(
    const Literal& bitmap_id) {
    if (bitmap_id.IsNull()) {
        if (has_null_value_) {
            return &null_value_entry_;
        }
    }
    auto entry_iter = entries_.find(bitmap_id);
    if (entry_iter != entries_.end()) {
        return &entry_iter->second;
    }
    return nullptr;
}

Status BitmapFileIndexMetaV1::Serialize(
    const std::shared_ptr<MemorySegmentOutputStream>& output_stream) {
    PAIMON_ASSIGN_OR_RAISE(std::function<void(const Literal&)> write_value,
                           GetValueWriter(output_stream));
    output_stream->WriteValue<int32_t>(row_count_);
    // non-null bitmap number
    output_stream->WriteValue<int32_t>(static_cast<int32_t>(write_entries_.size()));
    output_stream->WriteValue<bool>(has_null_value_);
    if (has_null_value_) {
        output_stream->WriteValue<int32_t>(null_value_entry_.offset);
    }
    for (const auto& entry : write_entries_) {
        write_value(entry.key);
        output_stream->WriteValue<int32_t>(entry.offset);
    }
    return Status::OK();
}

Status BitmapFileIndexMetaV1::Deserialize(const std::shared_ptr<InputStream>& input_stream) {
    PAIMON_ASSIGN_OR_RAISE(body_start_, input_stream->GetPos());
    auto buffered_input_stream = std::make_shared<BufferedInputStream>(
        input_stream, BufferedInputStream::DEFAULT_BUFFER_SIZE, pool_.get());
    auto in = std::make_shared<DataInputStream>(buffered_input_stream);

    PAIMON_ASSIGN_OR_RAISE(std::function<Result<Literal>()> value_reader,
                           GetValueReader(in, /*move_body_start=*/true));

    PAIMON_ASSIGN_OR_RAISE(row_count_, ReadAndMoveBodyStart<int32_t>(in));
    PAIMON_ASSIGN_OR_RAISE(int32_t non_null_bitmap_number, ReadAndMoveBodyStart<int32_t>(in));
    PAIMON_ASSIGN_OR_RAISE(has_null_value_, ReadAndMoveBodyStart<bool>(in));

    int32_t null_value_offset = -1;
    if (has_null_value_) {
        PAIMON_ASSIGN_OR_RAISE(null_value_offset, ReadAndMoveBodyStart<int32_t>(in));
    }

    Literal null_value(data_type_);
    Literal last_value = null_value;
    int32_t last_offset = null_value_offset;
    for (int32_t i = 0; i < non_null_bitmap_number; i++) {
        PAIMON_ASSIGN_OR_RAISE(Literal value, value_reader());
        PAIMON_ASSIGN_OR_RAISE(int32_t offset, ReadAndMoveBodyStart<int32_t>(in));
        if (offset >= 0) {
            if (last_offset >= 0) {
                int32_t length = offset - last_offset;
                entries_.emplace(last_value, Entry(last_value, last_offset, length));
            }
            last_offset = offset;
            last_value = value;
        } else {
            // offset is negative indicates offset is the inline bitmap
            entries_.emplace(value, Entry(value, offset, -1));
        }
    }
    if (last_offset >= 0) {
        entries_.emplace(last_value, Entry(last_value, last_offset,
                                           total_length_ - (body_start_ - start_) - last_offset));
    }

    if (has_null_value_) {
        auto null_iter = entries_.find(null_value);
        if (null_iter == entries_.end()) {
            assert(null_value_offset < 0);
            null_value_entry_ = Entry(null_value, null_value_offset, -1);
        } else {
            null_value_entry_ = null_iter->second;
        }
    }
    return Status::OK();
}

}  // namespace paimon
