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

#include "paimon/common/file_index/bitmap/bitmap_file_index_meta_v2.h"

#include <algorithm>
#include <cassert>
#include <functional>
#include <utility>

#include "fmt/format.h"
#include "paimon/common/file_index/bitmap/bitmap_file_index.h"
#include "paimon/common/options/memory_size.h"
#include "paimon/common/utils/field_type_utils.h"
#include "paimon/file_index/file_index_result.h"
#include "paimon/fs/file_system.h"
#include "paimon/io/buffered_input_stream.h"
#include "paimon/io/data_input_stream.h"

namespace paimon {
BitmapFileIndexMetaV2::BitmapFileIndexMetaV2(const FieldType& type, int32_t total_length,
                                             const std::shared_ptr<MemoryPool>& pool)
    : BitmapFileIndexMeta(type, total_length, pool) {}

BitmapFileIndexMetaV2::BitmapFileIndexMetaV2(const FieldType& type, int32_t row_count,
                                             bool has_null_value, const Entry& null_value_entry,
                                             std::vector<Entry>&& write_entries,
                                             const std::map<std::string, std::string>& options,
                                             const std::shared_ptr<MemoryPool>& pool)
    : BitmapFileIndexMeta(type, row_count, has_null_value, null_value_entry,
                          std::move(write_entries), pool),
      options_(options) {}

Status BitmapFileIndexMetaV2::Serialize(
    const std::shared_ptr<MemorySegmentOutputStream>& output_stream) {
    auto iter = options_.find(BitmapFileIndex::INDEX_BLOCK_SIZE);
    if (iter != options_.end()) {
        PAIMON_ASSIGN_OR_RAISE(block_size_limit_, MemorySize::ParseBytes(iter->second));
    }

    output_stream->WriteValue<int32_t>(row_count_);
    // non-null bitmap number
    output_stream->WriteValue<int32_t>(static_cast<int32_t>(write_entries_.size()));
    output_stream->WriteValue<bool>(has_null_value_);
    if (has_null_value_) {
        output_stream->WriteValue<int32_t>(null_value_entry_.offset);
        output_stream->WriteValue<int32_t>(null_value_entry_.length);
    }

    if (!write_entries_.empty()) {
        index_blocks_.push_back(
            std::make_unique<BitmapIndexBlock>(this, /*offset=*/0, pool_.get()));
    }
    std::sort(write_entries_.begin(), write_entries_.end(), [](const Entry& e1, const Entry& e2) {
        return e1.key.CompareTo(e2.key).value() < 0;
    });

    for (const auto& entry : write_entries_) {
        auto& last_block = index_blocks_.back();
        PAIMON_ASSIGN_OR_RAISE(bool added, last_block->TryAdd(entry));
        if (!added) {
            auto new_block = std::make_unique<BitmapIndexBlock>(
                this, /*offset=*/last_block->offset + last_block->serialized_bytes, pool_.get());
            index_blocks_.push_back(std::move(new_block));
            PAIMON_ASSIGN_OR_RAISE(bool new_added, index_blocks_.back()->TryAdd(entry));
            if (!new_added) {
                return Status::Invalid("add entry to BitmapIndexBlock failed");
            }
        }
    }

    output_stream->WriteValue<int32_t>(index_blocks_.size());

    int32_t bitmap_body_offset = 0;
    PAIMON_ASSIGN_OR_RAISE(std::function<void(const Literal&)> write_value,
                           GetValueWriter(output_stream));
    for (const auto& block : index_blocks_) {
        // secondary entry
        write_value(block->key);
        output_stream->WriteValue<int32_t>(block->offset);
        bitmap_body_offset += block->serialized_bytes;
    }
    // bitmap body offset
    output_stream->WriteValue<int32_t>(bitmap_body_offset);

    // bitmap index blocks
    for (const auto& block : index_blocks_) {
        output_stream->WriteValue<int32_t>(block->entry_list.size());
        for (const auto& entry : block->entry_list) {
            write_value(entry.key);
            output_stream->WriteValue<int32_t>(entry.offset);
            output_stream->WriteValue<int32_t>(entry.length);
        }
    }
    return Status::OK();
}

Result<const BitmapFileIndexMeta::Entry*> BitmapFileIndexMetaV2::FindEntry(
    const Literal& bitmap_id) {
    if (bitmap_id.IsNull()) {
        if (has_null_value_) {
            return &null_value_entry_;
        }
    } else {
        BitmapIndexBlock* block = FindBlock(bitmap_id);
        if (block) {
            return block->FindEntry(bitmap_id);
        }
    }
    return nullptr;
}

BitmapFileIndexMetaV2::BitmapIndexBlock* BitmapFileIndexMetaV2::FindBlock(
    const Literal& bitmap_id) {
    if (index_blocks_.empty()) {
        return nullptr;
    }
    auto iter = std::lower_bound(
        index_blocks_.begin(), index_blocks_.end(), bitmap_id,
        [](const std::unique_ptr<BitmapIndexBlock>& block, const Literal& literal) {
            return block->key.CompareTo(literal).value() < 0;
        });
    // bitmap_id < all data in index
    if (iter == index_blocks_.begin() && (*iter)->key != bitmap_id) {
        return nullptr;
    }
    if (iter == index_blocks_.end() || (*iter)->key != bitmap_id) {
        iter--;
    }
    return iter->get();
}

Status BitmapFileIndexMetaV2::Deserialize(const std::shared_ptr<InputStream>& input_stream) {
    PAIMON_ASSIGN_OR_RAISE(body_start_, input_stream->GetPos());
    auto buffered_input_stream = std::make_shared<BufferedInputStream>(
        input_stream, BufferedInputStream::DEFAULT_BUFFER_SIZE, pool_.get());
    auto in = std::make_shared<DataInputStream>(buffered_input_stream);

    PAIMON_ASSIGN_OR_RAISE(std::function<Result<Literal>()> value_reader,
                           GetValueReader(in, /*move_body_start=*/true));

    PAIMON_ASSIGN_OR_RAISE(row_count_, ReadAndMoveBodyStart<int32_t>(in));
    PAIMON_ASSIGN_OR_RAISE([[maybe_unused]] int32_t non_null_bitmap_number,
                           ReadAndMoveBodyStart<int32_t>(in));
    PAIMON_ASSIGN_OR_RAISE(has_null_value_, ReadAndMoveBodyStart<bool>(in));

    if (has_null_value_) {
        PAIMON_ASSIGN_OR_RAISE(int32_t offset, ReadAndMoveBodyStart<int32_t>(in));
        PAIMON_ASSIGN_OR_RAISE(int32_t length, ReadAndMoveBodyStart<int32_t>(in));
        null_value_entry_ = Entry(Literal(data_type_), offset, length);
    }

    PAIMON_ASSIGN_OR_RAISE(int32_t bitmap_block_number, ReadAndMoveBodyStart<int32_t>(in));

    for (int32_t i = 0; i < bitmap_block_number; i++) {
        PAIMON_ASSIGN_OR_RAISE(Literal key, value_reader());
        PAIMON_ASSIGN_OR_RAISE(int32_t offset, ReadAndMoveBodyStart<int32_t>(in));
        index_blocks_.push_back(
            std::make_unique<BitmapIndexBlock>(this, key, offset, input_stream, pool_.get()));
    }

    PAIMON_ASSIGN_OR_RAISE(int32_t bitmap_body_offset, ReadAndMoveBodyStart<int32_t>(in));

    index_block_start_ = body_start_;
    body_start_ += bitmap_body_offset;
    return Status::OK();
}

BitmapFileIndexMetaV2::BitmapIndexBlock::BitmapIndexBlock(
    BitmapFileIndexMetaV2* outer, const Literal& _key, int32_t _offset,
    const std::shared_ptr<InputStream>& input_stream, MemoryPool* pool)
    : key(_key), offset(_offset), input_stream_(input_stream), outer_(outer), pool_(pool) {
    assert(outer_);
    assert(pool_);
}

BitmapFileIndexMetaV2::BitmapIndexBlock::BitmapIndexBlock(BitmapFileIndexMetaV2* outer,
                                                          int32_t _offset, MemoryPool* pool)
    : key(outer->data_type_), offset(_offset), outer_(outer), pool_(pool) {
    assert(outer_);
    assert(pool_);
}

Status BitmapFileIndexMetaV2::BitmapIndexBlock::TryDeserialize() {
    if (!is_deserialized_) {
        PAIMON_RETURN_NOT_OK(
            input_stream_->Seek(outer_->index_block_start_ + offset, SeekOrigin::FS_SEEK_SET));
        auto buffered_input_stream = std::make_shared<BufferedInputStream>(
            input_stream_, BufferedInputStream::DEFAULT_BUFFER_SIZE, pool_);
        auto in = std::make_shared<DataInputStream>(buffered_input_stream);
        PAIMON_ASSIGN_OR_RAISE(std::function<Result<Literal>()> value_reader,
                               outer_->GetValueReader(in, /*move_body_start=*/false));
        PAIMON_ASSIGN_OR_RAISE(int32_t entry_num, in->ReadValue<int32_t>());
        entry_list.reserve(entry_num);
        for (int32_t i = 0; i < entry_num; i++) {
            PAIMON_ASSIGN_OR_RAISE(Literal key, value_reader());
            PAIMON_ASSIGN_OR_RAISE(int32_t offset, in->ReadValue<int32_t>());
            PAIMON_ASSIGN_OR_RAISE(int32_t length, in->ReadValue<int32_t>());
            entry_list.emplace_back(key, offset, length);
        }
        is_deserialized_ = true;
    }
    return Status::OK();
}

Result<const BitmapFileIndexMeta::Entry*> BitmapFileIndexMetaV2::BitmapIndexBlock::FindEntry(
    const Literal& bitmap_id) {
    PAIMON_RETURN_NOT_OK(TryDeserialize());
    auto iter = std::lower_bound(entry_list.begin(), entry_list.end(), bitmap_id,
                                 [](const Entry& entry, const Literal& literal) {
                                     return entry.key.CompareTo(literal).value() < 0;
                                 });
    if (iter != entry_list.end() && iter->key == bitmap_id) {
        return iter.base();
    }
    return nullptr;
}

Result<bool> BitmapFileIndexMetaV2::BitmapIndexBlock::TryAdd(const Entry& entry) {
    // null literal will not be added to block
    if (key.IsNull()) {
        key = entry.key;
    }
    PAIMON_ASSIGN_OR_RAISE(int32_t key_bytes, GetKeyBytes(entry.key));
    int32_t entry_bytes = 2 * sizeof(int32_t) + key_bytes;
    if (serialized_bytes + entry_bytes > outer_->block_size_limit_) {
        return false;
    }
    serialized_bytes += entry_bytes;
    entry_list.push_back(entry);
    return true;
}

Result<int32_t> BitmapFileIndexMetaV2::BitmapIndexBlock::GetKeyBytes(const Literal& literal) {
    auto field_type = literal.GetType();
    switch (field_type) {
        case FieldType::BOOLEAN:
            return sizeof(bool);
        case FieldType::TINYINT:
            return sizeof(int8_t);
        case FieldType::SMALLINT:
            return sizeof(int16_t);
        case FieldType::DATE:
        case FieldType::INT:
            return sizeof(int32_t);
        case FieldType::BIGINT:
            return sizeof(int64_t);
        case FieldType::FLOAT:
            return sizeof(float);
        case FieldType::DOUBLE:
            return sizeof(double);
        case FieldType::STRING:
            return sizeof(int32_t) + literal.GetValue<std::string>().size();
        default:
            return Status::Invalid(fmt::format("invalid index field type {}",
                                               FieldTypeUtils::FieldTypeToString(field_type)));
    }
}

}  // namespace paimon
