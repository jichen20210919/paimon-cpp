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

#include "paimon/common/io/offset_input_stream.h"

#include <utility>

#include "fmt/format.h"
#include "paimon/macros.h"

namespace paimon {
Result<std::unique_ptr<OffsetInputStream>> OffsetInputStream::Create(
    const std::shared_ptr<InputStream>& wrapped, int64_t length, int64_t offset) {
    if (PAIMON_UNLIKELY(wrapped == nullptr)) {
        return Status::Invalid("input stream is null pointer");
    }
    if (PAIMON_UNLIKELY(offset < 0)) {
        return Status::Invalid(fmt::format("offset {} is less than 0", offset));
    }
    if (PAIMON_UNLIKELY(length < -1)) {
        return Status::Invalid(fmt::format("length {} is less than -1", length));
    }
    PAIMON_ASSIGN_OR_RAISE(uint64_t total_length, wrapped->Length());
    if (PAIMON_UNLIKELY((uint64_t)offset > total_length)) {
        return Status::Invalid(
            fmt::format("offset {} exceed total length {}", offset, total_length));
    }
    if (length == -1) {
        // length == -1 means it's dynamic length, should read to the end
        length = total_length - offset;
    }
    if (PAIMON_UNLIKELY((uint64_t)offset + (uint64_t)length > total_length)) {
        return Status::Invalid(fmt::format("offset {} + length {} exceed total length {}", offset,
                                           length, total_length));
    }
    PAIMON_RETURN_NOT_OK(wrapped->Seek(offset, SeekOrigin::FS_SEEK_SET));
    return std::unique_ptr<OffsetInputStream>(
        new OffsetInputStream(std::move(wrapped), length, offset));
}

OffsetInputStream::OffsetInputStream(const std::shared_ptr<InputStream>& wrapped, int64_t length,
                                     int64_t offset)
    : wrapped_(wrapped), length_(length), offset_(offset) {}

Status OffsetInputStream::Seek(int64_t offset, SeekOrigin origin) {
    switch (origin) {
        case SeekOrigin::FS_SEEK_SET: {
            inner_position_ = offset;
            PAIMON_RETURN_NOT_OK(AssertBoundary(inner_position_));
            return wrapped_->Seek(offset_ + inner_position_, SeekOrigin::FS_SEEK_SET);
        }
        case SeekOrigin::FS_SEEK_CUR: {
            inner_position_ += offset;
            PAIMON_RETURN_NOT_OK(AssertBoundary(inner_position_));
            return wrapped_->Seek(offset, SeekOrigin::FS_SEEK_CUR);
        }
        case SeekOrigin::FS_SEEK_END: {
            inner_position_ = length_ + offset;
            PAIMON_RETURN_NOT_OK(AssertBoundary(inner_position_));
            return wrapped_->Seek(offset_ + inner_position_, SeekOrigin::FS_SEEK_SET);
        }
        default:
            return Status::Invalid(
                "invalid SeekOrigin, only support FS_SEEK_SET, FS_SEEK_CUR, and FS_SEEK_END");
    }
    return Status::OK();
}

Result<int32_t> OffsetInputStream::Read(char* buffer, uint32_t size) {
    PAIMON_RETURN_NOT_OK(AssertBoundary(inner_position_ + size));
    inner_position_ += size;
    return wrapped_->Read(buffer, size);
}

Result<int32_t> OffsetInputStream::Read(char* buffer, uint32_t size, uint64_t offset) {
    PAIMON_RETURN_NOT_OK(AssertBoundary(offset));
    PAIMON_RETURN_NOT_OK(AssertBoundary(offset + size));
    return wrapped_->Read(buffer, size, offset_ + offset);
}

void OffsetInputStream::ReadAsync(char* buffer, uint32_t size, uint64_t offset,
                                  std::function<void(Status)>&& callback) {
    auto status = AssertBoundary(offset);
    if (!status.ok()) {
        callback(status);
        return;
    }
    status = AssertBoundary(offset + size);
    if (!status.ok()) {
        callback(status);
        return;
    }
    wrapped_->ReadAsync(buffer, size, offset_ + offset, std::move(callback));
}

Status OffsetInputStream::Close() {
    return wrapped_->Close();
}

Result<std::string> OffsetInputStream::GetUri() const {
    return wrapped_->GetUri();
}

Result<int64_t> OffsetInputStream::GetPos() const {
    return inner_position_;
}

Result<uint64_t> OffsetInputStream::Length() const {
    return length_;
}

Status OffsetInputStream::AssertBoundary(int32_t inner_pos) const {
    if (inner_pos < 0 || inner_pos > length_) {
        return Status::Invalid(
            fmt::format("OffsetInputStream assert boundary failed: inner pos {} exceed length {}",
                        inner_pos, length_));
    }
    return Status::OK();
}

}  // namespace paimon
