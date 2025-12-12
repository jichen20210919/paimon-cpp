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

#include "paimon/record_batch.h"

#include <cassert>
#include <cstddef>
#include <limits>
#include <utility>

#include "arrow/c/abi.h"
#include "arrow/c/helpers.h"
#include "fmt/format.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

class RecordBatchBuilder::Impl {
 public:
    friend class RecordBatchBuilder;
    static constexpr int32_t INVALID_BUCKET = std::numeric_limits<int32_t>::min();

    void MoveData(::ArrowArray* data) {
        assert(data);
        ReleaseData();
        data_ = new ::ArrowArray();
        ArrowArrayMove(data, data_);
    }

    void Reset() {
        partition_.clear();
        bucket_ = INVALID_BUCKET;
        row_kinds_.clear();
        ReleaseData();
    }

 private:
    void ReleaseData() {
        if (data_) {
            ArrowArrayRelease(data_);
            delete data_;
            data_ = nullptr;
        }
    }

    std::map<std::string, std::string> partition_;
    int32_t bucket_ = INVALID_BUCKET;
    std::vector<RecordBatch::RowKind> row_kinds_;
    ::ArrowArray* data_ = nullptr;
};

RecordBatchBuilder::RecordBatchBuilder(::ArrowArray* data) : impl_(std::make_unique<Impl>()) {
    impl_->MoveData(data);
}

RecordBatchBuilder::~RecordBatchBuilder() {
    if (impl_) {
        impl_->Reset();
    }
}

RecordBatchBuilder& RecordBatchBuilder::MoveData(::ArrowArray* data) {
    impl_->MoveData(data);
    return *this;
}

RecordBatchBuilder& RecordBatchBuilder::SetRowKinds(
    const std::vector<RecordBatch::RowKind>& row_kinds) {
    impl_->row_kinds_ = row_kinds;
    return *this;
}

RecordBatchBuilder& RecordBatchBuilder::SetPartition(
    const std::map<std::string, std::string>& partition) {
    impl_->partition_ = partition;
    return *this;
}

RecordBatchBuilder& RecordBatchBuilder::SetBucket(int32_t bucket) {
    impl_->bucket_ = bucket;
    return *this;
}

Result<std::unique_ptr<RecordBatch>> RecordBatchBuilder::Finish() {
    ScopeGuard guard([this]() { impl_->Reset(); });
    if (impl_->data_ == nullptr) {
        return Status::Invalid("data is null pointer");
    }
    if (ArrowArrayIsReleased(impl_->data_)) {
        return Status::Invalid("data is released");
    }
    if (!impl_->row_kinds_.empty() &&
        impl_->row_kinds_.size() != static_cast<size_t>(impl_->data_->length)) {
        return Status::Invalid(fmt::format("data size {} does not match with row_kinds size {}",
                                           impl_->data_->length, impl_->row_kinds_.size()));
    }
    return std::make_unique<RecordBatch>(impl_->partition_, impl_->bucket_, impl_->row_kinds_,
                                         impl_->data_);
}

RecordBatch::RecordBatch(const std::map<std::string, std::string>& partition, int32_t bucket,
                         const std::vector<RowKind>& row_kinds, ArrowArray* data)
    : partition_(partition), bucket_(bucket), row_kinds_(row_kinds) {
    data_ = new ArrowArray();
    ArrowArrayMove(data, data_);
}

RecordBatch::~RecordBatch() {
    if (data_) {
        ArrowArrayRelease(data_);
        delete data_;
    }
}

RecordBatch::RecordBatch(RecordBatch&& other) {
    if (this != &other) {
        partition_ = std::move(other.partition_);
        bucket_ = other.bucket_;
        row_kinds_ = std::move(other.row_kinds_);
        data_ = other.data_;
        other.data_ = nullptr;
    }
}

RecordBatch& RecordBatch::operator=(RecordBatch&& other) {
    if (this == &other) {
        return *this;
    }
    partition_ = std::move(other.partition_);
    bucket_ = other.bucket_;
    row_kinds_ = std::move(other.row_kinds_);
    if (data_) {
        ArrowArrayRelease(data_);
        delete data_;
    }
    data_ = other.data_;
    other.data_ = nullptr;
    return *this;
}

bool RecordBatch::HasSpecifiedBucket() const {
    return bucket_ != RecordBatchBuilder::Impl::INVALID_BUCKET;
}

}  // namespace paimon
