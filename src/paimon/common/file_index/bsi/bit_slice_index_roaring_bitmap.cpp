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

#include "paimon/common/file_index/bsi/bit_slice_index_roaring_bitmap.h"

#include <utility>

#include "fmt/format.h"
#include "paimon/common/io/memory_segment_output_stream.h"
#include "paimon/common/memory/memory_segment_utils.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/io/data_input_stream.h"

namespace paimon {
class MemoryPool;

const int8_t BitSliceIndexRoaringBitmap::VERSION_1 = 1;
const std::shared_ptr<BitSliceIndexRoaringBitmap>& BitSliceIndexRoaringBitmap::Empty() {
    static std::shared_ptr<BitSliceIndexRoaringBitmap> kEmpty =
        std::shared_ptr<BitSliceIndexRoaringBitmap>(
            new BitSliceIndexRoaringBitmap(/*min=*/0, /*max=*/0, RoaringBitmap32(), {}));
    return kEmpty;
}

BitSliceIndexRoaringBitmap::BitSliceIndexRoaringBitmap(int64_t min, int64_t max,
                                                       RoaringBitmap32&& ebm,
                                                       std::vector<RoaringBitmap32>&& slices)
    : min_(min), max_(max), ebm_(std::move(ebm)), slices_(std::move(slices)) {}

Result<std::shared_ptr<BitSliceIndexRoaringBitmap>> BitSliceIndexRoaringBitmap::Create(
    const std::shared_ptr<ByteArrayInputStream>& input_stream) {
    DataInputStream data_input_stream(input_stream);
    PAIMON_ASSIGN_OR_RAISE(int8_t version, data_input_stream.ReadValue<int8_t>());
    if (version > VERSION_1) {
        return Status::Invalid(fmt::format(
            "deserialize bsi index fail, do not support version {}, please update plugin version",
            version));
    }
    // deserialize min & max
    PAIMON_ASSIGN_OR_RAISE(int64_t min, data_input_stream.ReadValue<int64_t>());
    PAIMON_ASSIGN_OR_RAISE(int64_t max, data_input_stream.ReadValue<int64_t>());

    // deserialize ebm
    RoaringBitmap32 ebm;
    PAIMON_RETURN_NOT_OK(ebm.Deserialize(input_stream.get()));

    // deserialize slices
    PAIMON_ASSIGN_OR_RAISE(int32_t slice_num, data_input_stream.ReadValue<int32_t>());
    std::vector<RoaringBitmap32> slices;
    slices.reserve(slice_num);
    for (int32_t i = 0; i < slice_num; i++) {
        RoaringBitmap32 slice;
        PAIMON_RETURN_NOT_OK(slice.Deserialize(input_stream.get()));
        slices.emplace_back(std::move(slice));
    }
    return std::shared_ptr<BitSliceIndexRoaringBitmap>(
        new BitSliceIndexRoaringBitmap(min, max, std::move(ebm), std::move(slices)));
}

bool BitSliceIndexRoaringBitmap::operator==(const BitSliceIndexRoaringBitmap& other) const {
    if (this == &other) {
        return true;
    }
    // TODO(xinyu.lxy): Java does not compare max
    return min_ == other.min_ && max_ == other.max_ && ebm_ == other.ebm_ &&
           slices_ == other.slices_;
}
Result<RoaringBitmap32> BitSliceIndexRoaringBitmap::Equal(int64_t literal) const {
    return Compare(Function::Type::EQUAL, literal);
}
Result<RoaringBitmap32> BitSliceIndexRoaringBitmap::LessThan(int64_t literal) const {
    return Compare(Function::Type::LESS_THAN, literal);
}
Result<RoaringBitmap32> BitSliceIndexRoaringBitmap::LessOrEqual(int64_t literal) const {
    return Compare(Function::Type::LESS_OR_EQUAL, literal);
}
Result<RoaringBitmap32> BitSliceIndexRoaringBitmap::GreaterThan(int64_t literal) const {
    return Compare(Function::Type::GREATER_THAN, literal);
}
Result<RoaringBitmap32> BitSliceIndexRoaringBitmap::GreaterOrEqual(int64_t literal) const {
    return Compare(Function::Type::GREATER_OR_EQUAL, literal);
}
const RoaringBitmap32& BitSliceIndexRoaringBitmap::IsNotNull() const {
    return ebm_;
}

Result<RoaringBitmap32> BitSliceIndexRoaringBitmap::Compare(const Function::Type& operation,
                                                            int64_t literal) const {
    // using min/max to fast skip
    PAIMON_ASSIGN_OR_RAISE(std::optional<RoaringBitmap32> min_max_compare_result,
                           CompareUsingMinMax(operation, literal));
    if (min_max_compare_result) {
        return min_max_compare_result.value();
    }
    return ONeilCompare(operation, literal - min_);
}

Result<std::optional<RoaringBitmap32>> BitSliceIndexRoaringBitmap::CompareUsingMinMax(
    const Function::Type& operation, int64_t literal) const {
    auto empty = []() -> std::optional<RoaringBitmap32> { return RoaringBitmap32(); };
    auto all = [&]() -> std::optional<RoaringBitmap32> { return IsNotNull(); };
    switch (operation) {
        case Function::Type::EQUAL: {
            if (min_ == max_ && min_ == literal) {
                return all();
            } else if (literal < min_ || literal > max_) {
                return empty();
            }
            break;
        }
        case Function::Type::NOT_EQUAL: {
            if (min_ == max_ && min_ == literal) {
                return empty();
            } else if (literal < min_ || literal > max_) {
                return all();
            }
            break;
        }
        case Function::Type::GREATER_OR_EQUAL: {
            if (literal <= min_) {
                return all();
            } else if (literal > max_) {
                return empty();
            }
            break;
        }
        case Function::Type::GREATER_THAN: {
            if (literal < min_) {
                return all();
            } else if (literal >= max_) {
                return empty();
            }
            break;
        }
        case Function::Type::LESS_OR_EQUAL: {
            if (literal >= max_) {
                return all();
            } else if (literal < min_) {
                return empty();
            }
            break;
        }
        case Function::Type::LESS_THAN: {
            if (literal > max_) {
                return all();
            } else if (literal <= min_) {
                return empty();
            }
            break;
        }
        default:
            return Status::Invalid(
                "Invalid Function::Type in CompareUsingMinMax of BitSliceIndex, only support "
                "EQUAL/NOT_EQUAL/GREATER_OR_EQUAL/GREATER_THAN/LESS_OR_EQUAL/LESS_THAN");
    }
    return std::optional<RoaringBitmap32>();
}

Result<RoaringBitmap32> BitSliceIndexRoaringBitmap::ONeilCompare(const Function::Type& operation,
                                                                 int64_t literal) const {
    RoaringBitmap32 greater_than;
    RoaringBitmap32 less_than;
    RoaringBitmap32 equal = ebm_;

    for (int32_t i = slices_.size() - 1; i >= 0; i--) {
        int64_t bit = (literal >> i) & 1;
        if (bit == 1) {
            less_than |= RoaringBitmap32::AndNot(equal, slices_[i]);
            equal &= slices_[i];
        } else {
            greater_than |= RoaringBitmap32::And(equal, slices_[i]);
            equal -= slices_[i];
        }
    }
    equal &= ebm_;

    switch (operation) {
        case Function::Type::EQUAL:
            return equal;
        case Function::Type::NOT_EQUAL:
            return RoaringBitmap32::AndNot(ebm_, equal);
        case Function::Type::GREATER_OR_EQUAL: {
            greater_than &= ebm_;
            greater_than |= equal;
            return greater_than;
        }
        case Function::Type::GREATER_THAN: {
            greater_than &= ebm_;
            return greater_than;
        }
        case Function::Type::LESS_OR_EQUAL: {
            less_than &= ebm_;
            less_than |= equal;
            return less_than;
        }
        case Function::Type::LESS_THAN: {
            less_than &= ebm_;
            return less_than;
        }
        default:
            return Status::Invalid(
                "Invalid Function::Type in ONeilCompare of BitSliceIndex, only support "
                "EQUAL/NOT_EQUAL/GREATER_OR_EQUAL/GREATER_THAN/LESS_OR_EQUAL/LESS_THAN");
    }
}

namespace {
int32_t NumberOfTrailingZeros(int64_t value) {
    if (value == 0) {
        return 64;
    }
    return __builtin_ctzll(value);
}
int32_t NumberOfLeadingZeros(int64_t value) {
    if (value == 0) {
        return 64;
    }
    return __builtin_clzll(value);
}
}  // namespace

Result<std::unique_ptr<BitSliceIndexRoaringBitmap::Appender>>
BitSliceIndexRoaringBitmap::Appender::Create(int64_t min, int64_t max) {
    if (min < 0) {
        return Status::Invalid("values should be non-negative in BitSliceIndexRoaringBitmap");
    }
    if (min > max) {
        return Status::Invalid("min should be less than max in BitSliceIndexRoaringBitmap");
    }
    std::vector<RoaringBitmap32> slices;
    slices.resize(64 - NumberOfLeadingZeros(max - min));
    auto bsi = std::shared_ptr<BitSliceIndexRoaringBitmap>(
        new BitSliceIndexRoaringBitmap(min, max, RoaringBitmap32(), std::move(slices)));
    return std::unique_ptr<BitSliceIndexRoaringBitmap::Appender>(
        new BitSliceIndexRoaringBitmap::Appender(std::move(bsi)));
}

BitSliceIndexRoaringBitmap::Appender::Appender(std::shared_ptr<BitSliceIndexRoaringBitmap>&& bitmap)
    : bsi_(std::move(bitmap)) {}

Status BitSliceIndexRoaringBitmap::Appender::Append(int32_t rid, int64_t value) {
    if (value > bsi_->max_) {
        return Status::Invalid(
            fmt::format("value {} is too large for append to BitSliceIndexRoaringBitmap", value));
    }
    if (bsi_->ebm_.Contains(rid)) {
        return Status::Invalid(
            fmt::format("rid {} is already exists for append to BitSliceIndexRoaringBitmap", rid));
    }

    // reduce the number of slices
    value = value - bsi_->min_;

    // only bit=1 need to set
    while (value != 0) {
        bsi_->slices_[NumberOfTrailingZeros(value)].Add(rid);
        value &= (value - 1);
    }
    bsi_->ebm_.Add(rid);
    return Status::OK();
}

bool BitSliceIndexRoaringBitmap::Appender::IsNotEmpty() const {
    return !bsi_->ebm_.IsEmpty();
}

std::shared_ptr<Bytes> BitSliceIndexRoaringBitmap::Appender::Serialize(
    const std::shared_ptr<MemoryPool>& pool) {
    MemorySegmentOutputStream output_stream(
        /*segment_size=*/MemorySegmentOutputStream::DEFAULT_SEGMENT_SIZE, pool);
    output_stream.WriteValue<int8_t>(BitSliceIndexRoaringBitmap::VERSION_1);
    output_stream.WriteValue<int64_t>(bsi_->min_);
    output_stream.WriteValue<int64_t>(bsi_->max_);
    std::shared_ptr<Bytes> ebm_bytes = bsi_->ebm_.Serialize(pool.get());
    output_stream.WriteBytes(ebm_bytes);
    output_stream.WriteValue<int32_t>(bsi_->slices_.size());
    for (auto& slice : bsi_->slices_) {
        std::shared_ptr<Bytes> slice_bytes = slice.Serialize(pool.get());
        output_stream.WriteBytes(slice_bytes);
    }
    return MemorySegmentUtils::CopyToBytes(output_stream.Segments(), 0, output_stream.CurrentSize(),
                                           pool.get());
}

}  // namespace paimon
