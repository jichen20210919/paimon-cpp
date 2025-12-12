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
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

#include "paimon/io/byte_array_input_stream.h"
#include "paimon/memory/bytes.h"
#include "paimon/predicate/function.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/utils/roaring_bitmap32.h"

namespace paimon {
class ByteArrayInputStream;
class MemoryPool;

/// A bit slice index compressed bitmap.
class BitSliceIndexRoaringBitmap {
 public:
    static Result<std::shared_ptr<BitSliceIndexRoaringBitmap>> Create(
        const std::shared_ptr<ByteArrayInputStream>& input_stream);

    Result<RoaringBitmap32> Equal(int64_t literal) const;
    Result<RoaringBitmap32> LessThan(int64_t literal) const;
    Result<RoaringBitmap32> LessOrEqual(int64_t literal) const;
    Result<RoaringBitmap32> GreaterThan(int64_t literal) const;
    Result<RoaringBitmap32> GreaterOrEqual(int64_t literal) const;
    const RoaringBitmap32& IsNotNull() const;

    bool operator==(const BitSliceIndexRoaringBitmap& other) const;

    class Appender {
     public:
        static Result<std::unique_ptr<Appender>> Create(int64_t min, int64_t max);
        Status Append(int32_t rid, int64_t value);
        bool IsNotEmpty() const;
        // TODO(xinyu.lxy): may use data output stream
        std::shared_ptr<Bytes> Serialize(const std::shared_ptr<MemoryPool>& pool);
        std::shared_ptr<BitSliceIndexRoaringBitmap> Build() const {
            return bsi_;
        }

     private:
        explicit Appender(std::shared_ptr<BitSliceIndexRoaringBitmap>&& bsi);

     private:
        std::shared_ptr<BitSliceIndexRoaringBitmap> bsi_;
    };

 public:
    static const int8_t VERSION_1;
    static const std::shared_ptr<BitSliceIndexRoaringBitmap>& Empty();

 private:
    BitSliceIndexRoaringBitmap(int64_t min, int64_t max, RoaringBitmap32&& ebm,
                               std::vector<RoaringBitmap32>&& slices);

    Result<RoaringBitmap32> Compare(const Function::Type& operation, int64_t literal) const;

    Result<std::optional<RoaringBitmap32>> CompareUsingMinMax(const Function::Type& operation,
                                                              int64_t literal) const;

    /// O'Neil bit-sliced index compare algorithm.
    ///
    /// See <a href="https://dl.acm.org/doi/10.1145/253262.253268">Improved query performance with
    /// variant indexes</a>
    ///
    /// @param operation compare operation
    /// @param literal the value we found filter
    /// @return rid set we found in this bsi with giving conditions, using RoaringBitmap to express
    Result<RoaringBitmap32> ONeilCompare(const Function::Type& operation, int64_t literal) const;

 private:
    int64_t min_;
    int64_t max_;
    RoaringBitmap32 ebm_;
    std::vector<RoaringBitmap32> slices_;
};
}  // namespace paimon
