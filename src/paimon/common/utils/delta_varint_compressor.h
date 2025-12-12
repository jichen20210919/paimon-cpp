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

#include <cstddef>
#include <cstdint>
#include <vector>

#include "paimon/result.h"
#include "paimon/visibility.h"

namespace paimon {

/// Combining Delta Encoding and Varints Encoding, suitable for integer sequences that are
/// increasing or not significantly different.
class PAIMON_EXPORT DeltaVarintCompressor {
 public:
    // Compresses an int64_t array using delta encoding, ZigZag transformation, and Varints encoding
    static std::vector<char> Compress(const std::vector<int64_t>& data);
    // Decompresses a byte array back to the original long array
    static Result<std::vector<int64_t>> Decompress(const std::vector<char>& bytes);

 private:
    // Encodes a long value using ZigZag and Varints
    static void EncodeVarint(int64_t value, std::vector<char>* out);
    // Decodes a Varints-encoded value and reverses ZigZag transformation
    static Result<int64_t> DecodeVarint(const std::vector<char>& in, size_t* pos);

    inline static uint64_t ZigZag(int64_t value) {
        return ((static_cast<uint64_t>(value) << 1) ^ (value >> 63));
    }
    inline static int64_t UnZigZag(uint64_t value) {
        return (value >> 1) ^ -(value & 1);
    }
};

}  // namespace paimon
