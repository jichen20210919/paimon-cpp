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

#include "paimon/common/utils/delta_varint_compressor.h"

#include <cstdint>
#include <utility>
#include <vector>

#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

std::vector<char> DeltaVarintCompressor::Compress(const std::vector<int64_t>& data) {
    if (data.empty()) {
        return {};
    }

    // 1. Delta encoding
    std::vector<int64_t> deltas;
    deltas.reserve(data.size());
    deltas.push_back(data[0]);
    for (size_t i = 1; i < data.size(); i++) {
        deltas.push_back(data[i] - data[i - 1]);
    }

    // 2. ZigZag + Varint
    std::vector<char> out;
    out.reserve(data.size() * 10);
    for (const auto& delta : deltas) {
        EncodeVarint(delta, &out);
    }
    return out;
}

Result<std::vector<int64_t>> DeltaVarintCompressor::Decompress(const std::vector<char>& bytes) {
    if (bytes.empty()) {
        return std::vector<int64_t>();
    }

    // 1. Decode ZigZag + Varint → delta
    std::vector<int64_t> deltas;
    deltas.reserve(bytes.size());
    size_t pos = 0;
    while (pos < bytes.size()) {
        PAIMON_ASSIGN_OR_RAISE(int64_t delta, DecodeVarint(bytes, &pos));
        deltas.push_back(delta);
    }

    // 2. Delta decoding
    std::vector<int64_t> result(deltas.size());
    result[0] = deltas[0];
    for (size_t i = 1; i < result.size(); i++) {
        result[i] = result[i - 1] + deltas[i];
    }
    return result;
}

void DeltaVarintCompressor::EncodeVarint(int64_t value, std::vector<char>* out) {
    uint64_t tmp = ZigZag(value);
    // Check if multiple bytes are needed
    while (tmp & ~0x7F) {
        // Set MSB to 1 (continuation)
        out->push_back(static_cast<char>((tmp & 0x7F) | 0x80));
        // Unsigned right shift
        tmp >>= 7;
    }
    // Final byte with MSB set to 0
    out->push_back(static_cast<char>(tmp));
}

Result<int64_t> DeltaVarintCompressor::DecodeVarint(const std::vector<char>& in, size_t* pos) {
    uint64_t result = 0;
    int32_t shift = 0;
    while (true) {
        if (*pos >= in.size()) {
            return Status::Invalid("Unexpected end of input");
        }
        char byte = in[(*pos)++];
        // Extract 7 bits
        result |= (static_cast<uint64_t>(byte & 0x7F) << shift);
        // MSB is 0, end of encoding
        if ((byte & 0x80) == 0) {
            break;
        }
        shift += 7;
        if (shift > 63) {
            return Status::Invalid("Varint overflow");
        }
    }
    return UnZigZag(result);
}

}  // namespace paimon
