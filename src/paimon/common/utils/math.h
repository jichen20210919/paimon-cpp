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

//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// Adapted from RocksDB
// https://github.com/facebook/rocksdb/blob/main/util/math.h

#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <type_traits>

namespace paimon {
// Swaps between big and little endian. Can be used in combination with the
// little-endian encoding/decoding functions in coding_lean.h and coding.h to
// encode/decode big endian.
template <typename T>
inline T EndianSwapValue(T v) {
    static_assert(std::is_integral_v<T>, "non-integral type");

#ifdef _MSC_VER
    if (sizeof(T) == 2) {
        return static_cast<T>(_byteswap_ushort(static_cast<uint16_t>(v)));
    } else if (sizeof(T) == 4) {
        return static_cast<T>(_byteswap_ulong(static_cast<uint32_t>(v)));
    } else if (sizeof(T) == 8) {
        return static_cast<T>(_byteswap_uint64(static_cast<uint64_t>(v)));
    }
#else
    if (sizeof(T) == 2) {
        return static_cast<T>(__builtin_bswap16(static_cast<uint16_t>(v)));
    } else if (sizeof(T) == 4) {
        return static_cast<T>(__builtin_bswap32(static_cast<uint32_t>(v)));
    } else if (sizeof(T) == 8) {
        return static_cast<T>(__builtin_bswap64(static_cast<uint64_t>(v)));
    }
#endif
    // Recognized by clang as bswap, but not by gcc :(
    T ret_val = 0;
    for (std::size_t i = 0; i < sizeof(T); ++i) {
        ret_val |= ((v >> (8 * i)) & 0xff) << (8 * (sizeof(T) - 1 - i));
    }
    return ret_val;
}

}  // namespace paimon
