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

#include "paimon/common/memory/memory_segment.h"

#include <algorithm>

namespace paimon {

int32_t MemorySegment::Compare(const MemorySegment& seg2, int32_t offset1, int32_t offset2,
                               int32_t len) const {
    while (len >= 8) {
        // TODO(yonghao.fyh): support big endian, decide SmallEndian or BigEndian
        // long l1 = GetLongBigEndian(offset1);
        // long l2 = seg2.getLongBigEndian(offset2);
        uint64_t l1 = GetValue<int64_t>(offset1);
        uint64_t l2 = seg2.GetValue<int64_t>(offset2);

        if (l1 != l2) {
            return (l1 < l2) ? -1 : 1;
        }

        offset1 += 8;
        offset2 += 8;
        len -= 8;
    }
    while (len > 0) {
        int32_t b1 = Get(offset1) & 0xff;
        int32_t b2 = seg2.Get(offset2) & 0xff;
        int32_t cmp = b1 - b2;
        if (cmp != 0) {
            return cmp;
        }
        offset1++;
        offset2++;
        len--;
    }
    return 0;
}

int32_t MemorySegment::Compare(const MemorySegment& seg2, int32_t offset1, int32_t offset2,
                               int32_t len1, int32_t len2) const {
    const int32_t min_length = std::min(len1, len2);
    int32_t c = Compare(seg2, offset1, offset2, min_length);
    return c == 0 ? (len1 - len2) : c;
}

void MemorySegment::SwapBytes(Bytes* temp_buffer, MemorySegment* seg2, int32_t offset1,
                              int32_t offset2, int32_t len) {
    if ((offset1 | offset2 | len | (temp_buffer->size() - len) |
         (heap_memory_->size() - (offset1 + len)) |
         (seg2->heap_memory_->size() - (offset2 + len))) >= 0) {
        // this -> temp buffer
        std::memcpy(temp_buffer->data(), heap_memory_->data() + offset1, len);

        // other -> this
        std::memcpy(heap_memory_->data() + offset1, seg2->heap_memory_->data() + offset2, len);

        // temp buffer -> other
        std::memcpy(seg2->heap_memory_->data() + offset2, temp_buffer->data(), len);
        return;
    }
    assert(false);

    // index is in fact invalid
    // return Status::InternalError(
    //     "IndexOutOfBoundsException",
    //     fmt::format("offset1={}, offset2={}, len={}, temp buffer size={}, heap
    //     mem 1 "
    //                 "size={}, heap mem 2 size={}",
    //                 offset1, offset2, len, temp_buffer->size(),
    //                 heap_memory_->size(), seg2->heap_memory_->size()));
}

bool MemorySegment::EqualTo(const MemorySegment& seg2, int32_t offset1, int32_t offset2,
                            int32_t length) const {
    int32_t i = 0;
    // we assume unaligned accesses are supported.
    // Compare 8 bytes at a time.
    while (i <= length - 8) {
        if (GetValue<int64_t>(offset1 + i) != seg2.GetValue<int64_t>(offset2 + i)) {
            return false;
        }
        i += 8;
    }

    // cover the last (length % 8) elements.
    while (i < length) {
        if (Get(offset1 + i) != seg2.Get(offset2 + i)) {
            return false;
        }
        i += 1;
    }

    return true;
}

}  // namespace paimon
