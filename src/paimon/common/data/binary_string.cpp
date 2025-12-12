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

#include "paimon/common/data/binary_string.h"

#include <algorithm>
#include <cctype>

#include "paimon/common/memory/memory_segment_utils.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"

namespace paimon {

const BinaryString& BinaryString::EmptyUtf8() {
    static const BinaryString empty_utf8 = BinaryString();
    return empty_utf8;
}

BinaryString::BinaryString(const std::vector<MemorySegment>& segments, int32_t offset,
                           int32_t size_in_bytes)
    : BinarySection(segments, offset, size_in_bytes) {}

BinaryString::BinaryString() {
    std::shared_ptr<Bytes> bytes = Bytes::AllocateBytes(0, GetDefaultPool().get());
    segments_ = {MemorySegment::Wrap(bytes)};
    offset_ = 0;
    size_in_bytes_ = bytes->size();
}

BinaryString BinaryString::FromAddress(const std::vector<MemorySegment>& segments, int32_t offset,
                                       int32_t num_bytes) {
    return BinaryString(segments, offset, num_bytes);
}

BinaryString BinaryString::FromString(const std::string& str, MemoryPool* pool) {
    std::shared_ptr<Bytes> bytes = Bytes::AllocateBytes(str, pool);
    return FromBytes(bytes);
}

BinaryString BinaryString::FromBytes(const std::shared_ptr<Bytes>& bytes) {
    return FromBytes(bytes, 0, bytes->size());
}

BinaryString BinaryString::FromBytes(const std::shared_ptr<Bytes>& bytes, int32_t offset,
                                     int32_t num_bytes) {
    std::vector<MemorySegment> segs = {MemorySegment::Wrap(bytes)};
    return BinaryString(segs, offset, num_bytes);
}

BinaryString BinaryString::BlankString(int32_t length, MemoryPool* pool) {
    std::shared_ptr<Bytes> spaces = Bytes::AllocateBytes(length, pool);
    std::fill(spaces->data(), spaces->data() + length, ' ');
    return FromBytes(spaces);
}

std::string BinaryString::ToString() const {
    std::string ret(size_in_bytes_, '\0');
    MemorySegmentUtils::CopyToBytes(segments_, offset_, &ret, 0, size_in_bytes_);
    return ret;
}

int32_t BinaryString::NumBytesForFirstByte(char b) {
    if (b >= 0) {
        // 1 byte, 7 bits: 0xxxxxxx
        return 1;
    } else if ((b >> 5) == -2 && (b & 0x1e) != 0) {
        // 2 bytes, 11 bits: 110xxxxx 10xxxxxx
        return 2;
    } else if ((b >> 4) == -2) {
        // 3 bytes, 16 bits: 1110xxxx 10xxxxxx 10xxxxxx
        return 3;
    } else if ((b >> 3) == -2) {
        // 4 bytes, 21 bits: 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
        return 4;
    } else {
        // Skip the first byte disallowed in UTF-8
        // Handling errors quietly, same semantics to java String.
        return 1;
    }
}

int32_t BinaryString::CompareTo(const BinaryString& other) const {
    if (segments_.size() == 1 && other.segments_.size() == 1) {
        int32_t len = std::min(size_in_bytes_, other.size_in_bytes_);
        const auto& seg1 = segments_[0];
        const auto& seg2 = other.segments_[0];
        for (int32_t i = 0; i < len; i++) {
            int32_t res = (seg1.Get(offset_ + i) & 0xFF) - (seg2.Get(other.offset_ + i) & 0xFF);
            if (res != 0) {
                return res;
            }
        }
        return size_in_bytes_ - other.size_in_bytes_;
    }
    return CompareMultiSegments(other);
}

int32_t BinaryString::CompareMultiSegments(const BinaryString& other) const {
    if (size_in_bytes_ == 0 || other.size_in_bytes_ == 0) {
        return size_in_bytes_ - other.size_in_bytes_;
    }

    int32_t len = std::min(size_in_bytes_, other.size_in_bytes_);

    MemorySegment seg1 = segments_[0];
    MemorySegment seg2 = other.segments_[0];

    int32_t segment_size = segments_[0].Size();
    int32_t other_segment_size = other.segments_[0].Size();

    int32_t size_of_first1 = segment_size - offset_;
    int32_t size_of_first2 = other_segment_size - other.offset_;

    int32_t var_seg_index1 = 1;
    int32_t var_seg_index2 = 1;

    // find the first segment of this string.
    while (size_of_first1 <= 0) {
        size_of_first1 += segment_size;
        seg1 = segments_[var_seg_index1++];
    }

    while (size_of_first2 <= 0) {
        size_of_first2 += other_segment_size;
        seg2 = other.segments_[var_seg_index2++];
    }

    int32_t offset1 = segment_size - size_of_first1;
    int32_t offset2 = other_segment_size - size_of_first2;

    int32_t need_compare = std::min(std::min(size_of_first1, size_of_first2), len);
    while (need_compare > 0) {
        // compare in one segment.
        for (int32_t i = 0; i < need_compare; i++) {
            int32_t res = (seg1.Get(offset1 + i) & 0xFF) - (seg2.Get(offset2 + i) & 0xFF);
            if (res != 0) {
                return res;
            }
        }
        if (need_compare == len) {
            break;
        }
        len -= need_compare;
        // next segment
        if (size_of_first1 < size_of_first2) {  // I am smaller
            seg1 = segments_[var_seg_index1++];
            offset1 = 0;
            offset2 += need_compare;
            size_of_first1 = segment_size;
            size_of_first2 -= need_compare;
        } else if (size_of_first1 > size_of_first2) {  // other is smaller
            seg2 = other.segments_[var_seg_index2++];
            offset2 = 0;
            offset1 += need_compare;
            size_of_first2 = other_segment_size;
            size_of_first1 -= need_compare;
        } else {  // same, should go ahead both.
            seg1 = segments_[var_seg_index1++];
            seg2 = other.segments_[var_seg_index2++];
            offset1 = 0;
            offset2 = 0;
            size_of_first1 = segment_size;
            size_of_first2 = other_segment_size;
        }
        need_compare = std::min(std::min(size_of_first1, size_of_first2), len);
    }

    assert(need_compare == len);
    return size_in_bytes_ - other.size_in_bytes_;
}

int32_t BinaryString::NumChars() const {
    if (InFirstSegment()) {
        int32_t len = 0;
        for (int32_t i = 0; i < size_in_bytes_; i += NumBytesForFirstByte(GetByteOneSegment(i))) {
            len++;
        }
        return len;
    } else {
        return NumCharsMultiSegs();
    }
}

int32_t BinaryString::NumCharsMultiSegs() const {
    int32_t len = 0;
    int32_t seg_size = segments_[0].Size();
    BinaryString::SegmentAndOffset index = FirstSegmentAndOffset(seg_size);
    int32_t i = 0;
    while (i < size_in_bytes_) {
        int32_t char_bytes = NumBytesForFirstByte(index.Value());
        i += char_bytes;
        len++;
        index.SkipBytes(char_bytes, seg_size);
    }
    return len;
}

char BinaryString::ByteAt(int32_t index) const {
    int32_t global_offset = offset_ + index;
    int32_t size = segments_[0].Size();
    if (global_offset < size) {
        return segments_[0].Get(global_offset);
    } else {
        return segments_[global_offset / size].Get(global_offset % size);
    }
}

BinaryString BinaryString::Copy(MemoryPool* pool) const {
    std::shared_ptr<Bytes> copy =
        MemorySegmentUtils::CopyToBytes(segments_, offset_, size_in_bytes_, pool);
    return FromBytes(copy);
}

BinaryString BinaryString::SubStringMultiSegs(const int32_t start, const int32_t until,
                                              MemoryPool* pool) const {
    int32_t seg_size = segments_[0].Size();
    SegmentAndOffset index = FirstSegmentAndOffset(seg_size);
    int32_t i = 0;
    int32_t c = 0;
    while (i < size_in_bytes_ && c < start) {
        int32_t char_size = NumBytesForFirstByte(index.Value());
        i += char_size;
        index.SkipBytes(char_size, seg_size);
        c += 1;
    }

    int32_t j = i;
    while (i < size_in_bytes_ && c < until) {
        int32_t char_size = NumBytesForFirstByte(index.Value());
        i += char_size;
        index.SkipBytes(char_size, seg_size);
        c += 1;
    }

    if (i > j) {
        std::shared_ptr<Bytes> bytes =
            MemorySegmentUtils::CopyToBytes(segments_, offset_ + j, i - j, pool);
        return FromBytes(bytes);
    } else {
        return EmptyUtf8();
    }
}

BinaryString BinaryString::Substring(int32_t begin_index, int32_t end_index,
                                     MemoryPool* pool) const {
    if (end_index <= begin_index || begin_index >= size_in_bytes_) {
        return EmptyUtf8();
    }
    if (InFirstSegment()) {
        MemorySegment segment = segments_[0];
        int32_t i = 0;
        int32_t c = 0;
        while (i < size_in_bytes_ && c < begin_index) {
            i += NumBytesForFirstByte(segment.Get(i + offset_));
            c += 1;
        }

        int32_t j = i;
        while (i < size_in_bytes_ && c < end_index) {
            i += NumBytesForFirstByte(segment.Get(i + offset_));
            c += 1;
        }

        if (i > j) {
            std::shared_ptr<Bytes> bytes = Bytes::AllocateBytes(i - j, pool);
            segment.Get(offset_ + j, bytes.get(), 0, i - j);
            return FromBytes(bytes);
        } else {
            return EmptyUtf8();
        }
    } else {
        return SubStringMultiSegs(begin_index, end_index, pool);
    }
}

bool BinaryString::Contains(const BinaryString& s) const {
    if (s.size_in_bytes_ == 0) {
        return true;
    }
    int32_t find = MemorySegmentUtils::Find(segments_, offset_, size_in_bytes_, s.segments_,
                                            s.offset_, s.size_in_bytes_);
    return find != -1;
}

bool BinaryString::StartsWith(const BinaryString& prefix) const {
    return MatchAt(prefix, 0);
}

bool BinaryString::EndsWith(const BinaryString& suffix) const {
    return MatchAt(suffix, size_in_bytes_ - suffix.size_in_bytes_);
}

int32_t BinaryString::IndexOf(const BinaryString& str, int32_t from_index) const {
    if (str.size_in_bytes_ == 0) {
        return 0;
    }
    if (InFirstSegment()) {
        // position in byte
        int32_t byte_idx = 0;
        // position is char
        int32_t char_idx = 0;
        while (byte_idx < size_in_bytes_ && char_idx < from_index) {
            byte_idx += NumBytesForFirstByte(GetByteOneSegment(byte_idx));
            char_idx++;
        }
        do {
            if (byte_idx + str.size_in_bytes_ > size_in_bytes_) {
                return -1;
            }
            if (MemorySegmentUtils::Equals(segments_, offset_ + byte_idx, str.segments_,
                                           str.offset_, str.size_in_bytes_)) {
                return char_idx;
            }
            byte_idx += NumBytesForFirstByte(GetByteOneSegment(byte_idx));
            char_idx++;
        } while (byte_idx < size_in_bytes_);

        return -1;
    } else {
        return IndexOfMultiSegs(str, from_index);
    }
}

int32_t BinaryString::IndexOfMultiSegs(const BinaryString& str, int32_t from_index) const {
    // position in byte
    int32_t byte_idx = 0;
    // position is char
    int32_t char_idx = 0;
    int32_t seg_size = segments_[0].Size();
    SegmentAndOffset index = FirstSegmentAndOffset(seg_size);
    while (byte_idx < size_in_bytes_ && char_idx < from_index) {
        int32_t char_bytes = NumBytesForFirstByte(index.Value());
        byte_idx += char_bytes;
        char_idx++;
        index.SkipBytes(char_bytes, seg_size);
    }
    do {
        if (byte_idx + str.size_in_bytes_ > size_in_bytes_) {
            return -1;
        }
        if (MemorySegmentUtils::Equals(segments_, offset_ + byte_idx, str.segments_, str.offset_,
                                       str.size_in_bytes_)) {
            return char_idx;
        }
        int32_t char_bytes = NumBytesForFirstByte(index.segment_->Get(index.offset_));
        byte_idx += char_bytes;
        char_idx++;
        index.SkipBytes(char_bytes, seg_size);
    } while (byte_idx < size_in_bytes_);
    return -1;
}

BinaryString BinaryString::ToUpperCase(MemoryPool* pool) const {
    if (size_in_bytes_ == 0) {
        return EmptyUtf8();
    }
    int32_t size = segments_[0].Size();
    SegmentAndOffset segment_and_offset = StartSegmentAndOffset(size);
    std::shared_ptr<Bytes> bytes = Bytes::AllocateBytes(size_in_bytes_, pool);
    (*bytes)[0] = tolower(segment_and_offset.Value());
    for (int32_t i = 0; i < size_in_bytes_; i++) {
        char b = segment_and_offset.Value();
        if (NumBytesForFirstByte(b) != 1) {
            // fallback
            return CppToUpperCase(pool);
        }
        int32_t upper = toupper(static_cast<int32_t>(b));
        if (upper > 127) {
            // fallback
            return CppToUpperCase(pool);
        }
        (*bytes)[i] = static_cast<char>(upper);
        segment_and_offset.NextByte(size);
    }
    return FromBytes(bytes);
}

BinaryString BinaryString::CppToUpperCase(MemoryPool* pool) const {
    std::string str = ToString();
    std::transform(str.begin(), str.end(), str.begin(), ::toupper);
    return FromString(str, pool);
}

BinaryString BinaryString::ToLowerCase(MemoryPool* pool) const {
    if (size_in_bytes_ == 0) {
        return EmptyUtf8();
    }
    int32_t size = segments_[0].Size();
    SegmentAndOffset segment_and_offset = StartSegmentAndOffset(size);
    std::shared_ptr<Bytes> bytes = Bytes::AllocateBytes(size_in_bytes_, pool);
    (*bytes)[0] = tolower(segment_and_offset.Value());
    for (int32_t i = 0; i < size_in_bytes_; i++) {
        char b = segment_and_offset.Value();
        if (NumBytesForFirstByte(b) != 1) {
            // fallback
            return CppToLowerCase(pool);
        }
        int32_t lower = tolower(static_cast<int32_t>(b));
        if (lower > 127) {
            // fallback
            return CppToLowerCase(pool);
        }
        (*bytes)[i] = static_cast<char>(lower);
        segment_and_offset.NextByte(size);
    }
    return FromBytes(bytes);
}

BinaryString BinaryString::CppToLowerCase(MemoryPool* pool) const {
    std::string str = ToString();
    std::transform(str.begin(), str.end(), str.begin(), ::tolower);
    return FromString(str, pool);
}

char BinaryString::GetByteOneSegment(int32_t i) const {
    return segments_[0].Get(offset_ + i);
}

bool BinaryString::InFirstSegment() const {
    return size_in_bytes_ + offset_ <= segments_[0].Size();
}

BinaryString::SegmentAndOffset BinaryString::FirstSegmentAndOffset(int32_t seg_size) const {
    int32_t seg_index = offset_ / seg_size;
    return BinaryString::SegmentAndOffset(segments_, seg_index, offset_ % seg_size);
}

BinaryString::SegmentAndOffset BinaryString::StartSegmentAndOffset(int32_t seg_size) const {
    return InFirstSegment() ? SegmentAndOffset(segments_, 0, offset_)
                            : FirstSegmentAndOffset(seg_size);
}

BinaryString BinaryString::CopyBinaryString(int32_t start, int32_t end, MemoryPool* pool) const {
    int32_t len = end - start + 1;
    std::shared_ptr<Bytes> new_bytes = Bytes::AllocateBytes(len, pool);
    MemorySegmentUtils::CopyToBytes(segments_, offset_ + start, new_bytes.get(), 0, len);
    return FromBytes(new_bytes);
}

BinaryString BinaryString::CopyBinaryStringInOneSeg(int32_t start, int32_t len,
                                                    MemoryPool* pool) const {
    std::shared_ptr<Bytes> new_bytes = Bytes::AllocateBytes(len, pool);
    segments_[0].Get(offset_ + start, new_bytes.get(), 0, len);
    return FromBytes(new_bytes);
}

bool BinaryString::MatchAt(const BinaryString& s, int32_t pos) const {
    return (InFirstSegment() && s.InFirstSegment()) ? MatchAtOneSeg(s, pos) : MatchAtVarSeg(s, pos);
}

bool BinaryString::MatchAtOneSeg(const BinaryString& s, int32_t pos) const {
    return s.size_in_bytes_ + pos <= size_in_bytes_ && pos >= 0 &&
           segments_[0].EqualTo(s.segments_[0], offset_ + pos, s.offset_, s.size_in_bytes_);
}

bool BinaryString::MatchAtVarSeg(const BinaryString& s, int32_t pos) const {
    return s.size_in_bytes_ + pos <= size_in_bytes_ && pos >= 0 &&
           MemorySegmentUtils::Equals(segments_, offset_ + pos, s.segments_, s.offset_,
                                      s.size_in_bytes_);
}

}  // namespace paimon
