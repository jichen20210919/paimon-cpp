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

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "paimon/common/data/binary_section.h"
#include "paimon/common/memory/memory_segment.h"
#include "paimon/visibility.h"

namespace paimon {
class Bytes;
class MemoryPool;

/// A string which is backed by `MemorySegment`s.
class PAIMON_EXPORT BinaryString : public BinarySection {
 public:
    static const BinaryString& EmptyUtf8();

    BinaryString(const std::vector<MemorySegment>& segments, int32_t offset, int32_t size_in_bytes);

    static BinaryString FromAddress(const std::vector<MemorySegment>& segments, int32_t offset,
                                    int32_t num_bytes);
    static BinaryString FromString(const std::string& str, MemoryPool* pool);

    /// Creates a `BinaryString` instance from the given UTF-8 bytes.
    static BinaryString FromBytes(const std::shared_ptr<Bytes>& bytes);
    /// Creates a `BinaryString` instance from the given UTF-8 bytes with offset and number of
    /// bytes.
    static BinaryString FromBytes(const std::shared_ptr<Bytes>& bytes, int32_t offset,
                                  int32_t num_bytes);
    /// Creates a `BinaryString` instance that contains length spaces.
    static BinaryString BlankString(int32_t length, MemoryPool* pool);

    std::string ToString() const;

    bool operator==(const BinaryString& other) const {
        if (this == &other) {
            return true;
        }
        return CompareTo(other) == 0;
    }
    bool operator<(const BinaryString& other) const {
        return CompareTo(other) < 0;
    }

    int32_t CompareTo(const BinaryString& other) const;

    /// Find the boundaries of segments, and then compare MemorySegment.
    int32_t CompareMultiSegments(const BinaryString& other) const;

    /// @return the number of UTF-8 code points in the string.
    int32_t NumChars() const;
    int32_t NumCharsMultiSegs() const;

    /// Returns the byte value at the specified index. An index ranges from `0` to
    /// `size_in_bytes_ - 1`.
    /// @param index the index of the byte value.
    /// @return the byte value at the specified index of this UTF-8 bytes.
    char ByteAt(int32_t index) const;

    /// Copy a new BinaryString.
    BinaryString Copy(MemoryPool* pool) const;

    /// Returns a binary string that is a substring of this binary string. The substring
    /// begins at the specified `begin_index` and extends to the character at index
    /// `end_index - 1`.
    /// Examples:
    /// FromString("hamburger").Substring(4, 8) returns binary string "urge"
    /// FromString("smiles").Substring(1, 5) returns binary string "mile"
    ///
    /// @param begin_index the beginning index, inclusive.
    /// @param end_index the ending index, exclusive.
    /// @return the specified substring, return `EmptyUtf8()` when index out of bounds
    BinaryString Substring(int32_t begin_index, int32_t end_index, MemoryPool* pool) const;

    bool Contains(const BinaryString& s) const;

    /// Tests if this BinaryString starts with the specified prefix.
    /// @param prefix the prefix.
    /// @return `true` if the bytes represented by the argument is a prefix of the
    /// bytes represented by this string; `false` otherwise. Note also that `true` will be returned
    /// if the argument is an empty BinaryString or is equal to this BinaryString object as
    /// determined by the `operator==` method.
    bool StartsWith(const BinaryString& prefix) const;

    /// Tests if this BinaryString ends with the specified suffix.
    /// @param suffix the suffix.
    /// @return `true` if the bytes represented by the argument is a suffix of the
    /// bytes represented by this object; `false` otherwise. Note that the result
    /// will be `true` if the argument is the empty string or is equal to this BinaryString object
    /// as determined by the `operator==` method.
    bool EndsWith(const BinaryString& suffix) const;

    /// Returns the index within this string of the first occurrence of the specified
    /// substring, starting at the specified index.
    /// @param str the substring to search for.
    /// @param from_index the index from which to start the search.
    /// @return the utf8 index of the first occurrence of the specified substring, starting
    /// at the specified index, or `-1` if there is no such occurrence.
    int32_t IndexOf(const BinaryString& str, int32_t from_index) const;

    /// Converts all of the characters in this BinaryString to upper case.
    /// @return the BinaryString, converted to uppercase.
    BinaryString ToUpperCase(MemoryPool* pool) const;

    /// Converts all of the characters in this BinaryString to lower case.
    /// @return the BinaryString, converted to lowercase.
    BinaryString ToLowerCase(MemoryPool* pool) const;

    /// @return the number of bytes for a code point with the first byte as b.
    /// @param b The first byte of a code point
    static int32_t NumBytesForFirstByte(char b);

    char GetByteOneSegment(int32_t i) const;
    bool InFirstSegment() const;
    bool MatchAt(const BinaryString& s, int32_t pos) const;
    bool MatchAtOneSeg(const BinaryString& s, int32_t pos) const;
    bool MatchAtVarSeg(const BinaryString& s, int32_t pos) const;
    BinaryString CopyBinaryStringInOneSeg(int32_t start, int32_t len, MemoryPool* pool) const;
    BinaryString CopyBinaryString(int32_t start, int32_t end, MemoryPool* pool) const;

    /// CurrentSegment and positionInSegment.
    class SegmentAndOffset {
        friend class BinaryString;

     public:
        SegmentAndOffset(const std::vector<MemorySegment>& segments, int32_t seg_index,
                         int32_t offset)
            : seg_index_(seg_index),
              offset_(offset),
              segments_(segments),
              segment_(segments[seg_index]) {}

        void NextByte(int32_t seg_size) {
            offset_++;
            CheckAdvance(seg_size);
        }

        void SkipBytes(int32_t n, int32_t seg_size) {
            int32_t remaining = seg_size - offset_;
            if (remaining > n) {
                offset_ += n;
            } else {
                while (true) {
                    int32_t to_skip = std::min(remaining, n);
                    n -= to_skip;
                    if (n <= 0) {
                        offset_ += to_skip;
                        CheckAdvance(seg_size);
                        return;
                    }
                    Advance();
                    remaining = seg_size - offset_;
                }
            }
        }
        char Value() const {
            assert(segment_ != std::nullopt);
            return segment_.value().Get(offset_);
        }

     private:
        int32_t seg_index_;
        int32_t offset_;
        std::vector<MemorySegment> segments_;
        std::optional<MemorySegment> segment_;

        void AssignSegment() {
            segment_ = seg_index_ >= 0 && seg_index_ < static_cast<int32_t>(segments_.size())
                           ? std::optional<MemorySegment>(segments_[seg_index_])
                           : std::nullopt;
        }

        void CheckAdvance(int32_t seg_size) {
            if (offset_ == seg_size) {
                Advance();
            }
        }

        void Advance() {
            seg_index_++;
            AssignSegment();
            offset_ = 0;
        }
    };
    SegmentAndOffset FirstSegmentAndOffset(int32_t seg_size) const;
    SegmentAndOffset StartSegmentAndOffset(int32_t seg_size) const;

 private:
    BinaryString();
    BinaryString SubStringMultiSegs(const int32_t start, const int32_t until,
                                    MemoryPool* pool) const;
    int32_t IndexOfMultiSegs(const BinaryString& str, int32_t fromIndex) const;
    BinaryString CppToLowerCase(MemoryPool* pool) const;
    BinaryString CppToUpperCase(MemoryPool* pool) const;
};

}  // namespace paimon
