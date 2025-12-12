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
#include "paimon/utils/range.h"

#include <cassert>

#include "fmt/format.h"
namespace paimon {
Range::Range(int64_t _from, int64_t _to) : from(_from), to(_to) {
    assert(from <= to);
}

int64_t Range::Count() const {
    return to - from + 1;
}

std::optional<Range> Range::Intersection(const Range& left, const Range& right) {
    int64_t start = std::max(left.from, right.from);
    int64_t end = std::min(left.to, right.to);
    if (start > end) {
        return std::nullopt;
    }
    return Range(start, end);
}

bool Range::HasIntersection(const Range& left, const Range& right) {
    int64_t intersection_start = std::max(left.from, right.from);
    int64_t intersection_end = std::min(left.to, right.to);
    return intersection_start <= intersection_end;
}

bool Range::operator==(const Range& other) const {
    if (this == &other) {
        return true;
    }
    return from == other.from && to == other.to;
}

bool Range::operator<(const Range& other) const {
    if (from == other.from) {
        return to < other.to;
    }
    return from < other.from;
}

std::string Range::ToString() const {
    return fmt::format("[{}, {}]", from, to);
}

}  // namespace paimon
