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

#include "gtest/gtest.h"

namespace paimon::test {
TEST(RangeTest, TestSimple) {
    Range range(/*from=*/0, /*to=*/5);
    ASSERT_EQ(range.Count(), 6);
    ASSERT_EQ(range.ToString(), "[0, 5]");
}

TEST(RangeTest, TestHasIntersection) {
    {
        Range r1(10, 20);
        Range r2(15, 25);
        ASSERT_TRUE(Range::HasIntersection(r1, r2));
        ASSERT_TRUE(Range::HasIntersection(r2, r1));
        ASSERT_TRUE(Range::HasIntersection(r1, r1));
        ASSERT_TRUE(Range::HasIntersection(r2, r2));
    }
    {
        Range r1(10, 20);
        Range r2(21, 30);
        ASSERT_FALSE(Range::HasIntersection(r1, r2));
        ASSERT_FALSE(Range::HasIntersection(r2, r1));
    }
    {
        Range r1(10, 20);
        Range r2(20, 30);
        ASSERT_TRUE(Range::HasIntersection(r1, r2));
        ASSERT_TRUE(Range::HasIntersection(r2, r1));
    }
    {
        Range r1(10, 20);
        Range r2(12, 18);
        ASSERT_TRUE(Range::HasIntersection(r1, r2));
        ASSERT_TRUE(Range::HasIntersection(r2, r1));
    }
}

TEST(RangeTest, TestIntersection) {
    {
        Range r1(10, 20);
        Range r2(15, 25);
        ASSERT_EQ(Range::Intersection(r1, r2), Range(15, 20));
        ASSERT_EQ(Range::Intersection(r2, r1), Range(15, 20));
        ASSERT_EQ(Range::Intersection(r1, r1), r1);
        ASSERT_EQ(Range::Intersection(r2, r2), r2);
    }
    {
        Range r1(10, 20);
        Range r2(21, 30);
        ASSERT_FALSE(Range::Intersection(r1, r2));
        ASSERT_FALSE(Range::Intersection(r2, r1));
    }
    {
        Range r1(10, 20);
        Range r2(20, 30);
        ASSERT_EQ(Range::Intersection(r1, r2), Range(20, 20));
        ASSERT_EQ(Range::Intersection(r2, r1), Range(20, 20));
    }
    {
        Range r1(10, 20);
        Range r2(12, 18);
        ASSERT_EQ(Range::Intersection(r1, r2), r2);
        ASSERT_EQ(Range::Intersection(r2, r1), r2);
    }
}

TEST(RangeTest, TestCompare) {
    Range r1(10, 20);
    Range r2(15, 25);
    Range r3(10, 30);
    ASSERT_EQ(r1, r1);
    ASSERT_TRUE(r1 < r2);
    ASSERT_TRUE(r1 < r3);
    ASSERT_TRUE(r3 < r2);
}
}  // namespace paimon::test
