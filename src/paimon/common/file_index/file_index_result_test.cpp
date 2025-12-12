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

#include "paimon/file_index/file_index_result.h"

#include <utility>

#include "gtest/gtest.h"

namespace paimon::test {
TEST(FileIndexResultTest, TestSimple) {
    auto remain = FileIndexResult::Remain();
    auto skip = FileIndexResult::Skip();

    ASSERT_TRUE(remain->And(remain).value()->IsRemain().value());
    ASSERT_FALSE(remain->And(skip).value()->IsRemain().value());
    ASSERT_FALSE(skip->And(remain).value()->IsRemain().value());
    ASSERT_FALSE(skip->And(skip).value()->IsRemain().value());

    ASSERT_TRUE(remain->Or(remain).value()->IsRemain().value());
    ASSERT_TRUE(remain->Or(skip).value()->IsRemain().value());
    ASSERT_TRUE(skip->Or(remain).value()->IsRemain().value());
    ASSERT_FALSE(skip->Or(skip).value()->IsRemain().value());

    ASSERT_EQ(remain->ToString(), "REMAIN");
    ASSERT_EQ(skip->ToString(), "SKIP");
}
}  // namespace paimon::test
