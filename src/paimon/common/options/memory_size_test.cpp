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

#include "paimon/common/options/memory_size.h"

#include <limits>

#include "gtest/gtest.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
TEST(MemorySizeTest, TestParseBytes) {
    ASSERT_OK_AND_ASSIGN(int64_t size, MemorySize::ParseBytes("1024"));
    ASSERT_EQ(size, 1024L);

    ASSERT_OK_AND_ASSIGN(size, MemorySize::ParseBytes("1024b"));
    ASSERT_EQ(size, 1024);

    ASSERT_OK_AND_ASSIGN(size, MemorySize::ParseBytes("1024bytes"));
    ASSERT_EQ(size, 1024L);

    ASSERT_OK_AND_ASSIGN(size, MemorySize::ParseBytes("1024k"));
    ASSERT_EQ(size, 1048576L);

    ASSERT_OK_AND_ASSIGN(size, MemorySize::ParseBytes("1024kb"));
    ASSERT_EQ(size, 1048576L);

    ASSERT_OK_AND_ASSIGN(size, MemorySize::ParseBytes("1024kibibytes"));
    ASSERT_EQ(size, 1048576L);

    ASSERT_OK_AND_ASSIGN(size, MemorySize::ParseBytes("1024m"));
    ASSERT_EQ(size, 1073741824L);

    ASSERT_OK_AND_ASSIGN(size, MemorySize::ParseBytes("1024mb"));
    ASSERT_EQ(size, 1073741824L);

    ASSERT_OK_AND_ASSIGN(size, MemorySize::ParseBytes("1024mebibytes"));
    ASSERT_EQ(size, 1073741824L);

    ASSERT_OK_AND_ASSIGN(size, MemorySize::ParseBytes("1024g"));
    ASSERT_EQ(size, 1099511627776L);

    ASSERT_OK_AND_ASSIGN(size, MemorySize::ParseBytes("1024gb"));
    ASSERT_EQ(size, 1099511627776L);

    ASSERT_OK_AND_ASSIGN(size, MemorySize::ParseBytes("1024gibibytes"));
    ASSERT_EQ(size, 1099511627776L);

    ASSERT_OK_AND_ASSIGN(size, MemorySize::ParseBytes("1024t"));
    ASSERT_EQ(size, 1125899906842624L);

    ASSERT_OK_AND_ASSIGN(size, MemorySize::ParseBytes("1024tb"));
    ASSERT_EQ(size, 1125899906842624L);

    ASSERT_OK_AND_ASSIGN(size, MemorySize::ParseBytes("1024tebibytes"));
    ASSERT_EQ(size, 1125899906842624L);
}

TEST(MemorySizeTest, TestParseBytesUpperCaseAndWithSpace) {
    ASSERT_OK_AND_ASSIGN(int64_t size, MemorySize::ParseBytes("1024 B"));
    ASSERT_EQ(size, 1024);

    ASSERT_OK_AND_ASSIGN(size, MemorySize::ParseBytes("1024Bytes "));
    ASSERT_EQ(size, 1024L);

    ASSERT_OK_AND_ASSIGN(size, MemorySize::ParseBytes("1024 K"));
    ASSERT_EQ(size, 1048576L);

    ASSERT_OK_AND_ASSIGN(size, MemorySize::ParseBytes("1024Kb "));
    ASSERT_EQ(size, 1048576L);

    ASSERT_OK_AND_ASSIGN(size, MemorySize::ParseBytes("1024kiBIbyTes"));
    ASSERT_EQ(size, 1048576L);

    ASSERT_OK_AND_ASSIGN(size, MemorySize::ParseBytes("1024M"));
    ASSERT_EQ(size, 1073741824L);

    ASSERT_OK_AND_ASSIGN(size, MemorySize::ParseBytes("1024mB"));
    ASSERT_EQ(size, 1073741824L);

    ASSERT_OK_AND_ASSIGN(size, MemorySize::ParseBytes("1024 meBIbyteS"));
    ASSERT_EQ(size, 1073741824L);

    ASSERT_OK_AND_ASSIGN(size, MemorySize::ParseBytes("1024G"));
    ASSERT_EQ(size, 1099511627776L);

    ASSERT_OK_AND_ASSIGN(size, MemorySize::ParseBytes("1024 GB"));
    ASSERT_EQ(size, 1099511627776L);

    ASSERT_OK_AND_ASSIGN(size, MemorySize::ParseBytes("1024gIbibytes"));
    ASSERT_EQ(size, 1099511627776L);

    ASSERT_OK_AND_ASSIGN(size, MemorySize::ParseBytes("1024   T"));
    ASSERT_EQ(size, 1125899906842624L);

    ASSERT_OK_AND_ASSIGN(size, MemorySize::ParseBytes(" 1024Tb "));
    ASSERT_EQ(size, 1125899906842624L);

    ASSERT_OK_AND_ASSIGN(size, MemorySize::ParseBytes("1024tebiBytes "));
    ASSERT_EQ(size, 1125899906842624L);
}

TEST(MemorySizeTest, TestInvalidInput) {
    ASSERT_NOK_WITH_MSG(MemorySize::ParseBytes(""),
                        "argument is an empty or whitespace-only string");
    ASSERT_NOK_WITH_MSG(MemorySize::ParseBytes("1 SomeUnknownUnit"),
                        "does not match any of the recognized units");
    ASSERT_NOK_WITH_MSG(MemorySize::ParseBytes("-1b"), "text does not start with a number");

    uint64_t overflow_num = std::numeric_limits<uint64_t>::max();
    ASSERT_NOK_WITH_MSG(MemorySize::ParseBytes(std::to_string(overflow_num) + " b"),
                        "cannot be represented as 64bit number (numeric overflow)");
    int64_t overflow_num_2 = std::numeric_limits<int64_t>::max();
    ASSERT_NOK_WITH_MSG(MemorySize::ParseBytes(std::to_string(overflow_num_2) + " kb"),
                        "cannot be represented as 64bit number of bytes (numeric overflow)");
}

}  // namespace paimon::test
