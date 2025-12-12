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

#include "paimon/common/utils/math.h"

#include <memory>

#include "gtest/gtest.h"

namespace paimon::test {

// Test case: Test EndianSwapValue for different integral types
TEST(EndianFunctionsTest, EndianSwapValue) {
    // Test 16-bit value
    uint16_t value16 = 0x1234;
    uint16_t swapped16 = EndianSwapValue(value16);
    EXPECT_EQ(swapped16, 0x3412);

    // Test 32-bit value
    uint32_t value32 = 0x12345678;
    uint32_t swapped32 = EndianSwapValue(value32);
    EXPECT_EQ(swapped32, 0x78563412);

    // Test 64-bit value
    uint64_t value64 = 0x123456789ABCDEF0;
    uint64_t swapped64 = EndianSwapValue(value64);
    EXPECT_EQ(swapped64, 0xF0DEBC9A78563412);
}

}  // namespace paimon::test
