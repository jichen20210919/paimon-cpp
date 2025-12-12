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

#include "paimon/common/reader/data_evolution_array.h"

#include "gtest/gtest.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
TEST(DataEvolutionArrayTest, TestSimple) {
    auto pool = GetDefaultPool();

    std::vector<int32_t> array_offsets = {0, 2, 0, 1, 2, 1};
    std::vector<int32_t> field_offsets = {0, 0, 1, 1, 1, 0};

    std::vector<int64_t> src_array1 = {1, -100};
    auto array1 = BinaryRowGenerator::FromLongArrayWithNull(src_array1, pool.get());

    std::vector<int64_t> src_array2 = {2, -2};
    auto array2 = BinaryRowGenerator::FromLongArrayWithNull(src_array2, pool.get());

    std::vector<int64_t> src_array3 = {3, -3};
    auto array3 = BinaryRowGenerator::FromLongArrayWithNull(src_array3, pool.get());

    DataEvolutionArray data_evolution_array(std::vector<BinaryArray>({array1, array2, array3}),
                                            array_offsets, field_offsets);

    ASSERT_FALSE(data_evolution_array.IsNullAt(0));

    ASSERT_EQ(data_evolution_array.GetLong(0), 1);
    ASSERT_EQ(data_evolution_array.GetLong(1), 3);
    ASSERT_EQ(data_evolution_array.GetLong(2), -100);
    ASSERT_EQ(data_evolution_array.GetLong(3), -2);
    ASSERT_EQ(data_evolution_array.GetLong(4), -3);
    ASSERT_EQ(data_evolution_array.GetLong(5), 2);

    ASSERT_OK_AND_ASSIGN(auto ret, data_evolution_array.ToLongArray());
    ASSERT_EQ(ret, std::vector<int64_t>({1, 3, -100, -2, -3, 2}));

    ASSERT_NOK_WITH_MSG(data_evolution_array.ToBooleanArray(),
                        "DataEvolutionArray not support ToBooleanArray");
}

TEST(DataEvolutionArrayTest, TestNullValue) {
    auto pool = GetDefaultPool();

    std::vector<int32_t> array_offsets = {-2, -1, 0, 0};
    std::vector<int32_t> field_offsets = {-1, -1, 0, 1};

    std::vector<int64_t> src_array1 = {1, -100};
    auto array1 = BinaryRowGenerator::FromLongArrayWithNull(src_array1, pool.get());

    DataEvolutionArray data_evolution_array(std::vector<BinaryArray>({array1}), array_offsets,
                                            field_offsets);

    ASSERT_TRUE(data_evolution_array.IsNullAt(0));
    ASSERT_TRUE(data_evolution_array.IsNullAt(1));
    ASSERT_EQ(data_evolution_array.GetLong(2), 1);
    ASSERT_EQ(data_evolution_array.GetLong(3), -100);
}

}  // namespace paimon::test
