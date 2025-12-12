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

#include "paimon/memory/memory_pool.h"

#include "gtest/gtest.h"
#include "paimon/memory/bytes.h"

namespace paimon::test {
TEST(MemoryPoolTest, TestSimple) {
    auto pool = GetMemoryPool();
    auto* p1 = pool->Malloc(10);
    ASSERT_TRUE(p1);
    ASSERT_EQ(10, pool->CurrentUsage());
    ASSERT_EQ(10, pool->MaxMemoryUsage());

    // test malloc and free
    auto* p2 = pool->Malloc(20);
    ASSERT_TRUE(p2);
    ASSERT_EQ(30, pool->CurrentUsage());
    ASSERT_EQ(30, pool->MaxMemoryUsage());
    pool->Free(p1, 10);
    ASSERT_EQ(20, pool->CurrentUsage());
    ASSERT_EQ(30, pool->MaxMemoryUsage());

    // test realloc without alignment
    auto* p3 = pool->Realloc(p2, /*old_size=*/20, /*new_size=*/40);
    ASSERT_TRUE(p3);
    ASSERT_EQ(40, pool->CurrentUsage());
    ASSERT_EQ(40, pool->MaxMemoryUsage());

    // test realloc with nullptr
    auto* p4 = pool->Realloc(nullptr, /*old_size=*/0, /*new_size=*/10, /*alignment=*/8);
    ASSERT_TRUE(p4);
    ASSERT_EQ(50, pool->CurrentUsage());
    ASSERT_EQ(50, pool->MaxMemoryUsage());

    // test realloc with same size
    auto* p5 = pool->Realloc(p4, /*old_size=*/10, /*new_size=*/10, /*alignment=*/8);
    ASSERT_EQ(p5, p4);
    ASSERT_EQ(50, pool->CurrentUsage());
    ASSERT_EQ(50, pool->MaxMemoryUsage());

    // test new size is 0
    auto* p6 = pool->Realloc(p4, /*old_size=*/10, /*new_size=*/0, /*alignment=*/8);
    ASSERT_TRUE(p6);
    ASSERT_EQ(40, pool->CurrentUsage());
    ASSERT_EQ(50, pool->MaxMemoryUsage());

    // do not shrink to fit, when new size is not very small, to avoid memory copy
    auto* p7 = pool->Realloc(p3, /*old_size=*/40, /*new_size=*/30, /*alignment=*/8);
    ASSERT_EQ(p7, p3);
    ASSERT_EQ(30, pool->CurrentUsage());
    ASSERT_EQ(50, pool->MaxMemoryUsage());

    // test normal realloc, malloc new pointer and free the old
    auto* p8 = pool->Realloc(p7, /*old_size=*/30, /*new_size=*/100, /*alignment=*/8);
    ASSERT_TRUE(p8);
    ASSERT_NE(p3, p8);
    ASSERT_EQ(100, pool->CurrentUsage());
    ASSERT_EQ(130, pool->MaxMemoryUsage());

    pool->Free(p8, 100);
    pool->Free(p6, 0);
    ASSERT_EQ(0, pool->CurrentUsage());
    ASSERT_EQ(130, pool->MaxMemoryUsage());
}
}  // namespace paimon::test
