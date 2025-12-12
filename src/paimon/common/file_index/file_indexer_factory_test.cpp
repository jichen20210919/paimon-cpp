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

#include "paimon/file_index/file_indexer_factory.h"

#include "gtest/gtest.h"
#include "paimon/common/file_index/bitmap/bitmap_file_index.h"
#include "paimon/common/file_index/bloomfilter/bloom_filter_file_index.h"
#include "paimon/common/file_index/bsi/bit_slice_index_bitmap_file_index.h"
#include "paimon/file_index/file_indexer.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
TEST(FileIndexerFactoryTest, TestRegister) {
    ASSERT_OK_AND_ASSIGN(auto file_indexer1, FileIndexerFactory::Get("bitmap", {}));
    ASSERT_TRUE(file_indexer1);
    auto* bitmap_indexer = dynamic_cast<BitmapFileIndex*>(file_indexer1.get());
    ASSERT_TRUE(bitmap_indexer);

    ASSERT_OK_AND_ASSIGN(auto file_indexer2, FileIndexerFactory::Get("bloom-filter", {}));
    ASSERT_TRUE(file_indexer2);
    auto* bloomfilter_indexer = dynamic_cast<BloomFilterFileIndex*>(file_indexer2.get());
    ASSERT_TRUE(bloomfilter_indexer);

    ASSERT_OK_AND_ASSIGN(auto file_indexer3, FileIndexerFactory::Get("bsi", {}));
    ASSERT_TRUE(file_indexer3);
    auto* bsi_indexer = dynamic_cast<BitSliceIndexBitmapFileIndex*>(file_indexer3.get());
    ASSERT_TRUE(bsi_indexer);

    ASSERT_OK_AND_ASSIGN(auto non_exist_file_indexer, FileIndexerFactory::Get("non-exist", {}));
    ASSERT_FALSE(non_exist_file_indexer);
}
}  // namespace paimon::test
