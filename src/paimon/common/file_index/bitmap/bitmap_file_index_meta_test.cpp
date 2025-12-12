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

#include "paimon/common/file_index/bitmap/bitmap_file_index_meta.h"

#include <memory>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/common/file_index/bitmap/bitmap_file_index_meta_v1.h"
#include "paimon/common/file_index/bitmap/bitmap_file_index_meta_v2.h"
#include "paimon/defs.h"
#include "paimon/fs/file_system.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/literal.h"
#include "paimon/result.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
TEST(BitmapFileIndexMetaTest, TestStringType) {
    auto check_result = [](BitmapFileIndexMeta* index_meta) {
        Literal lit_a(FieldType::STRING, "a", 1);
        Literal lit_b(FieldType::STRING, "b", 1);
        Literal lit_null(FieldType::STRING);

        ASSERT_OK_AND_ASSIGN(auto entry_a, index_meta->FindEntry(lit_a));
        ASSERT_EQ(entry_a->offset, 20);
        ASSERT_EQ(entry_a->length, 20);

        ASSERT_OK_AND_ASSIGN(auto entry_b, index_meta->FindEntry(lit_b));
        ASSERT_EQ(entry_b->offset, -3);
        ASSERT_EQ(entry_b->length, -1);

        ASSERT_OK_AND_ASSIGN(auto entry_null, index_meta->FindEntry(lit_null));
        ASSERT_EQ(entry_null->offset, 0);
        ASSERT_EQ(entry_null->length, 20);

        // test non-exist field
        Literal lit_non_exist(FieldType::STRING, "non", 3);
        ASSERT_OK_AND_ASSIGN(auto* entry_non, index_meta->FindEntry(lit_non_exist));
        ASSERT_FALSE(entry_non);

        Literal lit_non_exist2(FieldType::STRING, "A", 1);
        ASSERT_OK_AND_ASSIGN(auto* entry_non2, index_meta->FindEntry(lit_non_exist2));
        ASSERT_FALSE(entry_non2);
    };
    {
        // test v1 version
        std::vector<char> index_bytes = {
            1, 0, 0, 0,  5,  0,  0,  0,  2,  1,  0, 0, 0, 0, 0, 0, 0,  1, 97, 0, 0,  0, 20, 0,
            0, 0, 1, 98, -1, -1, -1, -3, 58, 48, 0, 0, 1, 0, 0, 0, 0,  0, 1,  0, 16, 0, 0,  0,
            1, 0, 3, 0,  58, 48, 0,  0,  1,  0,  0, 0, 0, 0, 1, 0, 16, 0, 0,  0, 0,  0, 4,  0};

        auto input_stream =
            std::make_shared<ByteArrayInputStream>(index_bytes.data(), index_bytes.size());
        // skip version
        ASSERT_OK(input_stream->Seek(1, SeekOrigin::FS_SEEK_SET));
        BitmapFileIndexMetaV1 index_meta(FieldType::STRING, 0, index_bytes.size(),
                                         GetDefaultPool());
        ASSERT_OK(index_meta.Deserialize(input_stream));
        check_result(&index_meta);
    }
    {
        // test v2 version
        std::vector<char> index_bytes = {
            2,  0,  0,  0,  5, 0,  0, 0, 2, 1,  0, 0, 0, 0,  0,  0,  0,  20, 0,  0,  0,
            1,  0,  0,  0,  1, 97, 0, 0, 0, 0,  0, 0, 0, 30, 0,  0,  0,  2,  0,  0,  0,
            1,  97, 0,  0,  0, 20, 0, 0, 0, 20, 0, 0, 0, 1,  98, -1, -1, -1, -3, -1, -1,
            -1, -1, 58, 48, 0, 0,  1, 0, 0, 0,  0, 0, 1, 0,  16, 0,  0,  0,  1,  0,  3,
            0,  58, 48, 0,  0, 1,  0, 0, 0, 0,  0, 1, 0, 16, 0,  0,  0,  0,  0,  4,  0};

        auto input_stream =
            std::make_shared<ByteArrayInputStream>(index_bytes.data(), index_bytes.size());
        // skip version
        ASSERT_OK(input_stream->Seek(1, SeekOrigin::FS_SEEK_SET));
        BitmapFileIndexMetaV2 index_meta(FieldType::STRING, index_bytes.size(), GetDefaultPool());
        ASSERT_OK(index_meta.Deserialize(input_stream));
        check_result(&index_meta);
    }
}

TEST(BitmapFileIndexMetaTest, TestInvalidType) {
    std::vector<char> index_bytes = {
        2,  0,  0,  0,  5, 0,  0, 0, 2, 1,  0, 0, 0, 0,  0,  0,  0,  20, 0,  0,  0,
        1,  0,  0,  0,  1, 97, 0, 0, 0, 0,  0, 0, 0, 30, 0,  0,  0,  2,  0,  0,  0,
        1,  97, 0,  0,  0, 20, 0, 0, 0, 20, 0, 0, 0, 1,  98, -1, -1, -1, -3, -1, -1,
        -1, -1, 58, 48, 0, 0,  1, 0, 0, 0,  0, 0, 1, 0,  16, 0,  0,  0,  1,  0,  3,
        0,  58, 48, 0,  0, 1,  0, 0, 0, 0,  0, 1, 0, 16, 0,  0,  0,  0,  0,  4,  0};

    auto input_stream =
        std::make_shared<ByteArrayInputStream>(index_bytes.data(), index_bytes.size());
    // skip version
    ASSERT_OK(input_stream->Seek(1, SeekOrigin::FS_SEEK_SET));
    BitmapFileIndexMetaV2 index_meta(FieldType::DECIMAL, index_bytes.size(), GetDefaultPool());
    ASSERT_NOK_WITH_MSG(index_meta.Deserialize(input_stream),
                        "not support field type DECIMAL in BitmapIndex");
}

}  // namespace paimon::test
