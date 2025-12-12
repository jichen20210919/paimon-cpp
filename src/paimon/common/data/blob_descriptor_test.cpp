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

#include "paimon/common/data/blob_descriptor.h"

#include <memory>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class BlobDescriptorTest : public testing::Test {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
        ASSERT_OK_AND_ASSIGN(descriptor_,
                             BlobDescriptor::Create("test_uri", /*offset=*/1024, /*length=*/2048));

        std::vector<char> bytes = {1, 8, 0, 0, 0, 116, 101, 115, 116, 95, 117, 114, 105, 0, 4,
                                   0, 0, 0, 0, 0, 0,   0,   8,   0,   0,  0,   0,   0,   0};
        java_serialized_ = std::string(bytes.data(), bytes.size());
    }

 private:
    std::shared_ptr<MemoryPool> pool_;
    std::unique_ptr<BlobDescriptor> descriptor_;
    std::string java_serialized_;
};

TEST_F(BlobDescriptorTest, TestConstructorAndGetters) {
    ASSERT_EQ(descriptor_->Uri(), "test_uri");
    ASSERT_EQ(descriptor_->Offset(), 1024);
    ASSERT_EQ(descriptor_->Length(), 2048);
}

TEST_F(BlobDescriptorTest, TestSerializeDeserializeAndCompatibilityWithJava) {
    auto serialized = descriptor_->Serialize(pool_);
    std::string serialized_str(serialized->data(), serialized->size());
    ASSERT_EQ(serialized_str, java_serialized_);

    ASSERT_OK_AND_ASSIGN(auto restored_descriptor,
                         BlobDescriptor::Deserialize(serialized->data(), serialized->size()));
    ASSERT_EQ(restored_descriptor->Uri(), "test_uri");
    ASSERT_EQ(restored_descriptor->Offset(), 1024);
    ASSERT_EQ(restored_descriptor->Length(), 2048);
}

TEST_F(BlobDescriptorTest, TestSerializeDeserializeWithEmptyUri) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<BlobDescriptor> empty_uri_descriptor,
                         BlobDescriptor::Create(/*uri=*/"", /*offset=*/0, /*length=*/0));
    auto serialized = empty_uri_descriptor->Serialize(pool_);
    ASSERT_OK_AND_ASSIGN(auto restored_descriptor,
                         BlobDescriptor::Deserialize(serialized->data(), serialized->size()));

    ASSERT_EQ(restored_descriptor->Uri(), "");
    ASSERT_EQ(restored_descriptor->Offset(), 0);
    ASSERT_EQ(restored_descriptor->Length(), 0);
}

TEST_F(BlobDescriptorTest, TestSerializeDeserializeWithDynamicLength) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<BlobDescriptor> empty_uri_descriptor,
                         BlobDescriptor::Create("test_uri", /*offset=*/0, /*length=*/-1));
    auto serialized = empty_uri_descriptor->Serialize(pool_);
    ASSERT_OK_AND_ASSIGN(auto restored_descriptor,
                         BlobDescriptor::Deserialize(serialized->data(), serialized->size()));

    ASSERT_EQ(restored_descriptor->Uri(), "test_uri");
    ASSERT_EQ(restored_descriptor->Offset(), 0);
    ASSERT_EQ(restored_descriptor->Length(), -1);
}

TEST_F(BlobDescriptorTest, TestInvalidParameters) {
    // Test deserialize invalid version
    {
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<BlobDescriptor> descriptor,
                             BlobDescriptor::Create(/*uri=*/"test", /*offset=*/1, /*length=*/2));
        auto serialized = descriptor->Serialize(pool_);
        (*serialized)[0] = '\x02';
        ASSERT_NOK_WITH_MSG(BlobDescriptor::Deserialize(serialized->data(), serialized->size()),
                            "Expecting BlobDescriptor version to be 1, but found 2");
    }
    // Test deserialize invalid buffer size
    {
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<BlobDescriptor> descriptor,
                             BlobDescriptor::Create(/*uri=*/"test", /*offset=*/1, /*length=*/2));
        auto serialized = descriptor->Serialize(pool_);
        ASSERT_NOK(BlobDescriptor::Deserialize(serialized->data(), /*size=*/5));
    }
    // Test invalid offset
    {
        ASSERT_NOK_WITH_MSG(BlobDescriptor::Create(/*uri=*/"test", /*offset=*/-1, /*length=*/2),
                            "offset -1 is less than 0");
    }
    // Test invalid length
    {
        ASSERT_NOK_WITH_MSG(BlobDescriptor::Create(/*uri=*/"test", /*offset=*/1, /*length=*/-2),
                            "length -2 is less than -1");
    }
}

TEST_F(BlobDescriptorTest, TestToString) {
    std::string debug_str = descriptor_->ToString();
    ASSERT_FALSE(debug_str.empty());
    ASSERT_TRUE(debug_str.find("version=1") != std::string::npos);
    ASSERT_TRUE(debug_str.find("uri='test_uri'") != std::string::npos);
    ASSERT_TRUE(debug_str.find("offset=1024") != std::string::npos);
    ASSERT_TRUE(debug_str.find("length=2048") != std::string::npos);
}

TEST_F(BlobDescriptorTest, TestRoundTripConsistency) {
    auto first_serialized = descriptor_->Serialize(pool_);
    ASSERT_OK_AND_ASSIGN(
        auto first_restored,
        BlobDescriptor::Deserialize(first_serialized->data(), first_serialized->size()));
    auto second_serialized = first_restored->Serialize(pool_);
    ASSERT_EQ(*first_serialized, *second_serialized);

    ASSERT_OK_AND_ASSIGN(
        auto second_restored,
        BlobDescriptor::Deserialize(second_serialized->data(), second_serialized->size()));
    ASSERT_EQ(second_restored->Uri(), "test_uri");
    ASSERT_EQ(second_restored->Offset(), 1024);
    ASSERT_EQ(second_restored->Length(), 2048);
}

}  // namespace paimon::test
