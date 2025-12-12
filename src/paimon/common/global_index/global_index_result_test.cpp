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

#include "paimon/global_index/global_index_result.h"

#include <utility>

#include "gtest/gtest.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
class GlobalIndexResultTest : public ::testing::Test {
 public:
    void SetUp() override {}
    void TearDown() override {}

    class FakeGlobalIndexResult : public GlobalIndexResult {
     public:
        explicit FakeGlobalIndexResult(const std::vector<int64_t>& values) : values_(values) {}
        class Iterator : public GlobalIndexResult::Iterator {
         public:
            Iterator(const std::vector<int64_t>* values,
                     const std::vector<int64_t>::const_iterator& iter)
                : values_(values), iter_(iter) {}
            bool HasNext() const override {
                return iter_ != values_->end();
            }
            int64_t Next() override {
                int64_t value = *iter_;
                iter_++;
                return value;
            }
            const std::vector<int64_t>* values_;
            std::vector<int64_t>::const_iterator iter_;
        };

        Result<std::unique_ptr<GlobalIndexResult::Iterator>> CreateIterator() const override {
            auto iter = values_.begin();
            return std::make_unique<Iterator>(&values_, iter);
        }

        Result<bool> IsEmpty() const override {
            return values_.empty();
        }

        std::string ToString() const override {
            return "fake";
        }

     private:
        std::vector<int64_t> values_;
    };
};

TEST_F(GlobalIndexResultTest, TestSimple) {
    auto result1 = std::make_shared<FakeGlobalIndexResult>(std::vector<int64_t>({1, 3, 5, 100}));
    auto result2 =
        std::make_shared<FakeGlobalIndexResult>(std::vector<int64_t>({100, 5, 4, 3, 200}));
    ASSERT_OK_AND_ASSIGN(auto and_result, result1->And(result2));
    ASSERT_EQ(and_result->ToString(), "{3,5,100}");

    ASSERT_OK_AND_ASSIGN(auto or_result, result1->Or(result2));
    ASSERT_EQ(or_result->ToString(), "{1,3,4,5,100,200}");
}
}  // namespace paimon::test
