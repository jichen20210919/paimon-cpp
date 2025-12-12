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

#include "paimon/common/utils/decimal_utils.h"

#include <memory>

#include "arrow/api.h"
#include "gtest/gtest.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(DecimalUtilsTest, TestCheckDecimalType) {
    ASSERT_NOK_WITH_MSG(DecimalUtils::CheckDecimalType(*arrow::int32()), "Invalid decimal type:");
    ASSERT_NOK_WITH_MSG(DecimalUtils::CheckDecimalType(*arrow::decimal128(20, 22)),
                        "precision must >= scale");
}

}  // namespace paimon::test
