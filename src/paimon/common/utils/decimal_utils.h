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

#pragma once

#include <cstdint>
#include <optional>
#include <string>

#include "arrow/api.h"
#include "arrow/util/decimal.h"
#include "fmt/format.h"
#include "paimon/data/decimal.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace arrow {
class DataType;
}  // namespace arrow

namespace paimon {
class DecimalUtils {
 public:
    DecimalUtils() = delete;
    ~DecimalUtils() = delete;

    static Status CheckDecimalType(const arrow::DataType& type);

    static std::optional<arrow::Decimal128> RescaleDecimalWithOverflowCheck(
        const arrow::Decimal128& src_decimal, int32_t src_scale, int32_t target_precision,
        int32_t target_scale);

    static Result<Decimal::int128_t> StrToInt128(const std::string& str);
};

}  // namespace paimon
