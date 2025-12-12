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

#include "paimon/format/column_stats.h"

#include <utility>

#include "gtest/gtest.h"
#include "paimon/common/utils/decimal_utils.h"
#include "paimon/result.h"

namespace paimon::test {

TEST(ColumnStatsTest, TestBooleanColumnStats) {
    auto stats = ColumnStats::CreateBooleanColumnStats(std::nullopt, std::nullopt, std::nullopt);
    auto typed_stats = dynamic_cast<BooleanColumnStats*>(stats.get());
    typed_stats->Collect(std::nullopt);
    ASSERT_EQ(std::nullopt, typed_stats->Min());
    ASSERT_EQ(std::nullopt, typed_stats->Max());
    ASSERT_EQ(1, typed_stats->NullCount().value());
    ASSERT_EQ("min null, max null, null count 1", stats->ToString());
    typed_stats->Collect(true);
    ASSERT_EQ(true, typed_stats->Min().value());
    ASSERT_EQ(true, typed_stats->Max().value());
    ASSERT_EQ(1, typed_stats->NullCount().value());
    typed_stats->Collect(std::nullopt);
    ASSERT_EQ(true, typed_stats->Min().value());
    ASSERT_EQ(true, typed_stats->Max().value());
    ASSERT_EQ(2, typed_stats->NullCount().value());
    ASSERT_EQ("min true, max true, null count 2", stats->ToString());
}

TEST(ColumnStatsTest, TestTinyIntColumnStats) {
    auto stats = ColumnStats::CreateTinyIntColumnStats(std::nullopt, std::nullopt, std::nullopt);
    auto typed_stats = dynamic_cast<TinyIntColumnStats*>(stats.get());
    typed_stats->Collect(std::nullopt);
    ASSERT_EQ(std::nullopt, typed_stats->Min());
    ASSERT_EQ(std::nullopt, typed_stats->Max());
    ASSERT_EQ(1, typed_stats->NullCount().value());
    ASSERT_EQ("min null, max null, null count 1", stats->ToString());
    typed_stats->Collect(5);
    ASSERT_EQ(5, typed_stats->Min().value());
    ASSERT_EQ(5, typed_stats->Max().value());
    typed_stats->Collect(10);
    ASSERT_EQ(5, typed_stats->Min().value());
    ASSERT_EQ(10, typed_stats->Max().value());
    typed_stats->Collect(1);
    ASSERT_EQ(1, typed_stats->Min().value());
    ASSERT_EQ(10, typed_stats->Max().value());
    ASSERT_EQ(1, typed_stats->NullCount().value());
    typed_stats->Collect(std::nullopt);
    ASSERT_EQ(1, typed_stats->Min().value());
    ASSERT_EQ(10, typed_stats->Max().value());
    ASSERT_EQ(2, typed_stats->NullCount().value());
    ASSERT_EQ("min 1, max 10, null count 2", stats->ToString());
}
TEST(ColumnStatsTest, TestSmallIntColumnStats) {
    auto stats = ColumnStats::CreateSmallIntColumnStats(std::nullopt, std::nullopt, std::nullopt);
    auto typed_stats = dynamic_cast<SmallIntColumnStats*>(stats.get());
    typed_stats->Collect(std::nullopt);
    ASSERT_EQ(std::nullopt, typed_stats->Min());
    ASSERT_EQ(std::nullopt, typed_stats->Max());
    ASSERT_EQ(1, typed_stats->NullCount().value());
    ASSERT_EQ("min null, max null, null count 1", stats->ToString());
    typed_stats->Collect(5);
    ASSERT_EQ(5, typed_stats->Min().value());
    ASSERT_EQ(5, typed_stats->Max().value());
    typed_stats->Collect(10);
    ASSERT_EQ(5, typed_stats->Min().value());
    ASSERT_EQ(10, typed_stats->Max().value());
    typed_stats->Collect(1);
    ASSERT_EQ(1, typed_stats->Min().value());
    ASSERT_EQ(10, typed_stats->Max().value());
    ASSERT_EQ(1, typed_stats->NullCount().value());
    typed_stats->Collect(std::nullopt);
    ASSERT_EQ(1, typed_stats->Min().value());
    ASSERT_EQ(10, typed_stats->Max().value());
    ASSERT_EQ(2, typed_stats->NullCount().value());
    ASSERT_EQ("min 1, max 10, null count 2", stats->ToString());
}

TEST(ColumnStatsTest, TestIntColumnStats) {
    auto stats = ColumnStats::CreateIntColumnStats(std::nullopt, std::nullopt, std::nullopt);
    auto typed_stats = dynamic_cast<IntColumnStats*>(stats.get());
    typed_stats->Collect(std::nullopt);
    ASSERT_EQ(std::nullopt, typed_stats->Min());
    ASSERT_EQ(std::nullopt, typed_stats->Max());
    ASSERT_EQ(1, typed_stats->NullCount().value());
    ASSERT_EQ("min null, max null, null count 1", stats->ToString());
    typed_stats->Collect(5);
    ASSERT_EQ(5, typed_stats->Min().value());
    ASSERT_EQ(5, typed_stats->Max().value());
    typed_stats->Collect(10);
    ASSERT_EQ(5, typed_stats->Min().value());
    ASSERT_EQ(10, typed_stats->Max().value());
    typed_stats->Collect(1);
    ASSERT_EQ(1, typed_stats->Min().value());
    ASSERT_EQ(10, typed_stats->Max().value());
    ASSERT_EQ(1, typed_stats->NullCount().value());
    typed_stats->Collect(std::nullopt);
    ASSERT_EQ(1, typed_stats->Min().value());
    ASSERT_EQ(10, typed_stats->Max().value());
    ASSERT_EQ(2, typed_stats->NullCount().value());
    ASSERT_EQ("min 1, max 10, null count 2", stats->ToString());
}
TEST(ColumnStatsTest, TestDateColumnStats) {
    auto stats = ColumnStats::CreateDateColumnStats(std::nullopt, std::nullopt, std::nullopt);
    auto typed_stats = dynamic_cast<DateColumnStats*>(stats.get());
    typed_stats->Collect(std::nullopt);
    ASSERT_EQ(std::nullopt, typed_stats->Min());
    ASSERT_EQ(std::nullopt, typed_stats->Max());
    ASSERT_EQ(1, typed_stats->NullCount().value());
    ASSERT_EQ("min null, max null, null count 1", stats->ToString());
    typed_stats->Collect(5);
    ASSERT_EQ(5, typed_stats->Min().value());
    ASSERT_EQ(5, typed_stats->Max().value());
    typed_stats->Collect(10);
    ASSERT_EQ(5, typed_stats->Min().value());
    ASSERT_EQ(10, typed_stats->Max().value());
    typed_stats->Collect(1);
    ASSERT_EQ(1, typed_stats->Min().value());
    ASSERT_EQ(10, typed_stats->Max().value());
    ASSERT_EQ(1, typed_stats->NullCount().value());
    typed_stats->Collect(std::nullopt);
    ASSERT_EQ(1, typed_stats->Min().value());
    ASSERT_EQ(10, typed_stats->Max().value());
    ASSERT_EQ(2, typed_stats->NullCount().value());
    ASSERT_EQ("min 1, max 10, null count 2", stats->ToString());
}
TEST(ColumnStatsTest, TestBigIntColumnStats) {
    auto stats = ColumnStats::CreateBigIntColumnStats(std::nullopt, std::nullopt, std::nullopt);
    auto typed_stats = dynamic_cast<BigIntColumnStats*>(stats.get());
    typed_stats->Collect(std::nullopt);
    ASSERT_EQ(std::nullopt, typed_stats->Min());
    ASSERT_EQ(std::nullopt, typed_stats->Max());
    ASSERT_EQ(1l, typed_stats->NullCount().value());
    ASSERT_EQ("min null, max null, null count 1", stats->ToString());
    typed_stats->Collect(5l);
    ASSERT_EQ(5l, typed_stats->Min().value());
    ASSERT_EQ(5l, typed_stats->Max().value());
    typed_stats->Collect(10l);
    ASSERT_EQ(5l, typed_stats->Min().value());
    ASSERT_EQ(10l, typed_stats->Max().value());
    typed_stats->Collect(1l);
    ASSERT_EQ(1l, typed_stats->Min().value());
    ASSERT_EQ(10l, typed_stats->Max().value());
    ASSERT_EQ(1l, typed_stats->NullCount().value());
    typed_stats->Collect(std::nullopt);
    ASSERT_EQ(1l, typed_stats->Min().value());
    ASSERT_EQ(10l, typed_stats->Max().value());
    ASSERT_EQ(2, typed_stats->NullCount().value());
    ASSERT_EQ("min 1, max 10, null count 2", stats->ToString());
}
TEST(ColumnStatsTest, TestFloatColumnStats) {
    auto stats = ColumnStats::CreateFloatColumnStats(std::nullopt, std::nullopt, std::nullopt);
    auto typed_stats = dynamic_cast<FloatColumnStats*>(stats.get());
    typed_stats->Collect(std::nullopt);
    ASSERT_EQ(std::nullopt, typed_stats->Min());
    ASSERT_EQ(std::nullopt, typed_stats->Max());
    ASSERT_EQ(1, typed_stats->NullCount().value());
    ASSERT_EQ("min null, max null, null count 1", stats->ToString());
    typed_stats->Collect(5.1f);
    ASSERT_EQ(5.1f, typed_stats->Min().value());
    ASSERT_EQ(5.1f, typed_stats->Max().value());
    typed_stats->Collect(10.1f);
    ASSERT_EQ(5.1f, typed_stats->Min().value());
    ASSERT_EQ(10.1f, typed_stats->Max().value());
    typed_stats->Collect(1.1f);
    ASSERT_EQ(1.1f, typed_stats->Min().value());
    ASSERT_EQ(10.1f, typed_stats->Max().value());
    ASSERT_EQ(1, typed_stats->NullCount().value());
    typed_stats->Collect(std::nullopt);
    ASSERT_EQ(1.1f, typed_stats->Min().value());
    ASSERT_EQ(10.1f, typed_stats->Max().value());
    ASSERT_EQ(2, typed_stats->NullCount().value());
    ASSERT_EQ("min 1.1, max 10.1, null count 2", stats->ToString());
}

TEST(ColumnStatsTest, TestDoubleColumnStats) {
    auto stats = ColumnStats::CreateDoubleColumnStats(std::nullopt, std::nullopt, std::nullopt);
    auto typed_stats = dynamic_cast<DoubleColumnStats*>(stats.get());
    typed_stats->Collect(std::nullopt);
    ASSERT_EQ(std::nullopt, typed_stats->Min());
    ASSERT_EQ(std::nullopt, typed_stats->Max());
    ASSERT_EQ(1, typed_stats->NullCount().value());
    ASSERT_EQ("min null, max null, null count 1", stats->ToString());
    typed_stats->Collect(5.12);
    ASSERT_EQ(5.12, typed_stats->Min().value());
    ASSERT_EQ(5.12, typed_stats->Max().value());
    typed_stats->Collect(10.12);
    ASSERT_EQ(5.12, typed_stats->Min().value());
    ASSERT_EQ(10.12, typed_stats->Max().value());
    typed_stats->Collect(1.12);
    ASSERT_EQ(1.12, typed_stats->Min().value());
    ASSERT_EQ(10.12, typed_stats->Max().value());
    ASSERT_EQ(1, typed_stats->NullCount().value());
    typed_stats->Collect(std::nullopt);
    ASSERT_EQ(1.12, typed_stats->Min().value());
    ASSERT_EQ(10.12, typed_stats->Max().value());
    ASSERT_EQ(2, typed_stats->NullCount().value());
    ASSERT_EQ("min 1.12, max 10.12, null count 2", stats->ToString());
}

TEST(ColumnStatsTest, TestStringColumnStats) {
    auto stats = ColumnStats::CreateStringColumnStats(std::nullopt, std::nullopt, std::nullopt);
    auto typed_stats = dynamic_cast<StringColumnStats*>(stats.get());
    typed_stats->Collect(std::nullopt);
    ASSERT_EQ(std::nullopt, typed_stats->Min());
    ASSERT_EQ(std::nullopt, typed_stats->Max());
    ASSERT_EQ(1, typed_stats->NullCount().value());
    ASSERT_EQ("min null, max null, null count 1", stats->ToString());
    typed_stats->Collect("abc");
    ASSERT_EQ("abc", typed_stats->Min().value());
    ASSERT_EQ("abc", typed_stats->Max().value());
    typed_stats->Collect("cba");
    ASSERT_EQ("abc", typed_stats->Min().value());
    ASSERT_EQ("cba", typed_stats->Max().value());
    typed_stats->Collect("你好");
    ASSERT_EQ("abc", typed_stats->Min().value());
    ASSERT_EQ("你好", typed_stats->Max().value());
    ASSERT_EQ(1, typed_stats->NullCount().value());
    typed_stats->Collect(std::nullopt);
    ASSERT_EQ("abc", typed_stats->Min().value());
    ASSERT_EQ("你好", typed_stats->Max().value());
    ASSERT_EQ(2, typed_stats->NullCount().value());
    ASSERT_EQ("min abc, max 你好, null count 2", stats->ToString());
}

TEST(ColumnStatsTest, TestTimestampColumnStats) {
    auto stats = ColumnStats::CreateTimestampColumnStats(std::nullopt, std::nullopt, std::nullopt,
                                                         /*precision=*/9);
    auto typed_stats = dynamic_cast<TimestampColumnStats*>(stats.get());
    typed_stats->Collect(std::nullopt);
    ASSERT_EQ(std::nullopt, typed_stats->Min());
    ASSERT_EQ(std::nullopt, typed_stats->Max());
    ASSERT_EQ(1l, typed_stats->NullCount().value());
    ASSERT_EQ("min null, max null, null count 1", stats->ToString());
    typed_stats->Collect(Timestamp(/*millisecond=*/10, /*nano_of_millisecond=*/1));
    ASSERT_EQ(Timestamp(/*millisecond=*/10, /*nano_of_millisecond=*/1), typed_stats->Min().value());
    ASSERT_EQ(Timestamp(/*millisecond=*/10, /*nano_of_millisecond=*/1), typed_stats->Max().value());
    typed_stats->Collect(Timestamp(/*millisecond=*/10, /*nano_of_millisecond=*/2));
    ASSERT_EQ(Timestamp(/*millisecond=*/10, /*nano_of_millisecond=*/1), typed_stats->Min().value());
    ASSERT_EQ(Timestamp(/*millisecond=*/10, /*nano_of_millisecond=*/2), typed_stats->Max().value());
    typed_stats->Collect(Timestamp(/*millisecond=*/9, /*nano_of_millisecond=*/1));
    ASSERT_EQ(Timestamp(/*millisecond=*/9, /*nano_of_millisecond=*/1), typed_stats->Min().value());
    ASSERT_EQ(Timestamp(/*millisecond=*/10, /*nano_of_millisecond=*/2), typed_stats->Max().value());
    ASSERT_EQ(1l, typed_stats->NullCount().value());
    typed_stats->Collect(std::nullopt);
    ASSERT_EQ(Timestamp(/*millisecond=*/9, /*nano_of_millisecond=*/1), typed_stats->Min().value());
    ASSERT_EQ(Timestamp(/*millisecond=*/10, /*nano_of_millisecond=*/2), typed_stats->Max().value());
    ASSERT_EQ(2, typed_stats->NullCount().value());
    ASSERT_EQ("min 1970-01-01 00:00:00.009000001, max 1970-01-01 00:00:00.010000002, null count 2",
              stats->ToString());
}

TEST(ColumnStatsTest, TestDecimalColumnStats) {
    int32_t precision = 21;
    int32_t scale = 3;
    auto stats = ColumnStats::CreateDecimalColumnStats(std::nullopt, std::nullopt, std::nullopt,
                                                       precision, scale);
    auto typed_stats = dynamic_cast<DecimalColumnStats*>(stats.get());
    typed_stats->Collect(std::nullopt);
    ASSERT_EQ(std::nullopt, typed_stats->Min());
    ASSERT_EQ(std::nullopt, typed_stats->Max());
    ASSERT_EQ(1l, typed_stats->NullCount().value());
    ASSERT_EQ("min null, max null, null count 1", stats->ToString());
    typed_stats->Collect(
        Decimal(precision, scale, DecimalUtils::StrToInt128("123456789987654321234").value()));
    ASSERT_EQ(Decimal(precision, scale, DecimalUtils::StrToInt128("123456789987654321234").value()),
              typed_stats->Min().value());
    ASSERT_EQ(Decimal(precision, scale, DecimalUtils::StrToInt128("123456789987654321234").value()),
              typed_stats->Max().value());
    typed_stats->Collect(
        Decimal(precision, scale, DecimalUtils::StrToInt128("923456789987654321234").value()));
    ASSERT_EQ(Decimal(precision, scale, DecimalUtils::StrToInt128("123456789987654321234").value()),
              typed_stats->Min().value());
    ASSERT_EQ(Decimal(precision, scale, DecimalUtils::StrToInt128("923456789987654321234").value()),
              typed_stats->Max().value());
    typed_stats->Collect(
        Decimal(precision, scale, DecimalUtils::StrToInt128("123456789987654321233").value()));
    ASSERT_EQ(Decimal(precision, scale, DecimalUtils::StrToInt128("123456789987654321233").value()),
              typed_stats->Min().value());
    ASSERT_EQ(Decimal(precision, scale, DecimalUtils::StrToInt128("923456789987654321234").value()),
              typed_stats->Max().value());
    ASSERT_EQ(1l, typed_stats->NullCount().value());
    typed_stats->Collect(std::nullopt);
    ASSERT_EQ(Decimal(precision, scale, DecimalUtils::StrToInt128("123456789987654321233").value()),
              typed_stats->Min().value());
    ASSERT_EQ(Decimal(precision, scale, DecimalUtils::StrToInt128("923456789987654321234").value()),
              typed_stats->Max().value());
    ASSERT_EQ(2, typed_stats->NullCount().value());
    ASSERT_EQ("min 123456789987654321.233, max 923456789987654321.234, null count 2",
              stats->ToString());
}

}  // namespace paimon::test
