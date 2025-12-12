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

#include "paimon/common/options/time_duration.h"

#include "gtest/gtest.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(TimeDurationTest, TestParseTime) {
    ASSERT_OK_AND_ASSIGN(int64_t time, TimeDuration::Parse("1000 ns"));
    ASSERT_EQ(time, 0L);
    for (const auto& unit : {"ns", " ns", "nano", " nano", "nanos", "nanosecond", "nanoseconds"}) {
        ASSERT_OK_AND_ASSIGN(time, TimeDuration::Parse(std::string("123456789") + unit));
        ASSERT_EQ(time, 123L);
    }
    for (const auto& unit : {"us", " us", "µs", "micro", "micros", "microsecond", "microseconds"}) {
        ASSERT_OK_AND_ASSIGN(time, TimeDuration::Parse(std::string("123456789") + unit));
        ASSERT_EQ(time, 123456L);
    }
    for (const auto& unit : {"ms", "milli", " milli", "millis", "millisecond", "milliseconds"}) {
        ASSERT_OK_AND_ASSIGN(time, TimeDuration::Parse(std::string("1000") + unit));
        ASSERT_EQ(time, 1000L);
    }
    // without time unit, default time unit is milli
    ASSERT_OK_AND_ASSIGN(time, TimeDuration::Parse("1000"));
    ASSERT_EQ(time, 1000L);
    for (const auto& unit : {"s", "sec", "secs", " second", "seconds"}) {
        ASSERT_OK_AND_ASSIGN(time, TimeDuration::Parse(std::string("1000") + unit));
        ASSERT_EQ(time, 1000000L);
    }
    for (const auto& unit : {" min", "m", "minute", "minutes"}) {
        ASSERT_OK_AND_ASSIGN(time, TimeDuration::Parse(std::string("1000") + unit));
        ASSERT_EQ(time, 60000000L);
    }
    for (const auto& unit : {"h", "hour", " hours"}) {
        ASSERT_OK_AND_ASSIGN(time, TimeDuration::Parse(std::string("1000") + unit));
        ASSERT_EQ(time, 3600000000L);
    }
    for (const auto& unit : {"d", "day", " days"}) {
        ASSERT_OK_AND_ASSIGN(time, TimeDuration::Parse(std::string("1000") + unit));
        ASSERT_EQ(time, 86400000000L);
    }
    // with invalid time unit
    ASSERT_NOK_WITH_MSG(TimeDuration::Parse("1000ss"),
                        "Time duration unit 'ss' does not match any of the recognized units");
    // with invalid time duration
    ASSERT_NOK_WITH_MSG(TimeDuration::Parse(""), "argument is an empty or whitespace-only string");
    ASSERT_NOK_WITH_MSG(TimeDuration::Parse("       "),
                        "argument is an empty or whitespace-only string");
    ASSERT_NOK_WITH_MSG(TimeDuration::Parse("ns"), "text does not start with a number");
}

TEST(TimeDurationTest, TestBoundaryCheck) {
    ASSERT_OK_AND_ASSIGN(int64_t time, TimeDuration::Parse("1000 us"));
    ASSERT_EQ(time, 1L);
    ASSERT_OK(TimeDuration::Parse("106751991167d"));
    ASSERT_NOK(TimeDuration::Parse("106751991168d"));
    ASSERT_OK(TimeDuration::Parse("2562047788015h"));
    ASSERT_NOK(TimeDuration::Parse("2562047788016h"));
    ASSERT_OK(TimeDuration::Parse("153722867280912m"));
    ASSERT_NOK(TimeDuration::Parse("153722867280913m"));
    ASSERT_OK(TimeDuration::Parse("9223372036854775s"));
    ASSERT_NOK(TimeDuration::Parse("9223372036854776s"));
    ASSERT_OK(TimeDuration::Parse("9223372036854775807ms"));
    ASSERT_NOK(TimeDuration::Parse("9223372036854775808ms"));
}
}  // namespace paimon::test
