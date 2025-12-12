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

#include <cstddef>
#include <limits>
#include <optional>
#include <utility>

#include "fmt/format.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

const TimeDuration::TimeUnit& TimeDuration::NanoSeconds() {
    static const TimeUnit kNanoSeconds{{"ns", "nano", "nanos", "nanosecond", "nanoseconds"},
                                       1000 * 1000L,
                                       TimeUnit::CoefficientOperator::DIVIDE};
    return kNanoSeconds;
}

const TimeDuration::TimeUnit& TimeDuration::MicroSeconds() {
    static const TimeUnit kMicroSeconds{
        {"us", "µs", "micro", "micros", "microsecond", "microseconds"},
        1000L,
        TimeUnit::CoefficientOperator::DIVIDE};
    return kMicroSeconds;
}

const TimeDuration::TimeUnit& TimeDuration::MilliSeconds() {
    static const TimeUnit kMilliSeconds{{"ms", "milli", "millis", "millisecond", "milliseconds"},
                                        1,
                                        TimeUnit::CoefficientOperator::MULTIPLY};
    return kMilliSeconds;
}

const TimeDuration::TimeUnit& TimeDuration::Seconds() {
    static const TimeUnit kSeconds{
        {"s", "sec", "secs", "second", "seconds"}, 1000L, TimeUnit::CoefficientOperator::MULTIPLY};
    return kSeconds;
}

const TimeDuration::TimeUnit& TimeDuration::Minutes() {
    static const TimeUnit kMinutes{
        {"min", "m", "minute", "minutes"}, 1000L * 60, TimeUnit::CoefficientOperator::MULTIPLY};
    return kMinutes;
}

const TimeDuration::TimeUnit& TimeDuration::Hours() {
    static const TimeUnit kHours{
        {"h", "hour", "hours"}, 1000L * 60 * 60, TimeUnit::CoefficientOperator::MULTIPLY};
    return kHours;
}

const TimeDuration::TimeUnit& TimeDuration::Days() {
    static const TimeUnit kDays{
        {"d", "day", "days"}, 1000L * 60 * 60 * 24, TimeUnit::CoefficientOperator::MULTIPLY};
    return kDays;
}

Result<int64_t> TimeDuration::Parse(const std::string& text) {
    std::string trimmed = text;
    StringUtils::Trim(&trimmed);
    if (trimmed.empty()) {
        return Status::Invalid("argument is an empty or whitespace-only string");
    }
    const size_t len = trimmed.length();
    size_t pos = 0;

    char current;
    while (pos < len) {
        current = trimmed[pos];
        if (current >= '0' && current <= '9') {
            pos++;
        } else {
            break;
        }
    }
    std::string number = trimmed.substr(0, pos);
    if (number.empty()) {
        return Status::Invalid("text does not start with a number");
    }
    std::string raw_unit = trimmed.substr(pos);
    StringUtils::Trim(&raw_unit);
    std::string unit = StringUtils::ToLowerCase(raw_unit);

    std::optional<int64_t> value = StringUtils::StringToValue<int64_t>(number);
    if (value == std::nullopt) {
        return Status::Invalid(fmt::format(
            "The value '{}' cannot be represented as 64bit number (numeric overflow).", number));
    }
    PAIMON_ASSIGN_OR_RAISE(TimeUnit parsed_unit, ParseUnit(unit));
    int64_t coefficient = parsed_unit.GetCoefficient();
    TimeUnit::CoefficientOperator op = parsed_unit.GetCoefficientOperator();

    int64_t maximum = std::numeric_limits<int64_t>::max() /
                      (op == TimeUnit::CoefficientOperator::DIVIDE ? 1 : coefficient);
    // check for overflow
    if (value.value() > maximum) {
        return Status::Invalid(
            fmt::format("The value '{}' cannot be represented as 64bit number of milliseconds "
                        "(numeric overflow).",
                        text));
    }
    if (op == TimeUnit::CoefficientOperator::MULTIPLY) {
        return value.value() * coefficient;
    } else {
        return value.value() / coefficient;
    }
}

Result<TimeDuration::TimeUnit> TimeDuration::ParseUnit(const std::string& unit) {
    if (MatchesAny(unit, NanoSeconds())) {
        return NanoSeconds();
    } else if (MatchesAny(unit, MicroSeconds())) {
        return MicroSeconds();
    } else if (MatchesAny(unit, MilliSeconds())) {
        return MilliSeconds();
    } else if (MatchesAny(unit, Seconds())) {
        return Seconds();
    } else if (MatchesAny(unit, Minutes())) {
        return Minutes();
    } else if (MatchesAny(unit, Hours())) {
        return Hours();
    } else if (MatchesAny(unit, Days())) {
        return Days();
    } else if (!unit.empty()) {
        return Status::Invalid(fmt::format(
            "Time duration unit '{}' does not match any of the recognized units", unit));
    }
    return MilliSeconds();
}

bool TimeDuration::MatchesAny(const std::string& str, const TimeDuration::TimeUnit& unit) {
    for (const auto& u : unit.GetUnits()) {
        if (u == str) {
            return true;
        }
    }
    return false;
}

}  // namespace paimon
