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
#include <string>
#include <vector>

#include "paimon/result.h"

namespace paimon {

/// TimeDuration is a representation of a number of millisecond, viewable in different units.
///
/// <h2>Parsing</h2>
///
/// The size can be parsed from a text expression. If the expression is a pure number, the value
/// will be interpreted as millisecond.
class TimeDuration {
 public:
    /// class which defines time duration, mostly used to parse value from configuration file.
    ///
    /// To make larger values more compact, the common size suffixes are supported:
    ///
    /// <ul>
    /// <li>1ns or 1nano or 1nanos or 1nanosecond or 1nanoseconds
    /// <li>1us or 1µs or 1micro or 1micros or 1microsecond or 1microseconds
    /// <li>1ms or 1milli or 1millis or 1millisecond or 1milliseconds
    /// <li>1s or 1sec or 1secs or 1second or 1seconds
    /// <li>1min or 1m or 1minute or 1minutes
    /// <li>1h or 1hour or 1hours
    /// <li>1d or 1day or 1days
    /// </ul>
    class TimeUnit {
     public:
        enum class CoefficientOperator { MULTIPLY, DIVIDE };

        TimeUnit(const std::vector<std::string>& units, int64_t coefficient, CoefficientOperator op)
            : units_(units), coefficient_(coefficient), op_(op) {}

        const std::vector<std::string>& GetUnits() const {
            return units_;
        }

        int64_t GetCoefficient() const {
            return coefficient_;
        }

        CoefficientOperator GetCoefficientOperator() const {
            return op_;
        }

     private:
        std::vector<std::string> units_;
        int64_t coefficient_;
        CoefficientOperator op_;
    };

    static const TimeUnit& NanoSeconds();
    static const TimeUnit& MicroSeconds();
    static const TimeUnit& MilliSeconds();
    static const TimeUnit& Seconds();
    static const TimeUnit& Minutes();
    static const TimeUnit& Hours();
    static const TimeUnit& Days();

    static Result<int64_t> Parse(const std::string& text);
    static Result<TimeUnit> ParseUnit(const std::string& unit);
    static bool MatchesAny(const std::string& str, const TimeDuration::TimeUnit& unit);

    TimeDuration() = delete;
    ~TimeDuration() = delete;
};

}  // namespace paimon
